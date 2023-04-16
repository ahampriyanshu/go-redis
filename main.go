package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KeyValueStore struct {
	mu    sync.RWMutex
	store map[string]Data
}

type Data struct {
	Value      string
	ExpiryTime *time.Time
}

type Command struct {
	Command string `json:"command"`
}

type Response struct {
	Value string `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		store: make(map[string]Data),
	}
}

func sendOKResp(w http.ResponseWriter, data string) {
	w.Header().Set("Content-Type", "application/json")
	response := Response{
		Value: data,
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func sendErrRes(w http.ResponseWriter, status int, err string) {
	w.Header().Set("Content-Type", "application/json")
	response := Response{
		Error: err,
	}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}

func (kv *KeyValueStore) Set(args []string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(args) < 2 {
		return fmt.Errorf("invalid command")
	}
	key := args[0]
	value := args[1]
	hasExpiry := false
	expiry := time.Time{}
	kv.removeExpiredKey(key)
	index := 2

	if index < len(args) && strings.ToUpper(args[index]) == "EX" {
		index++
		if index >= len(args) {
			return fmt.Errorf("invalid command")
		}
		expirySec, err := strconv.Atoi(args[index])
		if err != nil {
			return fmt.Errorf("invalid command")
		}
		expiry = time.Now().Add(time.Duration(expirySec) * time.Second)
		index++
		hasExpiry = true
	}

	if index < len(args) {

		if strings.ToUpper(args[index]) == "NX" && kv.exists(key) {
			return fmt.Errorf("key already exists")
		}

		if strings.ToUpper(args[index]) == "XX" && !kv.exists(key) {
			return fmt.Errorf("key does not exist")
		}
	}

	if hasExpiry {
		kv.store[key] = Data{
			Value:      value,
			ExpiryTime: &expiry,
		}
	} else {
		kv.store[key] = Data{
			Value: value,
		}
	}
	return nil
}

func (kv *KeyValueStore) Get(args []string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if len(args) != 1 {
		return "", fmt.Errorf("invalid command")
	}

	key := args[0]
	kv.removeExpiredKey(key)
	data, ok := kv.store[key]

	if !ok {
		return "", fmt.Errorf("key not found")
	}

	return data.Value, nil
}

func (kv *KeyValueStore) QPush(args []string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(args) < 2 {
		return fmt.Errorf("invalid command")
	}

	key := args[0]
	values := args[1:]

	value, ok := kv.store[key]
	if !ok {
		kv.store[key] = Data{
			Value: strings.Join(values, " "),
		}
	} else {
		value.Value += " " + strings.Join(values, " ")
		kv.store[key] = value
	}

	return nil
}

func (kv *KeyValueStore) QPop(args []string) (string, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(args) != 1 {
		return "", fmt.Errorf("invalid command")
	}
	key := args[0]

	value, ok := kv.store[key]
	if !ok {
		return "", fmt.Errorf("queue is empty")
	}

	values := strings.Split(value.Value, " ")
	if len(values) == 0 {
		return "", fmt.Errorf("queue is empty")
	}

	lastValue := values[len(values)-1]
	values = values[:len(values)-1]
	value.Value = strings.Join(values, " ")

	if value.Value == "" {
		delete(kv.store, key)
	} else {
		kv.store[key] = value
	}
	return lastValue, nil
}

func (kv *KeyValueStore) exists(key string) bool {
	_, ok := kv.store[key]
	return ok
}

func (kv *KeyValueStore) removeExpiredKey(key string) {
	data, ok := kv.store[key]

	if ok {
		if data.ExpiryTime != nil && data.ExpiryTime.Before(time.Now()) {
			delete(kv.store, key)
		}
	}

}

func handleCommand(kv *KeyValueStore, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if r.Method != http.MethodPost {
		sendErrRes(w, http.StatusMethodNotAllowed, "invalid request")
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendErrRes(w, http.StatusUnprocessableEntity, "invalid request")
		log.Fatal(err.Error())
		return
	}

	var input Command
	err = json.Unmarshal(body, &input)
	if err != nil {
		sendErrRes(w, http.StatusUnprocessableEntity, "invalid request")
		log.Fatal(err.Error())
		return
	}

	tokenizedCommand := strings.Split(input.Command, " ")
	command := strings.ToUpper(tokenizedCommand[0])
	args := tokenizedCommand[1:]

	switch command {
	case "SET":
		err := kv.Set(args)
		if err != nil {
			sendErrRes(w, http.StatusBadRequest, err.Error())
			return
		}
	case "GET":
		result, err := kv.Get(args)
		if err != nil {
			sendErrRes(w, http.StatusBadRequest, err.Error())
			return
		}
		sendOKResp(w, result)
	case "QPUSH":
		err := kv.QPush(args)
		if err != nil {
			sendErrRes(w, http.StatusBadRequest, err.Error())
			return
		}
		sendOKResp(w, "OK")
	case "QPOP":
		result, err := kv.QPop(args)
		if err != nil {
			sendErrRes(w, http.StatusBadRequest, err.Error())
			return
		}
		sendOKResp(w, result)
	default:
		sendErrRes(w, http.StatusBadRequest, "invalid command")
	}
}

func main() {
	kv := NewKeyValueStore()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleCommand(kv, w, r)
	})
	http.ListenAndServe(":8080", nil)
}
