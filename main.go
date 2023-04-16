package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KeyValueStore struct {
	mu    sync.RWMutex
	store map[string]Value
}

type Value struct {
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
		store: make(map[string]Value),
	}
}

func (kv *KeyValueStore) Set(key string, value string, hasExpiry bool, expiryTime *time.Time, condition string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if condition == "NX" && kv.exists(key) {
		return fmt.Errorf("key already exists")
	} else if condition == "XX" && !kv.exists(key) {
		return fmt.Errorf("key does not exist")
	}

	if hasExpiry {
		kv.store[key] = Value{
			Value:      value,
			ExpiryTime: expiryTime,
		}
	} else {
		kv.store[key] = Value{
			Value: value,
		}
	}

	return nil
}

func (kv *KeyValueStore) Get(key string) (string, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, ok := kv.store[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}

	return value.Value, nil
}

func (kv *KeyValueStore) QPush(key string, values []string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.store[key]
	if !ok {
		kv.store[key] = Value{
			Value: strings.Join(values, " "),
		}
	} else {
		value.Value += " " + strings.Join(values, " ")
		kv.store[key] = value
	}
}

func (kv *KeyValueStore) QPop(key string) (string, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

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

func (kv *KeyValueStore) BQPop(key string, timeout *time.Duration) (string, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

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

	if timeout != nil {
		select {
		case <-time.After(*timeout):
			return "", fmt.Errorf("timeout")
		default:
			// continue
		}
	}

	return lastValue, nil
}

func (kv *KeyValueStore) exists(key string) bool {
	_, ok := kv.store[key]
	return ok
}

func handleSuccess(w http.ResponseWriter, data string) {
	w.Header().Set("Content-Type", "application/json")
	response := Response{
		Value: data,
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func handleError(w http.ResponseWriter, err string) {
	w.Header().Set("Content-Type", "application/json")
	response := Response{
		Error: err,
	}
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(response)
}

func handledResponse(w http.ResponseWriter, data string, err error) {
	w.Header().Set("Content-Type", "application/json")

	if err != nil {
		response := Response{
			Value: data,
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	} else {
		response := Response{
			Error: err.Error(),
		}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
	}
}

func parseCommand(rawCmd string, r *http.Request) (string, []string, string, string, bool, *time.Time, string) {
	cmd := strings.Split(rawCmd, " ")
	command := ""
	key := ""
	value := ""
	hasExpiry := false
	expiryTime := time.Time{}
	condition := ""

	if len(cmd) >= 1 {
		command = strings.ToUpper(cmd[0])
	}
	if len(cmd) >= 2 {
		key = cmd[1]
	}
	if len(cmd) >= 3 {
		value = cmd[2]
	}
	if len(cmd) >= 4 {
		if strings.ToUpper(cmd[3]) == "EX" {
			hasExpiry = true
		}
	}
	if len(cmd) >= 5 {
		expiryTimeStr := cmd[4]
		expiryTimeInt, _ := strconv.Atoi(expiryTimeStr)
		expiryTime = time.Now().Add(time.Second * time.Duration(expiryTimeInt))
	}

	if len(cmd) >= 6 {
		condition = strings.ToUpper(cmd[5])
	}

	return command, cmd[2:], key, value, hasExpiry, &expiryTime, condition
}

func handleCommand(kv *KeyValueStore, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		handleError(w, err.Error())
		return
	}

	var cmd Command
	err = json.Unmarshal(body, &cmd)
	if err != nil {
		handleError(w, err.Error())
		return
	}

	fmt.Println(cmd)

	command, args, key, value, hasExpiry, expiryTime, condition := parseCommand(cmd.Command, r)
	fmt.Println(args)

	switch command {
	case "SET":
		err := kv.Set(key, value, hasExpiry, expiryTime, condition)
		if err != nil {
			handleError(w, err.Error())
			return
		}
		handleSuccess(w, "OK")
	case "GET":
		result, err := kv.Get(key)
		if err != nil {
			handleError(w, err.Error())
			return
		}
		handleSuccess(w, result)
	case "QPUSH":
		values := args
		if len(values) == 0 {
			handleError(w, "value not provided")
			return
		}
		kv.QPush(key, values)
		handleSuccess(w, "OK")
	case "QPOP":
		result, err := kv.QPop(key)
		if err != nil {
			handleError(w, err.Error())
			return
		}
		handleSuccess(w, result)
	case "BQPOP":
		timeout, err := time.ParseDuration(args[0])
		if err != nil {
			handleError(w, err.Error())
		}

		result, err := kv.BQPop(key, &timeout)
		if err != nil {
			handleError(w, err.Error())
			return
		}
		handleSuccess(w, result)
	default:
		handleError(w, "Invalid command")
	}
}

func main() {
	kv := NewKeyValueStore()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleCommand(kv, w, r)
	})
	http.ListenAndServe(":8000", nil)
}
