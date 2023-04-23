package controller

import (
	"encoding/json"
	"fmt"
	"greedy-games/constants"
	"greedy-games/utils"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func Set(kv *utils.KeyValueStore, args []string) error {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if len(args) < 2 {
		return fmt.Errorf("invalid command")
	}
	key := args[0]
	value := args[1]
	hasExpiry := false
	expiry := time.Time{}
	utils.RemoveExpiredKey(kv, key)
	index := 2

	if index < len(args) && strings.ToUpper(args[index]) == constants.EX {
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

		if strings.ToUpper(args[index]) == constants.NX && utils.Exists(kv, key) {
			return fmt.Errorf("key already exists")
		}

		if strings.ToUpper(args[index]) == constants.XX && !utils.Exists(kv, key) {
			return fmt.Errorf("key does not exist")
		}
	}

	if hasExpiry {
		kv.Store[key] = utils.Data{
			Value:      value,
			ExpiryTime: &expiry,
		}
	} else {
		kv.Store[key] = utils.Data{
			Value: value,
		}
	}
	return nil
}

func Get(kv *utils.KeyValueStore, args []string) (string, error) {
	kv.Mu.RLock()
	defer kv.Mu.RUnlock()

	if len(args) != 1 {
		return "", fmt.Errorf("invalid command")
	}

	key := args[0]
	utils.RemoveExpiredKey(kv, key)
	data, ok := kv.Store[key]

	if !ok {
		return "", fmt.Errorf("key not found")
	}

	return data.Value, nil
}

func QPush(kv *utils.KeyValueStore, args []string) error {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if len(args) < 2 {
		return fmt.Errorf("invalid command")
	}

	key := args[0]
	values := args[1:]

	value, ok := kv.Store[key]
	if !ok {
		kv.Store[key] = utils.Data{
			Value: strings.Join(values, " "),
		}
	} else {
		value.Value += " " + strings.Join(values, " ")
		kv.Store[key] = value
	}

	return nil
}

func QPop(kv *utils.KeyValueStore, args []string) (string, error) {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if len(args) != 1 {
		return "", fmt.Errorf("invalid command")
	}
	key := args[0]

	value, ok := kv.Store[key]
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
		delete(kv.Store, key)
	} else {
		kv.Store[key] = value
	}
	return lastValue, nil
}

func HandleCommand(kv *utils.KeyValueStore, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if r.Method != http.MethodPost {
		utils.SendErrRes(w, http.StatusMethodNotAllowed, "invalid request")
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		utils.SendErrRes(w, http.StatusUnprocessableEntity, "invalid request")
		log.Fatal(err.Error())
		return
	}

	var input utils.Command
	err = json.Unmarshal(body, &input)
	if err != nil {
		utils.SendErrRes(w, http.StatusUnprocessableEntity, "invalid request")
		log.Fatal(err.Error())
		return
	}

	tokenizedCommand := strings.Split(input.Command, " ")
	command := strings.ToUpper(tokenizedCommand[0])
	args := tokenizedCommand[1:]

	switch command {
	case "SET":
		err := Set(kv, args)
		if err != nil {
			utils.SendErrRes(w, http.StatusBadRequest, err.Error())
			return
		}
	case "GET":
		result, err := Get(kv, args)
		if err != nil {
			utils.SendErrRes(w, http.StatusBadRequest, err.Error())
			return
		}
		utils.SendOKResp(w, result)
	case "QPUSH":
		err := QPush(kv, args)
		if err != nil {
			utils.SendErrRes(w, http.StatusBadRequest, err.Error())
			return
		}
		utils.SendOKResp(w, "OK")
	case "QPOP":
		result, err := QPop(kv, args)
		if err != nil {
			utils.SendErrRes(w, http.StatusBadRequest, err.Error())
			return
		}
		utils.SendOKResp(w, result)
	default:
		utils.SendErrRes(w, http.StatusBadRequest, "invalid command")
	}
}
