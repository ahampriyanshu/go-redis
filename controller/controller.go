package controller

import (
	"encoding/json"
	"fmt"
	"go-redis/constants"
	"go-redis/dto"
	"go-redis/store"
	"go-redis/utils"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Sets a value for a given key in the store.
func Set(kv *store.KeyValueStore, args []string) error {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	// SET command must have at least two arguments (key and value)
	if len(args) < 2 {
		return fmt.Errorf(constants.INVALID_COMMAND)
	}
	key := args[0]
	value := args[1]
	hasExpiry := false
	expiry := time.Time{}
	utils.RemoveExpiredKey(kv, key)
	index := 2

	// Check if an expiration time has been provided
	if index < len(args) && strings.ToUpper(args[index]) == constants.EX {
		index++
		if index >= len(args) {
			return fmt.Errorf(constants.INVALID_COMMAND)
		}
		expirySec, err := strconv.Atoi(args[index])
		if err != nil {
			return fmt.Errorf(constants.INVALID_COMMAND)
		}
		expiry = time.Now().Add(time.Duration(expirySec) * time.Second)
		index++
		hasExpiry = true
	}

	// Check for NX or XX flags, which affect the behavior if the key already exists or not
	if index < len(args) {

		if strings.ToUpper(args[index]) == constants.NX && utils.Exists(kv, key) {
			return fmt.Errorf(constants.NX_ERR)
		}

		if strings.ToUpper(args[index]) == constants.XX && !utils.Exists(kv, key) {
			return fmt.Errorf(constants.XX_ERR)
		}
	}

	// Set the key value pair in the store
	if hasExpiry {
		kv.Store[key] = store.Data{
			Value:      value,
			ExpiryTime: &expiry,
		}
	} else {
		kv.Store[key] = store.Data{
			Value: value,
		}
	}
	return nil
}

// Returns the value for a given key in the store
func Get(kv *store.KeyValueStore, args []string) (string, error) {
	// GET command must have exactly one argument (the key)
	if len(args) != 1 {
		return "", fmt.Errorf(constants.INVALID_COMMAND)
	}

	key := args[0]
	
	// First, check if key exists and if it's expired (requires write lock if expired)
	kv.Mu.RLock()
	data, ok := kv.Store[key]
	kv.Mu.RUnlock()

	if !ok {
		return "", fmt.Errorf(constants.XX_ERR)
	}

	// Check if expired and remove if necessary (upgrade to write lock)
	if data.ExpiryTime != nil && data.ExpiryTime.Before(time.Now()) {
		kv.Mu.Lock()
		// Double-check that key still exists and is still expired
		if data, ok := kv.Store[key]; ok && data.ExpiryTime != nil && data.ExpiryTime.Before(time.Now()) {
			delete(kv.Store, key)
		}
		kv.Mu.Unlock()
		return "", fmt.Errorf(constants.XX_ERR)
	}

	return data.Value, nil
}

// Adds values to the end of a queue for a given key in the store.
func QPush(kv *store.KeyValueStore, args []string) error {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if len(args) < 2 {
		return fmt.Errorf(constants.INVALID_COMMAND)
	}

	key := args[0]
	values := args[1:]

	queue, ok := kv.Queues[key]

	if !ok {
		// If the queue doesn't exist, create a new channel
		queue = make(chan string, 100)
		kv.Queues[key] = queue
	}

	for _, value := range values {
		select {
		case queue <- value:
			log.Printf("Value '%s' pushed to queue with key '%s'", value, key)
		default:
			return fmt.Errorf("queue '%s' is full", key)
		}
	}

	return nil
}

// Pops the elements in the Queue from the back
func QPop(kv *store.KeyValueStore, args []string) (string, error) {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if len(args) != 1 {
		return "", fmt.Errorf(constants.INVALID_COMMAND)
	}
	key := args[0]

	queue, ok := kv.Queues[key]

	if !ok {
		return "", fmt.Errorf(constants.EMPTY_QUEUE)
	}

	// Creating temp memory for storing the first n-1 elements
	var values []string

	for len(queue) > 0 {
		value := <-queue
		values = append(values, value)
	}

	n := len(values)

	if n == 1 {
		// Removing empty queue
		close(queue)
		delete(kv.Queues, key)
		return values[0], nil
	}

	for _, value := range values[:n-1] {
		select {
		case queue <- value:
			log.Printf("Value '%s' pushed to queue with key '%s'", value, key)
		default:
			return "", fmt.Errorf("queue '%s' is full", key)
		}
	}

	return values[n-1], nil
}

func HandleCommand(kv *store.KeyValueStore, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if r.Method != http.MethodPost {
		utils.SendResponse(w, http.StatusMethodNotAllowed, "", constants.INVALID_REQUEST)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		utils.SendResponse(w, http.StatusUnprocessableEntity, "", constants.INVALID_REQUEST)
		return
	}

	var input dto.Command
	err = json.Unmarshal(body, &input)
	if err != nil {
		utils.SendResponse(w, http.StatusUnprocessableEntity, "", constants.INVALID_REQUEST)
		return
	}

	tokenizedCommand := strings.Split(input.Command, " ")
	command := strings.ToUpper(tokenizedCommand[0])
	args := tokenizedCommand[1:]

	switch command {
	case "SET":
		err := Set(kv, args)
		if err != nil {
			utils.SendResponse(w, http.StatusBadRequest, "", err.Error())
			return
		}
	case "GET":
		result, err := Get(kv, args)
		if err != nil {
			utils.SendResponse(w, http.StatusBadRequest, "", err.Error())
			return
		}
		utils.SendResponse(w, http.StatusOK, result, "")
	case "QPUSH":
		err := QPush(kv, args)
		if err != nil {
			utils.SendResponse(w, http.StatusBadRequest, "", err.Error())
			return
		}
		utils.SendResponse(w, http.StatusOK, constants.OK, "")
	case "QPOP":
		result, err := QPop(kv, args)
		if err != nil {
			utils.SendResponse(w, http.StatusBadRequest, "", err.Error())
			return
		}
		utils.SendResponse(w, http.StatusOK, result, "")
	default:
		utils.SendResponse(w, http.StatusBadRequest, "", constants.INVALID_COMMAND)
	}
}
