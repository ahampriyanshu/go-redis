package utils

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type KeyValueStore struct {
	Mu    sync.RWMutex
	Store map[string]Data
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
		Store: make(map[string]Data),
	}
}

func Exists(kv *KeyValueStore, key string) bool {
	_, ok := kv.Store[key]
	return ok
}

func RemoveExpiredKey(kv *KeyValueStore, key string) {
	data, ok := kv.Store[key]

	if ok {
		if data.ExpiryTime != nil && data.ExpiryTime.Before(time.Now()) {
			delete(kv.Store, key)
		}
	}
}

func SendOKResp(w http.ResponseWriter, data string) {
	w.Header().Set("Content-Type", "application/json")
	response := Response{
		Value: data,
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func SendErrRes(w http.ResponseWriter, status int, err string) {
	w.Header().Set("Content-Type", "application/json")
	response := Response{
		Error: err,
	}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}
