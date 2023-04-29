package utils

import (
	"encoding/json"
	"go-redis/dto"
	"go-redis/store"
	"log"
	"net/http"
	"time"
)

func Exists(kv *store.KeyValueStore, key string) bool {
	_, ok := kv.Store[key]
	return ok
}

func RemoveExpiredKey(kv *store.KeyValueStore, key string) {
	data, ok := kv.Store[key]

	if ok {
		if data.ExpiryTime != nil && data.ExpiryTime.Before(time.Now()) {
			delete(kv.Store, key)
		}
	}
}

func SendResponse(w http.ResponseWriter, status int, data string, err string) {
	w.Header().Set("Content-Type", "application/json")
	response := dto.Response{
		Value: data,
		Error: err,
	}
	if status >= 200 && status <= 299 {
		log.Printf("[SUCCESS] HTTP request returned with %d", status)
	} else {
		log.Printf("[ERROR] HTTP request returned with %d", status)
	}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}
