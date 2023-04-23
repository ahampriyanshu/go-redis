package store

import (
	"sync"
	"time"
)

// Thread-safe, in-memory key-value store
type KeyValueStore struct {
	Mu     sync.RWMutex
	Store  map[string]Data
	Queues map[string]chan string
}

type Data struct {
	Value      string
	ExpiryTime *time.Time
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		Store:  make(map[string]Data),
		Queues: make(map[string]chan string),
	}
}
