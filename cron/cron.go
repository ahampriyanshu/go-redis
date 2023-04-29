package cron

import (
	"go-redis/store"
	"log"
	"time"

	"github.com/robfig/cron/v3"
)

func CleanUpJob(kv *store.KeyValueStore) {
	c := cron.New()
	c.AddFunc("0 0 * * *", func() {
		log.Println("Starting Cleanup cron job")
		now := time.Now()
		
		// Collect keys to delete first (with read lock)
		var keysToDelete []string
		kv.Mu.RLock()
		for key, data := range kv.Store {
			if data.ExpiryTime != nil && now.After(*data.ExpiryTime) {
				keysToDelete = append(keysToDelete, key)
			}
		}
		kv.Mu.RUnlock()
		
		// Now delete the expired keys (with write lock)
		if len(keysToDelete) > 0 {
			kv.Mu.Lock()
			for _, key := range keysToDelete {
				// Double-check the key is still expired before deleting
				if data, exists := kv.Store[key]; exists && data.ExpiryTime != nil && now.After(*data.ExpiryTime) {
					delete(kv.Store, key)
					log.Printf("Removed value mapped with %s", key)
				}
			}
			kv.Mu.Unlock()
		}
	})
	c.Start()
}
