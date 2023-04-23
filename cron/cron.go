package cron

import (
	"greedy-games/store"
	"log"
	"time"

	"github.com/robfig/cron/v3"
)

func CleanUpJob(kv *store.KeyValueStore) {
	c := cron.New()
	c.AddFunc("0 0 * * *", func() {
		log.Println("Starting Cleanup cron job")
		now := time.Now()
		for key, data := range kv.Store {
			if data.ExpiryTime != nil && now.After(*data.ExpiryTime) {
				kv.Mu.Lock()
				delete(kv.Store, key)
				log.Printf("Removed value mapped with %s", key)
				kv.Mu.Unlock()
			}

		}
	})
	c.Start()
}
