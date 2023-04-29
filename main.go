package main

import (
	"go-redis/constants"
	"go-redis/cron"
	"go-redis/server"
	"go-redis/store"
	"log"
)

// Importing the following packages:
// - constants: to access the port number to which the server is launched
// - cron: to perform periodic clean-up of the key-value store
// - server: to launch the server
// - store: to create a new key-value store

func main() {
	log.SetFlags(log.LstdFlags)
	log.Printf("Launching Server at port %s", constants.PORT)
	kv := store.NewKeyValueStore()
	cron.CleanUpJob(kv)
	err := server.Launch(kv, constants.PORT)
	if err != nil {
		log.Fatal("Server terminated!")
	}
}
