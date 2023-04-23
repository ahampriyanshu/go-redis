package main

import (
	"greedy-games/server"
	"greedy-games/utils"
)

func main() {
	kv := utils.NewKeyValueStore()
	err := server.ListenAndServe(kv, ":8080")
	if err != nil {
		panic(err)
	}
}
