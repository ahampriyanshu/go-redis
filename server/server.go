package server

import (
	"greedy-games/controller"
	"greedy-games/store"
	"net/http"
)

func Lanuch(kv *store.KeyValueStore, port string) error {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		controller.HandleCommand(kv, w, r)
	})
	return http.ListenAndServe(port, nil)
}
