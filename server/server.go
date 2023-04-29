package server

import (
	"go-redis/controller"
	"go-redis/store"
	"net/http"
)

func Launch(kv *store.KeyValueStore, port string) error {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		controller.HandleCommand(kv, w, r)
	})
	return http.ListenAndServe(port, nil)
}
