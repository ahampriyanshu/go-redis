package server

import (
	"greedy-games/controller"
	"greedy-games/utils"
	"net/http"
)

func ListenAndServe(kv *utils.KeyValueStore, port string) error {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		controller.HandleCommand(kv, w, r)
	})
	return http.ListenAndServe(port, nil)
}
