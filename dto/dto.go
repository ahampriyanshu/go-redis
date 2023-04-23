package dto

type Command struct {
	Command string `json:"command"`
}

type Response struct {
	Value string `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}
