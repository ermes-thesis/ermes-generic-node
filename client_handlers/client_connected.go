package client_handlers

import (
	"fmt"
	"io"
	"net/http"
)

func Accept_connection() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("got /accept_\n")
		io.WriteString(w, "Hello, HTTP!\n")
	}
}
