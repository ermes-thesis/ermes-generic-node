package main

// Tutorial found here: https://www.digitalocean.com/community/tutorials/how-to-make-an-http-server-in-go

// More info about net package can be found here: https://pkg.go.dev/net

import (
	"client_handlers"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

func getRoot(w http.ResponseWriter, r *http.Request) {

	hasFirst := r.URL.Query().Has("first")
	first := r.URL.Query().Get("first")
	hasSecond := r.URL.Query().Has("second")
	second := r.URL.Query().Get("second")

	fmt.Printf("got / request. first(%t)=%s, second(%t)=%s\n",
		hasFirst, first,
		hasSecond, second)

	io.WriteString(w, "This is my website!\n")
}

func main() {

	// The main function is responsible for parsing the info received.
	// For the moment the info passed as arguments are his own name

	// Accept all incoming connections.
	// If want to define only a set of addresses use "localhost:3333"
	mux := http.NewServeMux()
	mux.HandleFunc("/", getRoot)
	mux.HandleFunc("/hello", client_handlers.Accept_connection())

	err := http.ListenAndServe(":3333", mux)

	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}
}
