package routes

import (
	"fmt"
	"io"
	"net/http"
)

func handleRouteAccept(logger *io.Writer) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(*logger, "got /accept \n")

			// Once an accept arrives this means that a new client has connected.

			// At this moment we have to create a user Id.
			// In order to have an unique id we need to contact the higher levels.

			// So we need to do a "create user"
			io.WriteString(w, "You have connected succesfully! \n")
		},
	)
}
