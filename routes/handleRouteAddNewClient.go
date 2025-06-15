package routes

import (
	"clientrepo"
	"encoding/json"
	"fmt"
	"info"
	"io"
	"net/http"
)

func handleRouteAddClient(logger *io.Writer, repo *clientrepo.ClientRepo) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(*logger, "got /addclient \n")

			// Once an accept arrives this means that a new client has connected.

			// At this moment we have to create a user Id.
			// In order to have an unique id we need to contact the higher levels.

			// So we need to do a "create user"

			userId, err := clientrepo.CreateUser(repo)

			if err != nil {
				fmt.Fprintf(*logger, "Error when creating the user.")
				w.WriteHeader(http.StatusInternalServerError)
			}

			w.Header().Set("Content-Type", "application/json")

			response := info.CreateSimpleClientIdRequest(userId)

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)

			io.WriteString(w, "You have connected succesfully! \n")
		},
	)
}
