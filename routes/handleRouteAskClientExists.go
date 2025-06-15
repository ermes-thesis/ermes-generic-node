package routes

import (
	"clientrepo"
	"encoding/json"
	"errors"
	"fmt"
	"helper"
	"info"
	"io"
	"net/http"
	"os"
)

// Method called whenever a /ask-client-exists call is made.
// This method checks if the client exists in the memory of the current node,
// if the client does not exist ask his father.
// IF also the top of the network does not know it then return an error response.
func handleRouteAskClientExists(logger *io.Writer, serverinfo *info.Serverinfo, repo *clientrepo.ClientRepo) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(*logger, "got /ask-father-client-exists \n")

			// Read the body of the request

			fmt.Fprintf(*logger, "decoding body of request... ")

			var body info.SimpleClientIdRequest

			err := helper.DecodeJSONPrintlnBody(w, r, &body)

			if err != nil {
				var mr *helper.MalformedRequest
				if errors.As(err, &mr) {
					fmt.Fprintf(*logger, "The request could not be decoded correctly.\n")
					http.Error(w, mr.Msg, mr.Status)
				} else {
					fmt.Fprintln(*logger, err.Error())
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
				return
			}

			fmt.Fprintf(*logger, "done\n")

			// Once the body has been decoded check if the client is inside the current repository.
			// If it is present then return an ok to the caller.

			if clientrepo.IsUserPresent(repo, body.UserId) {

				fmt.Fprintf(*logger, "Client present in the current node, returing ok to caller.")

				w.WriteHeader(http.StatusOK)

				return
			}

			// In the case the client is not present in the current node ask the father.
			// If the node has no father then it means that it cannot be present in the network so return an error.

			fathername := serverinfo.Father

			if fathername == "nil" {

				fmt.Fprintf(*logger, "Father is at the top of the network. The client is not present. Return an error")

				w.WriteHeader(http.StatusForbidden)

				return
			}

			// If the client has a father contact him with the same request.

			reqUrl := fmt.Sprintf("http://localhost:%s/ask-client-exists", fathername)

			fmt.Fprintf(*logger, "Sending a request to the father, located at %s \n", reqUrl)

			req, err := http.NewRequest(http.MethodGet, reqUrl, nil)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when creating the http request: %s \n", err)

				w.WriteHeader(http.StatusBadRequest)

				return
			}

			req.Header.Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(body)

			res, err := http.DefaultClient.Do(req)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when contacting the father: %s \n", err)

				w.WriteHeader(http.StatusInternalServerError)

				return
			}

			fmt.Fprintf(*logger, "Got a response from the father! \n")

			// If the father had a positive answer add the id to the current node for for optimization purposes.

			if res.StatusCode == http.StatusOK && !clientrepo.IsUserPresent(repo, body.UserId) {
				clientrepo.AddUser(repo, body.UserId)
			}

			// Return the same status code of the father.

			w.WriteHeader(res.StatusCode)

		},
	)
}
