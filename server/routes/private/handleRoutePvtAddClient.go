package private

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"server/clientrepo"
	"server/helper"
	"server/info"
)

func HandleRoutePvtAddClient(logger *io.Writer, serverinfo *info.ServerInfo, repo *clientrepo.ClientRepo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*logger, "got /add-client \n")

		// The client is requesting to get a new user id.
		// Hence the request has to be forwarded to the top of the network since he can take decisions atomically.

		// If I am at the top of the network I can take a decision

		fathername := serverinfo.Father

		if fathername == nil {

			fmt.Fprintf(*logger, "Father is at the top of the network. The user will be created and the result returned.\n")

			userId, err := clientrepo.CreateUser(repo)

			if err != nil {
				fmt.Fprintf(*logger, "Error when creating the user.")

				w.WriteHeader(http.StatusInternalServerError)

				return

			}

			// The response will contain in its body the user id.

			w.Header().Set("Content-Type", "application/json")

			response := info.CreateSimpleClientIdRequest(userId)

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)

			return
		}

		// If I am not the top of the network I need to forward the request upwards and save the result.

		// If the client has a father contact him with the same request.

		reqUrl := fmt.Sprintf("http://localhost:%s/pvt-add-client", fathername)

		fmt.Fprintf(*logger, "Sending a request to the father, located at %s \n", reqUrl)

		req, err := http.NewRequest(http.MethodGet, reqUrl, nil)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when creating the http request: %s \n", err)

			w.WriteHeader(http.StatusBadRequest)

			return
		}

		req.Header.Set("X-ERMES-SECRET", serverinfo.Secret)

		res, err := http.DefaultClient.Do(req)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when contacting the father: %s \n", err)

			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		fmt.Fprintf(*logger, "Got a response from the father! \n")

		// reqDump, err := httputil.DumpResponse(res, true)
		// if err != nil {
		// 	fmt.Fprintf(os.Stderr, "Error when dumping the request: %s", err)
		// 	return
		// }

		// fmt.Printf("REQUEST:\n%s", string(reqDump))

		// The father will contain in its body the user id, so we need to deserialize it

		var body info.SimpleClientIdRequest

		err = helper.DecodeJSONResponseBody(w, res, &body)

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

		// Once it is deserialized save it locally and send the result back in the body.

		err = clientrepo.AddUser(repo, body.UserId)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when inserting the user in the local repo: %s \n", err)

			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		// Construct the response with the same body of what has been received

		// The response will contain in its body the user id.

		w.Header().Set("Content-Type", "application/json")

		w.WriteHeader(http.StatusOK)

		err = json.NewEncoder(w).Encode(body)
		if err != nil {
			fmt.Printf("error encoding response body: %s", err)
		}
	}
}
