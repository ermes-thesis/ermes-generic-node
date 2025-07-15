package private

import (
	"fmt"
	"io"
	"lookupManager"
	"net/http"
	"os"
	"server/clientrepo"
	"server/info"
)

func HandleRouteLookup(logger *io.Writer, serverinfo *info.Serverinfo, repo *clientrepo.ClientRepo, lookupMgr lookupManager.LookupManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*logger, "got /lookup \n")

		//lookupMgr.ExecuteQueries()

		geolocation := r.Header.Get("X-ERMES-POSITION")

		if geolocation == "" {
			fmt.Fprintf(*logger, "No geolocation provided in the request header.\n")

			var receiver string
			if serverinfo.CloudNode == "" {
				fmt.Fprintf(*logger, "No cloud node configured, forwarding to parent node.\n")
				receiver = serverinfo.Father
				return
			} else {
				fmt.Fprintf(*logger, "Using cloud node %s for the request.\n", serverinfo.CloudNode)
				receiver = serverinfo.CloudNode
			}

			req, err := http.NewRequest(r.Method, fmt.Sprintf("http://localhost:%s/lookup", receiver), nil)
			req.Header.Set("X-ERMES-SECRET", serverinfo.Secret)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when creating the http request: %s \n", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when contacting the cloud node: %s \n", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer res.Body.Close()

			w.WriteHeader(res.StatusCode)
			_, err = io.Copy(w, res.Body)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when copying the response body: %s \n", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

	}
}
