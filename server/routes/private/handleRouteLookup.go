package private

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"net/http"
	"os"
	"server/info"
)

func HandleRouteLookup(logger *io.Writer, serverinfo *info.Serverinfo, lookupMgr metadataManager.MetadataManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*logger, "got /lookup \n")

		queryParams := r.Header.Get("X-ERMES-QUERY")
		var queryMap map[string]map[string]any
		err := json.Unmarshal([]byte(queryParams), &queryMap)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when unmarshalling query parameters: %s \n", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		groups := []string{"groupC"}
		queries, err := lookupMgr.ExecuteQueries(queryMap, groups)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when executing queries: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		} else if len(queries) == 0 {
			geolocation := r.Header.Get("X-ERMES-POSITION")

			if geolocation == "" {
				fmt.Fprintf(*logger, "No geolocation provided in the request header.\n")

				var receiver string
				if serverinfo.CloudNode == "" {
					fmt.Fprintf(*logger, "No cloud node configured, forwarding to parent node.\n")
					receiver = serverinfo.Father
				} else {
					fmt.Fprintf(*logger, "Using cloud node %s for the request.\n", serverinfo.CloudNode)
					receiver = serverinfo.CloudNode
				}

				req, err := http.NewRequest(r.Method, fmt.Sprintf("http://localhost:%s/lookup", receiver), nil)
				req.Header.Set("X-ERMES-SECRET", serverinfo.Secret)
				req.Header.Set("X-ERMES-POSITION", geolocation)
				req.Header.Set("X-ERMES-QUERY", queryParams)
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
		} else {
			fmt.Fprintf(*logger, "Queries executed successfully, returning results.\n")
			w.Header().Set("Content-Type", "application/json")
			response, err := json.Marshal(queries)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when marshalling response: %s \n", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Write(response)
			return
		}
	}
}
