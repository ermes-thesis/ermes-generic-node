package private

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"net/http"
	"os"
	"tokenDecoder"
)

func HandleRouteLookup(logger *io.Writer, tokenDecoder tokenDecoder.TokenDecoder, lookupMgr metadataManager.MetadataManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*logger, "got /lookup \n")

		fnName := r.Header.Get("X-ERMES-REQ-FUNCTION")
		fnVersion := r.Header.Get("X-ERMES-REQ-FUNCTION-VERSION")
		queryParams := r.Header.Get("X-ERMES-QUERY")
		geolocation := r.Header.Get("X-ERMES-POSITION")
		groupToken := r.Header.Get("X-ERMES-GROUPS")
		r.Body.Close()

		groups, _, err := tokenDecoder.DecodeGroupToken(groupToken)
		if err != nil {
			http.Error(w, "Error decoding group token", http.StatusUnauthorized)
			return
		}

		queryResults, err := lookupMgr.ExecuteQueries(fnName, fnVersion, queryParams, groups, groupToken, geolocation)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when executing queries: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(*logger, "Queries executed successfully, returning results.\n")
		w.Header().Set("Content-Type", "application/json")
		response, err := json.Marshal(queryResults)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when marshalling response: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(response)
	}
}
