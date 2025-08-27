package private

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"net/http"
	"os"
)

func HandleRouteLookup(logger *io.Writer, lookupMgr metadataManager.MetadataManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*logger, "got /lookup \n")

		fnName := r.Header.Get("X-ERMES-REQ-FUNCTION")
		queryParams := r.Header.Get("X-ERMES-QUERY")
		geolocation := r.Header.Get("X-ERMES-POSITION")
		groups := r.Header.Get("X-ERMES-GROUPS")
		r.Body.Close()

		queries, err := lookupMgr.ExecuteQueries(fnName, queryParams, groups, geolocation)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when executing queries: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(*logger, "Queries executed successfully, returning results.\n")
		w.Header().Set("Content-Type", "application/json")
		response, err := json.Marshal(queries)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when marshalling response: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(response)
	}
}
