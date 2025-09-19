package private

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/utils"
	"net/http"
	"os"
	"scheduler"
	"tokenDecoder"
)

func HandleOffloadFunctionInvocation(logger *io.Writer, tokenDecoder tokenDecoder.TokenDecoder, scheduler scheduler.Scheduler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*logger, "got offloaded function invocation \n")
		defer r.Body.Close()

		fnName := r.PathValue("name")
		fnVersion := r.PathValue("version")
		if fnVersion == "" {
			fnVersion = "latest"
		}

		fmt.Fprintf(*logger, "Function name: %s, version: %s\n", fnName, fnVersion)

		collectionJson := r.Header.Get("X-ERMES-COLLECTIONS")
		groupToken := r.Header.Get("X-ERMES-GROUPS")

		groups, userId, err := tokenDecoder.DecodeGroupToken(groupToken)
		if err != nil {
			http.Error(w, "Error decoding group token", http.StatusUnauthorized)
			return
		}

		var collections map[string][]utils.CollectionInfo
		err = json.Unmarshal([]byte(collectionJson), &collections)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshalling cached query: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading request body: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		functionResponse, err := scheduler.Schedule(fnName, fnVersion, collections, body, groups, userId, groupToken)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when scheduling function: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(functionResponse.StatusCode)
		w.Write(functionResponse.Body)
	}
}
