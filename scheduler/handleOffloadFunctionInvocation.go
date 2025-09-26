package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/utils"
	"net/http"
	"os"
	"tokenDecoder"
)

func (s *SchedulerImpl) handleOffloadFunctionInvocation(tokenDecoder tokenDecoder.TokenDecoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*s.logger, "got offloaded function invocation \n")
		defer r.Body.Close()

		fnName := r.PathValue("name")
		fnVersion := r.PathValue("version")
		if fnVersion == "" {
			fnVersion = "latest"
		}

		fmt.Fprintf(*s.logger, "Function name: %s, version: %s\n", fnName, fnVersion)

		collectionJson := r.Header.Get("X-ERMES-COLLECTIONS")
		groupToken := r.Header.Get("X-ERMES-GROUPS")
		sequentialSeqNumberStr := r.Header.Get("X-ERMES-SEQUENTIAL-NUMBER")

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

		var sequentialSeqNumber uint64
		if sequentialSeqNumberStr != "" {
			_, err := fmt.Sscanf(sequentialSeqNumberStr, "%d", &sequentialSeqNumber)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error parsing sequential sequence number: %s \n", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		functionResponse, err := s.Schedule(fnName, fnVersion, collections, sequentialSeqNumber, body, groups, userId, groupToken)
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
