package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"metadataManager/utils"
	"net/http"
	"os"
	"tokenDecoder"
)

func (s *SchedulerImpl) handleFunctionInvocation(tokenDecoder tokenDecoder.TokenDecoder, metadataManager metadataManager.MetadataManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(s.logger, "got function invocation \n")
		defer r.Body.Close()

		fnName := r.PathValue("name")
		fnVersion := r.PathValue("version")
		if fnVersion == "" {
			fnVersion = "latest"
		}

		fmt.Fprintf(s.logger, "Function name: %s, version: %s\n", fnName, fnVersion)

		queryParams := r.Header.Get("X-ERMES-QUERY")
		cachedQuery := r.Header.Get("X-ERMES-CACHED-QUERY")
		sequentialSeqNumberStr := r.Header.Get("X-ERMES-SEQUENTIAL-NUMBER")
		geolocation := r.Header.Get("X-ERMES-POSITION")
		groupToken := r.Header.Get("X-ERMES-GROUPS")

		groups, userId, err := tokenDecoder.DecodeGroupToken(groupToken)
		if err != nil {
			http.Error(w, "Error decoding group token", http.StatusUnauthorized)
			return
		}

		var collections map[string][]utils.CollectionInfo
		var toCache map[uint64]utils.QueryResult

		if cachedQuery != "" {
			var cachedQueries map[string][]utils.CollectionInfo
			err := json.Unmarshal([]byte(cachedQuery), &cachedQueries)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error unmarshalling cached query: %s \n", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			collections = cachedQueries
		}

		if queryParams != "" {
			res, err := metadataManager.ExecuteQueries(fnName, fnVersion, queryParams, groups, groupToken, geolocation)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when executing queries: %s \n", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			toCache = res

			if collections == nil {
				collections = make(map[string][]utils.CollectionInfo)
			}
			for _, v := range res {
				collections[v.As] = append(collections[v.As], v.Collections...)
			}
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
		functionResponse, resSeqNumber, err := s.Schedule(fnName, fnVersion, collections, sequentialSeqNumber, body, groups, userId, groupToken)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when scheduling function: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		jsonToCache, err := json.Marshal(toCache)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshalling query to cache: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-ERMES-CACHE", string(jsonToCache))
		w.Header().Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", resSeqNumber))
		w.WriteHeader(functionResponse.StatusCode)
		w.Write(functionResponse.Body)
	}
}
