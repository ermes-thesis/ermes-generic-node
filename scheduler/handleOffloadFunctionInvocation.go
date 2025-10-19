package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/types"
	"net/http"
	"os"
	"strconv"
	"tokenDecoder"
)

func (s *SchedulerImpl) handleOffloadFunctionInvocation(tokenDecoder tokenDecoder.TokenDecoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.log.Info("got offloaded function invocation")
		defer r.Body.Close()

		fnName := r.PathValue("name")
		fnVersion := r.PathValue("version")
		if fnVersion == "" {
			fnVersion = "latest"
		}

		s.log.Infof("Function name: %s, version: %s", fnName, fnVersion)

		collectionJson := r.Header.Get("X-ERMES-COLLECTIONS")
		groupToken := r.Header.Get("X-ERMES-GROUPS")
		sequentialSeqNumberStr := r.Header.Get("X-ERMES-SEQUENTIAL-NUMBER")
		hostname := r.Header.Get("X-ERMES-HOSTNAME")
		cumulativeLatency := r.Header.Get("X-ERMES-LATENCY")

		cumuLatInt, err := strconv.Atoi(cumulativeLatency)
		if err != nil {
			http.Error(w, "Error converting latency from string to int", http.StatusInternalServerError)
		}

		groups, userId, err := tokenDecoder.DecodeGroupToken(groupToken)
		if err != nil {
			http.Error(w, "Error decoding group token", http.StatusUnauthorized)
			return
		}

		var collections map[string][]types.CollectionInfo
		err = json.Unmarshal([]byte(collectionJson), &collections)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshalling cached query: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		body := []byte("{}")
		if r.Body != http.NoBody {
			body, err = io.ReadAll(r.Body)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error reading request body: %s \n", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
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
		functionResponse, resSeqNumber, err := s.Schedule(fnName, fnVersion, collections, sequentialSeqNumber, hostname, body, groups, userId, groupToken, cumuLatInt)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when scheduling function: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", resSeqNumber))
		w.WriteHeader(functionResponse.StatusCode)
		w.Write(functionResponse.Body)
	}
}
