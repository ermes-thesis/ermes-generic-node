package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/model"
)

const maxRedirects = 10

func (s *statefulPolicy) handleOffloadFunctionInvocation(tokenDecoder tokenDecoder.TokenDecoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}()

		fnName := r.PathValue("name")
		fnVersion := r.PathValue("version")
		if fnVersion == "" {
			fnVersion = "latest"
		}

		s.log.Eval("Received offloaded function invocation request", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion)

		collectionJson := r.Header.Get("X-ERMES-COLLECTIONS")
		groupToken := r.Header.Get("X-ERMES-GROUPS")
		sequentialSeqNumberStr := r.Header.Get("X-ERMES-SEQUENTIAL-NUMBER")
		hostname := r.Header.Get("X-ERMES-HOSTNAME")
		cumulativeLatency := r.Header.Get("X-ERMES-LATENCY")
		redirects := r.Header.Get("X-ERMES-REDIRECTS")
		nodes := r.Header.Get("X-ERMES-NODES")

		cumuLatInt, err := strconv.Atoi(cumulativeLatency)
		if err != nil {
			s.log.Error("Error converting latency from string to int", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			http.Error(w, "Error converting latency from string to int", http.StatusInternalServerError)
			return
		}

		redirectsInt, err := strconv.Atoi(redirects)
		if err != nil {
			s.log.Error("Error converting redirects from string to int", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			http.Error(w, "Error converting redirects from string to int", http.StatusInternalServerError)
			return
		}
		if redirectsInt > maxRedirects {
			s.log.Error("Max redirects exceeded for offloaded invocation", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "redirects", redirectsInt, "nodes", nodes)
			http.Error(w, "Max redirects exceeded for offloaded invocation", http.StatusLoopDetected)
			return
		}

		groups, userId, err := tokenDecoder.DecodeGroupToken(groupToken)
		s.log.Info("Token decoded successfully", "executing_user", userId, "user_acces_groups", groups)
		if err != nil {
			s.log.Error("Failed to decode group token for offloaded invocation", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			http.Error(w, "Error decoding group token", http.StatusUnauthorized)
			return
		}
		s.log.Debug("Received offloaded function invocation request", "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt)

		var collections map[string][]model.CollectionInfo
		err = json.Unmarshal([]byte(collectionJson), &collections)
		if err != nil {
			s.log.Error("Error unmarshalling collections JSON from header", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			http.Error(w, fmt.Sprintf("Error unmarshalling collections JSON from header: %v", err), http.StatusInternalServerError)
			return
		}

		body := []byte("{}")
		if r.Body != http.NoBody {
			body, err = io.ReadAll(r.Body)
			if err != nil {
				s.log.Error("Error reading request body", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
				http.Error(w, fmt.Sprintf("error reading request body: %v", err), http.StatusInternalServerError)
				return
			}
		}

		var sequentialSeqNumber uint64
		if sequentialSeqNumberStr != "" {
			_, err := fmt.Sscanf(sequentialSeqNumberStr, "%d", &sequentialSeqNumber)
			if err != nil {
				s.log.Error("Error parsing sequential sequence number", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
				http.Error(w, fmt.Sprintf("error parsing sequential number: %v", err), http.StatusBadRequest)
				return
			}
		}
		functionResponse, resSeqNumber, err := s.Schedule(ScheduleRequest{
			FunctionName:        fnName,
			FunctionVersion:     fnVersion,
			Collections:         collections,
			SequentialSeqNumber: sequentialSeqNumber,
			GroupToken:          groupToken,
			HTTPMethod:          r.Method,
			Payload:             body,
			CumulativeLatency:   cumuLatInt,
			NumRedirects:        redirectsInt,
			Nodes:               nodes + "," + s.serverInfo.HostName,
		}, hostname, groups, userId)
		if err != nil {
			statusCode := http.StatusInternalServerError
			executionHostName := ""
			if functionResponse != nil {
				statusCode = functionResponse.StatusCode
				executionHostName = functionResponse.Hostname
			}

			s.log.Error("Error executing offloaded function", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			w.Header().Set("X-ERMES-EXECUTION-HOST", executionHostName)
			http.Error(w, fmt.Sprintf("error executing offloaded function: %v", err), statusCode)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", resSeqNumber))
		w.Header().Set("X-ERMES-EXECUTION-HOST", functionResponse.Hostname)
		w.WriteHeader(functionResponse.StatusCode)
		if _, err = w.Write(functionResponse.Body); err != nil {
			s.log.Error("Error parsing sequential sequence number", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			http.Error(w, fmt.Sprintf("error while writing HTTP response: %v", err), http.StatusInternalServerError)
			return
		}

		s.log.Info("Function invocation completed successfully", "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups)

	}
}
