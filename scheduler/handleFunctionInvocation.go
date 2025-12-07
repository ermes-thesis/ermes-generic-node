package scheduler

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/model"
)

func (s *SchedulerImpl) handleFunctionInvocation(tokenDecoder tokenDecoder.TokenDecoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		fnName := r.PathValue("name")
		fnVersion := r.PathValue("version")
		if fnVersion == "" {
			fnVersion = "latest"
		}

		s.log.Eval("Received function invocation request", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion)

		queryParams := r.Header.Get("X-ERMES-QUERY")
		cachedQuery := r.Header.Get("X-ERMES-CACHED-QUERY")
		sequentialSeqNumberStr := r.Header.Get("X-ERMES-SEQUENTIAL-NUMBER")
		geolocation := r.Header.Get("X-ERMES-POSITION")
		groupToken := r.Header.Get("X-ERMES-GROUPS")

		groups, userId, err := tokenDecoder.DecodeGroupToken(groupToken)
		s.log.Info("Token decoded successfully", "executing_user", userId, "user_acces_groups", groups)
		if err != nil {
			s.log.Error("Failed to decode group token", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number", sequentialSeqNumberStr, "queries_geolocation", geolocation, "error", err)
			http.Error(w, "Error decoding group token", http.StatusUnauthorized)
			return
		}
		s.log.Debug("Received function invocation request", "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number", sequentialSeqNumberStr, "queries_geolocation", geolocation)

		var collections map[string][]model.CollectionInfo
		var toCache map[uint64]model.QueryResult

		if cachedQuery != "" {
			s.log.Info("Unmarshalling cached collections", "fn_name", fnName, "fn_version", fnVersion, "cached_collections", cachedQuery)
			var cachedQueries map[string][]model.CollectionInfo
			err := json.Unmarshal([]byte(cachedQuery), &cachedQueries)
			if err != nil {
				s.log.Error("Error unmarshalling cached query", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number", sequentialSeqNumberStr, "queries_geolocation", geolocation, "error", err)
				http.Error(w, fmt.Sprintf("error unmarshalling cached query: %v", err), http.StatusInternalServerError)
				return
			}
			collections = cachedQueries
		}

		if queryParams != "" && queryParams != "{}" {
			s.log.Info("Executing metadata queries", "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams)
			s.log.Debug("Executing metadata queries", "user_access_groups", groups, "queries_geolocation", geolocation)
			res, err := s.metadataManager.ExecuteQueries(fnName, fnVersion, queryParams, groups, userId, groupToken, geolocation)
			if err != nil {
				s.log.Error("Error when executing metadata queries", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number", sequentialSeqNumberStr, "queries_geolocation", geolocation, "error", err)
				http.Error(w, fmt.Sprintf("error executing metadata queries: %v", err), http.StatusInternalServerError)
				return
			}
			toCache = res
			s.log.Debug("Metadata queries executed successfully", "queries_results", fmt.Sprintf("%+v", res))

			if collections == nil {
				collections = make(map[string][]model.CollectionInfo)
			}
			for _, v := range res {
				collections[v.As] = append(collections[v.As], v.Collections...)

				tmpMap := make(map[string]model.CollectionInfo)
				for _, coll := range collections[v.As] {
					tmpMap[coll.Id] = coll
				}

				collections[v.As] = slices.Collect(maps.Values(tmpMap))
			}
		}

		body := []byte("{}")
		if r.Body != http.NoBody {
			body, err = io.ReadAll(r.Body)
			if err != nil {
				s.log.Error("Error reading request body", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number", sequentialSeqNumberStr, "queries_geolocation", geolocation, "error", err)
				http.Error(w, fmt.Sprintf("error reading request body: %v", err), http.StatusInternalServerError)
				return
			}
		}

		var sequentialSeqNumber uint64
		if sequentialSeqNumberStr != "" {
			_, err := fmt.Sscanf(sequentialSeqNumberStr, "%d", &sequentialSeqNumber)
			if err != nil {
				s.log.Error("Error parsing sequential sequence number", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number_parsed", sequentialSeqNumber, "queries_geolocation", geolocation, "error", err)
				http.Error(w, fmt.Sprintf("error parsing sequential number: %v", err), http.StatusBadRequest)
				return
			}
			s.log.Info("Parsing sequential sequence number", "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "sequential_sequence_number", sequentialSeqNumber)
		}

		functionResponse, resSeqNumber, err := s.Schedule(ScheduleRequest{
			FunctionName:        fnName,
			FunctionVersion:     fnVersion,
			Collections:         collections,
			SequentialSeqNumber: sequentialSeqNumber,
			GroupToken:          groupToken,
			HTTPMethod:          r.Method,
			Payload:             body,
		}, "user", groups, userId)
		if err != nil {
			statusCode := http.StatusInternalServerError
			if functionResponse != nil {
				statusCode = functionResponse.StatusCode
			}
			s.log.Error("Error during function scheduling", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number_parsed", sequentialSeqNumber, "queries_geolocation", geolocation, "error", err)
			w.Header().Set("X-ERMES-EXECUTION-HOST", functionResponse.Hostname)
			http.Error(w, fmt.Sprintf("error executing function: %v", err), statusCode)
			return
		}

		s.log.Info("Function scheduled and executed successfully", "fn_name", fnName, "fn_version", fnVersion, "fn_response", fmt.Sprintf("%+v", functionResponse), "new_sequential_sequence_num", sequentialSeqNumber)

		jsonToCache, err := json.Marshal(toCache)
		if err != nil {
			s.log.Error("Error marshalling query results to cache client-side", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "queries_res_to_cache", fmt.Sprintf("%+v", toCache), "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number_parsed", sequentialSeqNumber, "queries_geolocation", geolocation, "error", err)
			http.Error(w, fmt.Sprintf("error marshalling query results to cache: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-ERMES-CACHE", string(jsonToCache))
		w.Header().Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", resSeqNumber))
		w.Header().Set("X-ERMES-EXECUTION-HOST", functionResponse.Hostname)
		w.WriteHeader(functionResponse.StatusCode)
		if _, err = w.Write(functionResponse.Body); err != nil {
			s.log.Error("Error while writing HTTP response", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number_parsed", sequentialSeqNumber, "queries_geolocation", geolocation, "error", err)
			http.Error(w, fmt.Sprintf("error while writing HTTP response: %v", err), http.StatusInternalServerError)
			return
		}

		s.log.Info("Function invocation completed successfully", "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups)
	}
}
