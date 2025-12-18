package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"server/helper"
	"slices"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/model"
	routing "github.com/qiangxue/fasthttp-routing"
)

func (s *SchedulerImpl) handleFunctionInvocation(tokenDecoder tokenDecoder.TokenDecoder) routing.Handler {
	return func(ctx *routing.Context) error {

		fnName := ctx.Param("name")
		fnVersion := ctx.Param("version")
		if fnVersion == "" {
			fnVersion = "latest"
		}

		s.log.Eval("Received function invocation request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion)

		queryParams := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-QUERY"))
		cachedQuery := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-CACHED-QUERY"))
		sequentialSeqNumberStr := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-SEQUENTIAL-NUMBER"))
		geolocation := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-POSITION"))
		groupToken := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-GROUPS"))

		groups, userId, err := tokenDecoder.DecodeGroupToken(groupToken)
		s.log.Info("Token decoded successfully", "executing_user", userId, "user_acces_groups", groups)
		if err != nil {
			s.log.Error("Failed to decode group token", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number", sequentialSeqNumberStr, "queries_geolocation", geolocation, "error", err)
			ctx.Error("Error decoding group token", http.StatusUnauthorized)
			return errors.New("error decoding group token")
		}
		s.log.Debug("Received function invocation request", "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number", sequentialSeqNumberStr, "queries_geolocation", geolocation)

		var collections map[string][]model.CollectionInfo
		var toCache map[uint64]model.QueryResult

		if cachedQuery != "" {
			s.log.Info("Unmarshalling cached collections", "fn_name", fnName, "fn_version", fnVersion, "cached_collections", cachedQuery)
			var cachedQueries map[string][]model.CollectionInfo
			err := json.Unmarshal([]byte(cachedQuery), &cachedQueries)
			if err != nil {
				s.log.Error("Error unmarshalling cached query", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number", sequentialSeqNumberStr, "queries_geolocation", geolocation, "error", err)
				ctx.Error(fmt.Sprintf("error unmarshalling cached query: %v", err), http.StatusInternalServerError)
				return errors.New(fmt.Sprintf("error unmarshalling cached query: %v", err))
			}
			collections = cachedQueries
		}

		if queryParams != "" && queryParams != "{}" {
			s.log.Info("Executing metadata queries", "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams)
			s.log.Debug("Executing metadata queries", "user_access_groups", groups, "queries_geolocation", geolocation)
			res, err := s.metadataManager.ExecuteQueries(fnName, fnVersion, queryParams, groups, userId, groupToken, geolocation)
			if err != nil {
				s.log.Error("Error when executing metadata queries", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number", sequentialSeqNumberStr, "queries_geolocation", geolocation, "error", err)
				ctx.Error(fmt.Sprintf("error executing metadata queries: %v", err), http.StatusInternalServerError)
				return errors.New(fmt.Sprintf("error executing metadata queries: %v", err))
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
		if len(ctx.PostBody()) != 0 {
			body = ctx.PostBody()
		}

		var sequentialSeqNumber uint64
		if sequentialSeqNumberStr != "" {
			_, err := fmt.Sscanf(sequentialSeqNumberStr, "%d", &sequentialSeqNumber)
			if err != nil {
				s.log.Error("Error parsing sequential sequence number", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number_parsed", sequentialSeqNumber, "queries_geolocation", geolocation, "error", err)
				ctx.Error(fmt.Sprintf("error parsing sequential number: %v", err), http.StatusBadRequest)
				return errors.New(fmt.Sprintf("error parsing sequential number: %v", err))
			}
			s.log.Info("Parsing sequential sequence number", "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "sequential_sequence_number", sequentialSeqNumber)
		}

		functionResponse, resSeqNumber, err := s.Schedule(ScheduleRequest{
			FunctionName:        fnName,
			FunctionVersion:     fnVersion,
			Collections:         collections,
			SequentialSeqNumber: sequentialSeqNumber,
			GroupToken:          groupToken,
			HTTPMethod:          helper.UnsafeString(ctx.Method()),
			Payload:             body,
		}, "user", groups, userId)
		if err != nil {
			statusCode := http.StatusInternalServerError
			executionHostName := ""
			if functionResponse != nil {
				statusCode = functionResponse.StatusCode
				executionHostName = functionResponse.Hostname
			}
			s.log.Error("Error during function scheduling", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number_parsed", sequentialSeqNumber, "queries_geolocation", geolocation, "error", err)
			ctx.Response.Header.Set("X-ERMES-EXECUTION-HOST", executionHostName)
			ctx.Error(fmt.Sprintf("error executing function: %v", err), statusCode)
			return errors.New(fmt.Sprintf("error executing function: %v", err))
		}

		s.log.Info("Function scheduled and executed successfully", "fn_name", fnName, "fn_version", fnVersion, "fn_response", fmt.Sprintf("%+v", functionResponse), "new_sequential_sequence_num", sequentialSeqNumber)

		jsonToCache, err := json.Marshal(toCache)
		if err != nil {
			s.log.Error("Error marshalling query results to cache client-side", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "queries_res_to_cache", fmt.Sprintf("%+v", toCache), "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number_parsed", sequentialSeqNumber, "queries_geolocation", geolocation, "error", err)
			ctx.Error(fmt.Sprintf("error marshalling query results to cache: %v", err), http.StatusInternalServerError)
			return errors.New(fmt.Sprintf("error marshalling query results to cache: %v", err))
		}
		ctx.SetContentType("application/json")
		ctx.Response.Header.Set("X-ERMES-CACHE", helper.UnsafeString(jsonToCache))
		ctx.Response.Header.Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", resSeqNumber))
		ctx.Response.Header.Set("X-ERMES-EXECUTION-HOST", functionResponse.Hostname)
		ctx.SetStatusCode(functionResponse.StatusCode)
		if _, err = ctx.Write(functionResponse.Body); err != nil {
			s.log.Error("Error while writing HTTP response", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "query_parameters", queryParams, "cached_collections", cachedQuery, "sequential_seq_number_parsed", sequentialSeqNumber, "queries_geolocation", geolocation, "error", err)
			ctx.Error(fmt.Sprintf("error while writing HTTP response: %v", err), http.StatusInternalServerError)
			return errors.New(fmt.Sprintf("error while writing HTTP response: %v", err))
		}

		s.log.Info("Function invocation completed successfully", "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups)
		return nil
	}
}
