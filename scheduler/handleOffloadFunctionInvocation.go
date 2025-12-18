package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"server/helper"
	"strconv"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/model"
	routing "github.com/qiangxue/fasthttp-routing"
)

const maxRedirects = 10

func (s *statefulPolicy) handleOffloadFunctionInvocation(tokenDecoder tokenDecoder.TokenDecoder) routing.Handler {
	return func(ctx *routing.Context) error {

		fnName := ctx.Param("name")
		fnVersion := ctx.Param("version")
		if fnVersion == "" {
			fnVersion = "latest"
		}

		s.log.Eval("Received offloaded function invocation request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion)

		collectionJson := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-COLLECTIONS"))
		groupToken := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-GROUPS"))
		sequentialSeqNumberStr := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-SEQUENTIAL-NUMBER"))
		hostname := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-HOSTNAME"))
		cumulativeLatency := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-LATENCY"))
		redirects := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-REDIRECTS"))
		nodes := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-NODES"))

		cumuLatInt, err := strconv.Atoi(cumulativeLatency)
		if err != nil {
			s.log.Error("Error converting latency from string to int", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			ctx.Error("Error converting latency from string to int", http.StatusInternalServerError)
			return errors.New("error converting latency from string to int")
		}

		redirectsInt, err := strconv.Atoi(redirects)
		if err != nil {
			s.log.Error("Error converting redirects from string to int", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			ctx.Error("Error converting redirects from string to int", http.StatusInternalServerError)
			return errors.New("error converting redirects from string to int")
		}
		if redirectsInt > maxRedirects {
			s.log.Error("Max redirects exceeded for offloaded invocation", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "redirects", redirectsInt, "nodes", nodes)
			ctx.Error("Max redirects exceeded for offloaded invocation", http.StatusLoopDetected)
			return errors.New("max redirects exceeded for offloaded invocation")
		}

		groups, userId, err := tokenDecoder.DecodeGroupToken(groupToken)
		s.log.Info("Token decoded successfully", "executing_user", userId, "user_acces_groups", groups)
		if err != nil {
			s.log.Error("Failed to decode group token for offloaded invocation", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			ctx.Error("Error decoding group token", http.StatusUnauthorized)
			return errors.New("error decoding group token")
		}
		s.log.Debug("Received offloaded function invocation request", "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt)

		var collections map[string][]model.CollectionInfo
		err = json.Unmarshal([]byte(collectionJson), &collections)
		if err != nil {
			s.log.Error("Error unmarshalling collections JSON from header", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			ctx.Error(fmt.Sprintf("Error unmarshalling collections JSON from header: %v", err), http.StatusInternalServerError)
			return errors.New(fmt.Sprintf("Error unmarshalling collections JSON from header: %v", err))
		}

		body := []byte("{}")
		if len(ctx.PostBody()) != 0 {
			body = ctx.PostBody()
		}

		var sequentialSeqNumber uint64
		if sequentialSeqNumberStr != "" {
			_, err := fmt.Sscanf(sequentialSeqNumberStr, "%d", &sequentialSeqNumber)
			if err != nil {
				s.log.Error("Error parsing sequential sequence number", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
				ctx.Error(fmt.Sprintf("error parsing sequential number: %v", err), http.StatusBadRequest)
				return errors.New(fmt.Sprintf("error parsing sequential number: %v", err))
			}
		}
		functionResponse, resSeqNumber, err := s.Schedule(ScheduleRequest{
			FunctionName:        fnName,
			FunctionVersion:     fnVersion,
			Collections:         collections,
			SequentialSeqNumber: sequentialSeqNumber,
			GroupToken:          groupToken,
			HTTPMethod:          helper.UnsafeString(ctx.Method()),
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

			s.log.Error("Error executing offloaded function", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			ctx.Response.Header.Set("X-ERMES-EXECUTION-HOST", executionHostName)
			ctx.Error(fmt.Sprintf("error executing offloaded function: %v", err), statusCode)
			return errors.New(fmt.Sprintf("error executing offloaded function: %v", err))
		}

		ctx.Response.Header.Set("Content-Type", "application/json")
		ctx.Response.Header.Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", resSeqNumber))
		ctx.Response.Header.Set("X-ERMES-EXECUTION-HOST", functionResponse.Hostname)
		ctx.SetStatusCode(functionResponse.StatusCode)
		if _, err = ctx.Write(functionResponse.Body); err != nil {
			s.log.Error("Error while writing HTTP response", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups, "collections", collectionJson, "sequential_seq_number", sequentialSeqNumberStr, "request_from_hostname", hostname, "cumulative_latency", cumuLatInt, "error", err)
			ctx.Error(fmt.Sprintf("error while writing HTTP response: %v", err), http.StatusInternalServerError)
			return errors.New(fmt.Sprintf("error while writing HTTP response: %v", err))
		}

		s.log.Info("Function invocation completed successfully", "fn_name", fnName, "fn_version", fnVersion, "executing_user", userId, "user_access_groups", groups)
		return nil
	}
}
