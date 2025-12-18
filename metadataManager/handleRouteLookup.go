package metadataManager

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"server/helper"
	"tokenDecoder"

	routing "github.com/qiangxue/fasthttp-routing"
)

func (m *MetadataManagerImpl) handleRouteLookup(tokenDecoder tokenDecoder.TokenDecoder) routing.Handler {
	return func(ctx *routing.Context) error {

		fnName := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-REQ-FUNCTION"))
		fnVersion := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-REQ-FUNCTION-VERSION"))
		queryParams := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-QUERY"))
		geolocation := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-POSITION"))
		groupToken := helper.UnsafeString(ctx.Request.Header.Peek("X-ERMES-GROUPS"))

		m.log.Eval("Received collection lookup request", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "node_geolocation", m.serverInfo.MetadataDomain, "templates_geolocation", geolocation)
		m.log.Debug("Received collection lookup request", "fn_name", fnName, "fn_version", fnVersion, "templates_parameters", queryParams)

		groups, userId, err := tokenDecoder.DecodeGroupToken(groupToken)
		if err != nil {
			m.log.Error("Failed to decode group token", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "templates_parameters", queryParams, "node_geolocation", m.serverInfo.MetadataDomain, "templates_geolocation", geolocation, "error", err)
			ctx.Error("Unauthorized: invalid group token", http.StatusUnauthorized)
			return errors.New("unauthorized: invalid group token")
		}

		m.log.Debug("Decoded group token successfully", "user_access_groups", groups, "applicant_user_id", userId)

		queryResults, err := m.ExecuteQueries(fnName, fnVersion, queryParams, groups, userId, groupToken, geolocation)
		if err != nil {
			m.log.Error("Error executing queries", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "templates_parameters", queryParams, "user_access_groups", groups, "node_geolocation", m.serverInfo.MetadataDomain, "templates_geolocation", geolocation, "error", err)
			ctx.Error(fmt.Sprintf("Internal Server Error: error while executing queries: %v", err), http.StatusInternalServerError)
			return errors.New(fmt.Sprintf("Internal Server Error: error while executing queries: %v", err))
		}

		m.log.Info("Queries executed successfully", "fn_name", fnName, "fn_version", fnVersion, "node_geolocation", m.serverInfo.MetadataDomain)
		m.log.Debug("Queries executed successfully", "templates_parameters", queryParams, "user_access_groups", groups, "applicant_user_id", userId, "templates_geolocation", geolocation, "queries_results", fmt.Sprintf("%+v", queryResults))

		ctx.SetContentType("application/json")

		encoded, err := json.Marshal(queryResults)
		if err != nil {
			m.log.Error("Failed to convert response into json", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "templates_parameters", queryParams, "user_access_groups", groups, "node_geolocation", m.serverInfo.MetadataDomain, "templates_geolocation", geolocation, "error", err)
			ctx.Error(fmt.Sprintf("Internal Server Error: Failed to convert response into json: %v", err), http.StatusInternalServerError)
			return errors.New(fmt.Sprintf("Internal Server Error: Failed to convert response into json: %v", err))
		}

		_, err = ctx.Write(encoded)
		if err != nil {
			m.log.Error("Failed to encode response into body", "remote_addr", ctx.RemoteAddr(), "method", ctx.Method(), "path", ctx.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "templates_parameters", queryParams, "user_access_groups", groups, "node_geolocation", m.serverInfo.MetadataDomain, "templates_geolocation", geolocation, "error", err)
			ctx.Error(fmt.Sprintf("Internal Server Error: error while encoding query results in response: %v", err), http.StatusInternalServerError)
			return errors.New(fmt.Sprintf("Internal Server Error: Ferror while encoding query results in response: %v", err))
		}

		m.log.Info("Response sent successfully", "fn_name", fnName, "fn_version", fnVersion, "node_geolocation", m.serverInfo.MetadataDomain)
		return nil
	}
}
