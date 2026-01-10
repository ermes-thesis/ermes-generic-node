package metadataManager

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"tokenDecoder"
)

func (m *MetadataManagerImpl) handleRouteLookup(tokenDecoder tokenDecoder.TokenDecoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}()

		fnName := r.Header.Get("X-ERMES-REQ-FUNCTION")
		fnVersion := r.Header.Get("X-ERMES-REQ-FUNCTION-VERSION")
		queryParams := r.Header.Get("X-ERMES-QUERY")
		geolocation := r.Header.Get("X-ERMES-POSITION")
		groupToken := r.Header.Get("X-ERMES-GROUPS")

		m.log.Eval("Received collection lookup request", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "node_geolocation", m.serverInfo.MetadataDomain, "templates_geolocation", geolocation)
		m.log.Debug("Received collection lookup request", "fn_name", fnName, "fn_version", fnVersion, "templates_parameters", queryParams)

		groups, userId, err := tokenDecoder.DecodeGroupToken(groupToken)
		if err != nil {
			m.log.Error("Failed to decode group token", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "templates_parameters", queryParams, "node_geolocation", m.serverInfo.MetadataDomain, "templates_geolocation", geolocation, "error", err)
			http.Error(w, "Unauthorized: invalid group token", http.StatusUnauthorized)
			return
		}

		m.log.Debug("Decoded group token successfully", "user_access_groups", groups, "applicant_user_id", userId)

		queryResults, err := m.ExecuteQueries(fnName, fnVersion, queryParams, groups, userId, groupToken, geolocation)
		if err != nil {
			m.log.Error("Error executing queries", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "templates_parameters", queryParams, "user_access_groups", groups, "node_geolocation", m.serverInfo.MetadataDomain, "templates_geolocation", geolocation, "error", err)
			http.Error(w, fmt.Sprintf("Internal Server Error: error while executing queries: %v", err), http.StatusInternalServerError)
			return
		}

		m.log.Info("Queries executed successfully", "fn_name", fnName, "fn_version", fnVersion, "node_geolocation", m.serverInfo.MetadataDomain)
		m.log.Debug("Queries executed successfully", "templates_parameters", queryParams, "user_access_groups", groups, "applicant_user_id", userId, "templates_geolocation", geolocation, "queries_results", fmt.Sprintf("%+v", queryResults))

		w.Header().Set("Content-Type", "application/json")
		if err = json.NewEncoder(w).Encode(queryResults); err != nil {
			m.log.Error("Failed to encode query results in response", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "fn_name", fnName, "fn_version", fnVersion, "templates_parameters", queryParams, "user_access_groups", groups, "node_geolocation", m.serverInfo.MetadataDomain, "templates_geolocation", geolocation, "error", err)
			http.Error(w, fmt.Sprintf("Internal Server Error: error while encoding query results in response: %v", err), http.StatusInternalServerError)
			return
		}

		m.log.Info("Response sent successfully", "fn_name", fnName, "fn_version", fnVersion, "node_geolocation", m.serverInfo.MetadataDomain)
	}
}
