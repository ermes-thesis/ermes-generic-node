package metadataManager

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/propagation"
	"net/http"
)

func (m *MetadataManagerImpl) handleRouteDeleteMetadata() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}()

		collectionId := r.PathValue("collection_id")
		reqMetadataDomain := r.Header.Get("X-ERMES-METADATA-DOMAIN")
		isCollRecreated := r.Header.Get("X-ERMES-COLLECTION-RECREATED") == "true"

		m.log.Eval("Received metadata deletion request", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "node_geolocation", m.serverInfo.MetadataDomain)
		m.log.Debug("Received metadata deletion request", "collection_id", collectionId, "previous_node_geolocation", reqMetadataDomain)

		var geolocation string
		err := json.NewDecoder(r.Body).Decode(&geolocation)
		if err != nil {
			m.log.Error("Failed to decode request payload", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "collection_id", collectionId, "previous_node_geolocation", reqMetadataDomain, "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var reqOrigin propagation.PropagationOrigin
		if m.serverInfo.Father != nil && reqMetadataDomain == m.serverInfo.Father.MetadataDomain {
			reqOrigin = propagation.FROM_FATHER
		} else {
			reqOrigin = propagation.FROM_CHILD
		}

		m.log.Debug("Request body decoded successfully", "collection_id", collectionId, "metadata_geolocation", geolocation, "request_origin", reqOrigin, "previous_node_geolocation", reqMetadataDomain)

		err = m.DeleteMetadata(collectionId, geolocation, reqOrigin, isCollRecreated)
		if err != nil {
			m.log.Error("Failed to delete metadata", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "collection_id", collectionId, "metadata_geolocation", geolocation, "request_origin", reqOrigin, "error", err)
			http.Error(w, fmt.Sprintf("Internal Server Error: failed to create metadata"), http.StatusInternalServerError)
			return
		}

		m.log.Info("Metadata deleted successfully", "collection_id", collectionId)
		m.log.Debug("Metadata deleted successfully", "metadata_geolocation", geolocation, "request_origin", reqOrigin, "previous_node_geolocation", reqMetadataDomain)
		w.WriteHeader(http.StatusOK)
	}
}
