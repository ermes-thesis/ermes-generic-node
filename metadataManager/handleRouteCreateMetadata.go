package metadataManager

import (
	"encoding/json"
	"fmt"
	"github.com/ermes-thesis/ermes-common/model"
	"net/http"
)

func (m *MetadataManagerImpl) handleRouteCreateMetadata() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		collectionId := r.PathValue("collection_id")
		reqMetadataDomain := r.Header.Get("X-ERMES-METADATA-DOMAIN")

		m.log.Info("Received metadata creation request", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "node_geolocation", m.serverInfo.MetadataDomain)
		m.log.Debug("Received metadata creation request", "collection_id", collectionId, "previous_node_geolocation", reqMetadataDomain)

		var meta model.Metadata
		err := json.NewDecoder(r.Body).Decode(&meta)
		if err != nil {
			m.log.Error("Failed to decode metadata payload", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "collection_id", collectionId, "previous_node_geolocation", reqMetadataDomain, "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var reqOrigin PropagationOrigin
		if m.serverInfo.Father != nil && reqMetadataDomain == m.serverInfo.Father.MetadataDomain {
			reqOrigin = FROM_FATHER
		} else {
			reqOrigin = FROM_CHILD
		}

		m.log.Debug("Request body decoded successfully", "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", meta), "request_origin", reqOrigin, "previous_node_geolocation", reqMetadataDomain)

		err = m.CreateMetadata(collectionId, meta, reqOrigin)
		if err != nil {
			m.log.Error("Failed to create metadata", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "collection_id", collectionId, "metadata", fmt.Sprintf("%+v", meta), "request_origin", reqOrigin, "error", err)
			http.Error(w, fmt.Sprintf("Internal Server Error: failed to create metadata"), http.StatusInternalServerError)
			return
		}

		m.log.Info("Metadata created successfully", "collection_id", collectionId)
		m.log.Debug("Metadata created successfully", "metadata", fmt.Sprintf("%+v", meta), "request_origin", reqOrigin, "previous_node_geolocation", reqMetadataDomain)
		w.WriteHeader(http.StatusCreated)
	}
}
