package metadataManager

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/propagation"
	"net/http"
)

func (m *MetadataManagerImpl) handleRoutePropagateGroups() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}()

		collectionId := r.PathValue("collection_id")

		m.log.Eval("Received group update propagation request", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI())
		m.log.Debug("Received group update propagation request", "collection_id", collectionId, "node_geolocation", m.serverInfo.MetadataDomain)

		var message propagation.UpdateGroupResponse
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			m.log.Error("Failed to decode group propagation payload", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "collection_id", collectionId, "node_geolocation", m.serverInfo.MetadataDomain, "error", err)
			http.Error(w, fmt.Sprintf("Internal Server Error: invalid request body: %v", err), http.StatusInternalServerError)
			return
		}

		m.log.Debug("Decoded group propagation payload", "collection_id", collectionId, "new_groups_set", message.Groups, "new_groups_sequence_num", message.Sequence)

		err = m.SetCollectionGroups(collectionId, message)
		if err != nil {
			m.log.Error("Failed to propagate group updates", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "collection_id", collectionId, "new_groups_set", message.Groups, "new_groups_sequence_num", message.Sequence, "error", err)
			http.Error(w, fmt.Sprintf("Internal Server Error: invalid request body: %v", err), http.StatusInternalServerError)
			return
		}

		m.log.Info("Group propagation completed successfully for the current node", "collection_id", collectionId, "new_groups_set", message.Groups, "new_groups_sequence_num", message.Sequence)
		w.WriteHeader(http.StatusOK)
	}
}
