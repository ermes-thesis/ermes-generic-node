package metadataManager

import (
	dataManagerShared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"metadataManager/propagation"
	"net/http"
)

func (m *MetadataManagerImpl) handleRouteRequestUpdateGroups() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		collectionId := r.PathValue("collection_id")

		m.log.Eval("Received group update new request for metadata", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI())
		m.log.Debug("Received group update new request for metadata", "collection_id", collectionId)

		var message propagation.GroupMessage
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			m.log.Error("Failed to decode request body", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "collection_id", collectionId, "error", err)
			http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusInternalServerError)
			return
		}

		m.log.Debug("Decoded group update message", "collection_id", collectionId, "user_access_groups", message.UserGroups, "groups_to_add", message.GroupsToAdd, "groups_to_remove", message.GroupsToRemove)

		resp, err := m.UpdateGroupsInCollection(collectionId, message.UserGroups, message.UserId, message.GroupsToAdd, message.GroupsToRemove)
		if err != nil {
			var unAuthErr *dataManagerShared.UnauthorizedError
			if errors.As(err, &unAuthErr) {
				m.log.Warn("Unauthorized group update attempt", "collection_id", collectionId, "user_access_groups", message.UserGroups, "error", err)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			m.log.Error("Failed to update groups in collection", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "collection_id", collectionId, "user_access_groups", message.UserGroups, "groups_to_add", message.GroupsToAdd, "groups_to_remove", message.GroupsToRemove, "error", err)
			http.Error(w, fmt.Sprintf("Internal Server Error: Failed to update groups in collection: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(resp)
		if err != nil {
			m.log.Error("Failed to encode response", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI(), "collection_id", collectionId, "new_group_set", resp.Groups, "new_group_sequence_num", resp.Sequence, "error", err)
			http.Error(w, fmt.Sprintf("Internal Server Error: Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}

		m.log.Info("Group update in collection's metadata handled successfully", "collection_id", collectionId)
		m.log.Debug("Group update in collection's metadata handled successfully", "new_groups", resp.Groups, "new_group_sequence_num", resp.Sequence)
		w.WriteHeader(http.StatusOK)
	}
}
