package metadataManager

import (
	dataManagerShared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
)

func (m *MetadataManagerImpl) handleRouteRequestUpdateGroups() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.log.Infof("[%s], got /pvt-request-update-groups \n", m.serverInfo.MetadataDomain)
		defer r.Body.Close()

		collectionId := r.PathValue("collection_id")

		var message GroupMessage
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding metadata: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var resp *UpdateGroupResponse
		if message.Action == Add {
			resp, err = m.AddGroupToCollection(collectionId, message.UserGroups, message.Groups...)
		} else {
			resp, err = m.RemoveGroupFromCollection(collectionId, message.UserGroups, message.Groups...)
		}
		if err != nil {
			var unAuthErr *dataManagerShared.UnauthorizedError
			if errors.As(err, &unAuthErr) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			fmt.Fprintf(os.Stderr, "Error when creating metadata: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(resp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error encoding response: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}
