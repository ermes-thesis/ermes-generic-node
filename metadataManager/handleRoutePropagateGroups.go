package metadataManager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

func (m *MetadataManagerImpl) handleRoutePropagateGroups() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.log.Infof("[%s], got /pvt-propagate-groups \n", m.serverInfo.MetadataDomain)
		defer r.Body.Close()

		collectionId := r.PathValue("collection_id")

		var message UpdateGroupResponse
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding metadata: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = m.SetCollectionGroups(collectionId, message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when propagating groups: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}
