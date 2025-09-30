package metadataManager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

func (m *MetadataManagerImpl) handleRouteAddGroup() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.log.Infof("[%s], got /pvt-add-group \n", m.serverInfo.MetadataDomain)
		defer r.Body.Close()

		collectionId := r.PathValue("collection_id")
		reqMetadataDomain := r.Header.Get("X-ERMES-METADATA-DOMAIN")

		var groups GroupMessage
		err := json.NewDecoder(r.Body).Decode(&groups)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding metadata: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var reqOrigin PropagationOrigin
		if m.serverInfo.Father != nil && reqMetadataDomain == m.serverInfo.Father.MetadataDomain {
			reqOrigin = FROM_FATHER
		} else {
			reqOrigin = FROM_CHILD
		}

		err = m.AddGroupToCollection(collectionId, reqOrigin, groups...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when creating metadata: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}
