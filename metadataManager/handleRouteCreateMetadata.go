package metadataManager

import (
	"encoding/json"
	"fmt"
	"metadataManager/utils"
	"net/http"
	"os"
)

func (m *MetadataManagerImpl) handleRouteCreateMetadata() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(m.logger, "[%s], got /pvt-create-metadata \n", m.serverInfo.MetadataDomain)
		defer r.Body.Close()

		collectionId := r.PathValue("collection_id")
		reqMetadataDomain := r.Header.Get("X-ERMES-METADATA-DOMAIN")
		var meta utils.Metadata
		err := json.NewDecoder(r.Body).Decode(&meta)
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

		err = m.CreateMetadata(collectionId, meta, reqOrigin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when creating metadata: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}
