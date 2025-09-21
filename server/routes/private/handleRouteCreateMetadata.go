package private

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"metadataManager/utils"
	"net/http"
	"os"
	"server/info"
)

func HandleRouteCreateMetadata(logger *io.Writer, lookupMgr metadataManager.MetadataManager, serverInfo *info.ServerInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*logger, "[%s], got /create_metadata \n", serverInfo.MetadataDomain)
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

		var reqOrigin metadataManager.PropagationOrigin
		if serverInfo.Father != nil && reqMetadataDomain == serverInfo.Father.MetadataDomain {
			reqOrigin = metadataManager.FROM_FATHER
		} else {
			reqOrigin = metadataManager.FROM_CHILD
		}

		err = lookupMgr.CreateMetadata(collectionId, meta, reqOrigin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when creating metadata: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}
