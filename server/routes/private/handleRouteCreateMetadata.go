package private

import (
	"db"
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"net/http"
	"os"
	"server/info"
)

func HandleRouteCreateMetadata(logger *io.Writer, lookupMgr metadataManager.MetadataManager, serverInfo *info.ServerInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*logger, "got /create_metadata \n")

		collectionId := r.PathValue("collection_id")
		reqMetadataDomain := r.Header.Get("X-ERMES-METADATA-DOMAIN")
		var meta db.Metadata
		err := json.NewDecoder(r.Body).Decode(&meta)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding metadata: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var isReqFromFather metadataManager.PropagationOrigin
		if reqMetadataDomain == serverInfo.Father.MetadataDomain {
			isReqFromFather = metadataManager.FROM_FATHER
		} else {
			isReqFromFather = metadataManager.FROM_CHILD
		}

		err = lookupMgr.CreateMetadata(collectionId, meta, isReqFromFather)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when executing queries: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}
