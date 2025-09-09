package public

import (
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"metadataManager/utils"
	"net/http"
	"os"
	"scheduler"
	"server/info"
)

func HandleFunctionInvocation(logger *io.Writer, serverinfo *info.ServerInfo, metadataManager metadataManager.MetadataManager, scheduler scheduler.Scheduler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(*logger, "got function invocation \n")
		defer r.Body.Close()

		fnName := r.PathValue("name")
		fnVersion := r.PathValue("version")
		if fnVersion == "" {
			fnVersion = "latest"
		}

		fmt.Fprintf(*logger, "Function name: %s, version: %s\n", fnName, fnVersion)

		queryParams := r.Header.Get("X-ERMES-QUERY")
		cachedQuery := r.Header.Get("X-ERMES-CACHED-QUERY")
		geolocation := r.Header.Get("X-ERMES-POSITION")
		groups := r.Header.Get("X-ERMES-GROUPS")

		var collections map[string][]utils.CollectionInfo
		var toCache map[uint64]utils.QueryResult

		if cachedQuery != "" {
			var cachedQueries map[string][]utils.CollectionInfo
			err := json.Unmarshal([]byte(cachedQuery), &cachedQueries)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error unmarshalling cached query: %s \n", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			collections = cachedQueries
		}

		if queryParams != "" {
			res, err := metadataManager.ExecuteQueries(fnName, fnVersion, queryParams, groups, geolocation)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error when executing queries: %s \n", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			toCache = res

			if collections == nil {
				collections = make(map[string][]utils.CollectionInfo)
			}
			for _, v := range res {
				collections[v.As] = append(collections[v.As], v.Collections...)
			}
		}

		httpCode, response, err := scheduler.Schedule(fnName, fnVersion, collections)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when scheduling function: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		jsonToCache, err := json.Marshal(toCache)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshalling query to cache: %s \n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-ERMES-CACHE", string(jsonToCache))
		w.WriteHeader(httpCode)
		w.Write(response)
	}
}
