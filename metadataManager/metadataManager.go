package metadataManager

import (
	"bytes"
	"db"
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/utils"
	"net/http"
	"os"
	"server/info"
	"slices"
	"strings"
)

type MetadataManager interface {
	CreateMetadata(collectionId string, metadata db.Metadata) error
	ExecuteQueries(query string, groups string, geolocation string) (map[string][]db.CollectionInfo, error)
}

type MetadataManagerImpl struct {
	redis              db.GenericDB
	logger             io.Writer
	templateCache      map[string]string
	serverInfo         *info.ServerInfo
	queryPropagator    *QueryPropagator
	customMetadataKeys []string
}

func NewMetadataManager(redis db.GenericDB, serverInfo *info.ServerInfo, logger io.Writer) MetadataManager {
	return &MetadataManagerImpl{
		redis:  redis,
		logger: logger,
		templateCache: map[string]string{
			"private_data_query": `
				{
				  "SELECT": "ALL",
				  "AS": "private_data",
				  "WHERE": {
					"AND": [
					  {
						"field": "owner",
						"eq": "12345"
					  },
					  {
						"field": "name",
						"eq": "<name>"
					  }
					]
				  }
				}
		`,
			"gt_data_query": `
				{
				  "SELECT": "ALL",
				  "AS": "gt_data",
				  "WHERE": {
					"AND": [
					  {
						"field": "owner",
						"eq": "12345"
					  },
					  {
						"field": "name",
						"eq": "DATI_PRIVATE"
					  },
					  {
						"field": "created_at",
						"gt": 1745747999
					  }
					]
				  }
				}
		`,
		},
		serverInfo:         serverInfo,
		queryPropagator:    NewQueryPropagator(serverInfo, &logger),
		customMetadataKeys: []string{},
	}
}

func (l MetadataManagerImpl) CreateMetadata(collectionId string, metadata db.Metadata) error {
	if metadata.Geolocation == l.serverInfo.MetadataDomain {
		var newMetadata []string
		for key, value := range metadata.CustomMetadata {
			if !slices.Contains(l.customMetadataKeys, key) {
				newMetadata = append(newMetadata, key, value)
			}
		}
		if len(newMetadata) > 0 {
			err := l.redis.AlterIndex(newMetadata...)
			if err != nil {
				fmt.Fprintf(l.logger, "Error altering index in Redis: %v\n", err)
				return err
			}
			l.customMetadataKeys = append(l.customMetadataKeys, newMetadata...)
		}

		err := l.redis.SetMetadata(collectionId, metadata)
		if err != nil {
			fmt.Fprintf(l.logger, "Error setting metadata in Redis: %v\n", err)
			return err
		}
	}
	body, err := json.Marshal(metadata)
	if err != nil {
		fmt.Fprintf(l.logger, "Error marshalling metadata: %v\n", err)
		return err
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%s/create_metadata/%s", l.serverInfo.Father, collectionId), bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(l.logger, "Error creating request: %v\n", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", l.serverInfo.Secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(l.logger, "Error sending request: %v\n", err)
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (l MetadataManagerImpl) ExecuteQueries(queryParams string, groupToken string, geolocation string) (map[string][]db.CollectionInfo, error) {
	// Execute the query locally if the server's metadata domain match the geolocation, it is its prefix or if the local node is a cloud node.
	//Otherwise, propagate the query to the cloud node or the father node.
	if strings.HasPrefix(geolocation, l.serverInfo.MetadataDomain) || l.serverInfo.Father == nil {
		result := make(map[string][]db.CollectionInfo)

		var queryMap map[string]map[string]any
		err := json.Unmarshal([]byte(queryParams), &queryMap)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when unmarshalling query parameters: %s \n", err)
			return nil, err
		}

		groups := []string{"groupC"}
		//TODO: decode the groupToken to get the groups

		for queryName, params := range queryMap {
			if q, ok := l.templateCache[queryName]; ok {
				var query utils.Query
				if err := json.Unmarshal([]byte(q), &query); err != nil {
					fmt.Fprintf(l.logger, "Error unmarshalling queryParams template: %v\n", err)
					return nil, err
				}

				err := query.ReplacePlaceholder(params)
				if err != nil {
					fmt.Fprintf(l.logger, "Error replacing placeholders in queryParams: %v\n", err)
					return nil, err
				}

				if err := query.Validate(); err != nil {
					fmt.Fprintf(l.logger, "Error validating queryParams: %v\n", err)
					return nil, err
				}

				collInfo, err := l.redis.SearchOnMetadata(query, groups)
				if err != nil {
					fmt.Fprintf(l.logger, "Error executing queryParams: %v\n", err)
					return nil, err
				}
				if len(collInfo) > 0 {
					result[query.As] = append(result[query.As], collInfo...)
				}
			}
		}
		return result, nil
	} else {
		return l.queryPropagator.Propagate(queryParams, groupToken, geolocation)
	}
}
