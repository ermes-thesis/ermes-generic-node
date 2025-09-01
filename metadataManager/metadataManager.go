package metadataManager

import (
	"caches"
	"db"
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/utils"
	"os"
	"server/info"
	"slices"
	"strings"
)

// map[tmpl_name]map[param_name]param_value
type QueryTemplatesParamsMapping[T any] map[string]map[string]T

type MetadataManager interface {
	CreateMetadata(collectionId string, metadata db.Metadata, reqOrigin PropagationOrigin) error
	ExecuteQueries(functionName string, templateParamsMapping string, groups string, geolocation string) (map[string][]db.CollectionInfo, error)
}

type MetadataManagerImpl struct {
	redis              db.GenericDB
	logger             io.Writer
	templateCache      caches.FunctionCache
	serverInfo         *info.ServerInfo
	queryPropagator    QueryPropagator
	metadataPropagator MetadataPropagator
	customMetadataKeys []string
}

func NewMetadataManager(redis db.GenericDB, serverInfo *info.ServerInfo, logger io.Writer, cache caches.FunctionCache) MetadataManager {
	return &MetadataManagerImpl{
		redis:         redis,
		logger:        logger,
		templateCache: cache,
		//templateCache: map[string]string{
		//	"private_data_query": `
		//		{
		//		  "SELECT": "ALL",
		//		  "AS": "private_data",
		//		  "WHERE": {
		//			"AND": [
		//			  {
		//				"field": "owner",
		//				"eq": "12345"
		//			  },
		//			  {
		//				"field": "name",
		//				"eq": "<name>"
		//			  }
		//			]
		//		  }
		//		}
		//`,
		//	"gt_data_query": `
		//		{
		//		  "SELECT": "ALL",
		//		  "AS": "gt_data",
		//		  "WHERE": {
		//			"AND": [
		//			  {
		//				"field": "owner",
		//				"eq": "12345"
		//			  },
		//			  {
		//				"field": "name",
		//				"eq": "DATI_PRIVATE"
		//			  },
		//			  {
		//				"field": "created_at",
		//				"gt": 1745747999
		//			  }
		//			]
		//		  }
		//		}
		//`,
		//},
		serverInfo:         serverInfo,
		queryPropagator:    NewQueryPropagator(serverInfo, &logger),
		metadataPropagator: NewMetadataPropagator(serverInfo, &logger),
		customMetadataKeys: []string{},
	}
}

func (l MetadataManagerImpl) CreateMetadata(collectionId string, metadata db.Metadata, reqOrigin PropagationOrigin) error {
	if strings.HasPrefix(metadata.Geolocation, l.serverInfo.MetadataDomain) {
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

	err := l.metadataPropagator.Propagate(collectionId, metadata, reqOrigin)
	if err != nil {
		fmt.Fprintf(l.logger, "Error propagating metadata: %v\n", err)
		return err
	}

	return nil
}

func (l MetadataManagerImpl) ExecuteQueries(functionName string, templateParamsMapping string, groupToken string, geolocation string) (map[string][]db.CollectionInfo, error) {
	// Execute the query locally if the server's metadata domain match the geolocation, it is its prefix or if the local node is a cloud node.
	//Otherwise, propagate the query to the cloud node or the father node.
	if strings.HasPrefix(geolocation, l.serverInfo.MetadataDomain) || l.serverInfo.Father == nil {
		result := make(map[string][]db.CollectionInfo)

		var queryMap QueryTemplatesParamsMapping[string]
		err := json.Unmarshal([]byte(templateParamsMapping), &queryMap)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when unmarshalling query parameters: %s \n", err)
			return nil, err
		}

		groups := []string{"groupC"}
		//TODO: decode the groupToken to get the groups

		// Getting from the cache (or remotely) the templates of the function name
		if tmpls, err := l.templateCache.GetTemplate(functionName); err == nil {
			// For each template specified in the HTTP request
			for templateName, params := range queryMap {
				// Check if the template really exists, if that's the case continue the normal execution
				if query, ok := tmpls[templateName]; ok {
					err := query.ReplacePlaceholderFromStrings(params)
					if err != nil {
						fmt.Fprintf(l.logger, "Error replacing placeholders in templateParamsMapping: %v\n", err)
						return nil, err
					}

					if err := query.Validate(); err != nil {
						fmt.Fprintf(l.logger, "Error validating templateParamsMapping: %v\n", err)
						return nil, err
					}

					collInfo, err := l.redis.SearchOnMetadata(query, groups)
					if err != nil {
						fmt.Fprintf(l.logger, "Error executing templateParamsMapping: %v\n", err)
						return nil, err
					}
					if len(collInfo) > 0 {
						result[query.As] = append(result[query.As], collInfo...)
					}
				} else {
					return nil, fmt.Errorf("template '%s' specified in the request was not found for function %s", templateName, functionName)
				}
			}
			return result, nil
		}
		return nil, fmt.Errorf("error while retrieving templates for function %s from cache: %v", functionName, err)
	} else {
		return l.queryPropagator.Propagate(functionName, templateParamsMapping, groupToken, geolocation)
	}
}
