package metadataManager

import (
	"caches"
	"db"
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/utils"
	"os"
	"server"
	"server/info"
	"slices"
	"strings"
	"tokenDecoder"
)

// map[tmpl_name]map[param_name]param_value
type QueryTemplatesParamsMapping[T any] map[string]map[string]T

type MetadataManager interface {
	CreateMetadata(collectionId string, metadata utils.Metadata, reqOrigin PropagationOrigin) error
	ExecuteQueries(functionName string, functionVersion string, templateParamsMapping string, groups []string, groupToken string, geolocation string) (map[uint64]utils.QueryResult, error)
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

func NewMetadataManager(redis db.GenericDB, serverInfo *info.ServerInfo, logger io.Writer, cache caches.FunctionCache) *MetadataManagerImpl {
	return &MetadataManagerImpl{
		redis:              redis,
		logger:             logger,
		templateCache:      cache,
		serverInfo:         serverInfo,
		queryPropagator:    NewQueryPropagator(serverInfo, &logger),
		metadataPropagator: NewMetadataPropagator(serverInfo, &logger),
		customMetadataKeys: []string{},
	}
}

func (m *MetadataManagerImpl) RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder) error {
	err := server.AddCustomRoute("/lookup", true, m.handleRouteLookup(tokenDecoder))
	if err != nil {
		return fmt.Errorf("Error while registering /lookup route: %v\n", err)
	}
	err = server.AddCustomRoute("/create_metadata/{collection_id}", true, m.handleRouteCreateMetadata())
	if err != nil {
		return fmt.Errorf("Error while registering /create_metadata/{collection_id} route: %v\n", err)
	}
	return nil
}

func (m *MetadataManagerImpl) CreateMetadata(collectionId string, metadata utils.Metadata, reqOrigin PropagationOrigin) error {
	if strings.HasPrefix(metadata.Geolocation, m.serverInfo.MetadataDomain) {
		var newMetadata []string
		for key, value := range metadata.CustomMetadata {
			if !slices.Contains(m.customMetadataKeys, key) {
				newMetadata = append(newMetadata, key, value)
			}
		}
		if len(newMetadata) > 0 {
			err := m.redis.AlterIndex(newMetadata...)
			if err != nil {
				fmt.Fprintf(m.logger, "Error altering index in Redis: %v\n", err)
				return err
			}
			m.customMetadataKeys = append(m.customMetadataKeys, newMetadata...)
		}

		err := m.redis.SetMetadata(collectionId, metadata)
		if err != nil {
			fmt.Fprintf(m.logger, "Error setting metadata in Redis: %v\n", err)
			return err
		}
	}

	err := m.metadataPropagator.Propagate(collectionId, metadata, reqOrigin)
	if err != nil {
		fmt.Fprintf(m.logger, "Error propagating metadata: %v\n", err)
		return err
	}

	return nil
}

func (m *MetadataManagerImpl) ExecuteQueries(functionName string, functionVersion string, templateParamsMapping string, groups []string, groupToken string, geolocation string) (map[uint64]utils.QueryResult, error) {
	// Execute the query locally if the server's metadata domain match the geolocation, it is its prefix or if the local node is a cloud node.
	//Otherwise, propagate the query to the cloud node or the father node.
	if strings.HasPrefix(geolocation, m.serverInfo.MetadataDomain) || m.serverInfo.Father == nil {
		result := make(map[uint64]utils.QueryResult)

		var queryMap QueryTemplatesParamsMapping[string]
		err := json.Unmarshal([]byte(templateParamsMapping), &queryMap)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when unmarshalling query parameters: %s \n", err)
			return nil, err
		}

		// For each template specified in the HTTP request
		for templateName, params := range queryMap {
			// Getting from the cache (or remotely) the templates of the function name
			if query, err := m.templateCache.GetTemplate(functionName, functionVersion, templateName); err == nil {
				err = query.ReplacePlaceholderFromStrings(params)
				if err != nil {
					fmt.Fprintf(m.logger, "Error replacing placeholders in templateParamsMapping: %v\n", err)
					return nil, err
				}

				if err = query.Validate(); err != nil {
					fmt.Fprintf(m.logger, "Error validating templateParamsMapping: %v\n", err)
					return nil, err
				}

				collInfo, err := m.redis.SearchOnMetadata(*query, groups)
				if err != nil {
					fmt.Fprintf(m.logger, "Error executing templateParamsMapping: %v\n", err)
					return nil, err
				}
				if len(collInfo) > 0 {
					hash := utils.NewQueryIdentifier(templateName, params).Hash()
					result[hash] = utils.QueryResult{
						As:          query.As,
						Collections: collInfo,
					}
				}
			} else {
				return nil, fmt.Errorf("error while retrieving template %s for function %s:%s, with error: %v", templateName, functionName, functionVersion, err)
			}
		}
		return result, nil
	} else {
		return m.queryPropagator.Propagate(functionName, functionVersion, templateParamsMapping, groupToken, geolocation)
	}
}
