package metadataManager

import (
	"caches"
	dataManagerShared "dataManager/shared"
	"db"
	"encoding/json"
	"errors"
	"fmt"
	"metadataManager/types"
	"os"
	"server"
	"server/info"
	"slices"
	"strings"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
)

// QueryTemplatesParamsMapping map[tmpl_name]map[param_name]param_value
type QueryTemplatesParamsMapping[T any] map[string]map[string]T

type GroupMessage struct {
	UserGroups []string       `json:"userGroups"`
	Groups     []string       `json:"groups"`
	Action     MetadataAction `json:"action"`
}

type MetadataManager interface {
	CreateMetadata(collectionId string, metadata model.Metadata, reqOrigin PropagationOrigin) error
	AddGroupToCollection(collectionId string, userGroups []string, groupsToAdd ...string) (*UpdateGroupResponse, error)
	RemoveGroupFromCollection(collectionId string, userGroups []string, groupsToRemove ...string) (*UpdateGroupResponse, error)
	SetCollectionGroups(collectionId string, message UpdateGroupResponse) error
	ExecuteQueries(functionName string, functionVersion string, templateParamsMapping string, groups []string, groupToken string, geolocation string) (map[uint64]types.QueryResult, error)
}

type MetadataManagerImpl struct {
	redis                        db.GenericDB
	log                          *logger.Logger
	templateCache                caches.FunctionCache
	serverInfo                   *info.ServerInfo
	queryPropagator              QueryPropagator
	createMetadataPropagator     CreateMetadataPropagator
	groupUpdateRequestPropagator GroupUpdateRequestPropagator
	updateGroupsPropagator       UpdateGroupsPropagator
	customMetadataKeys           []string
}

func NewMetadataManager(redis db.GenericDB, serverInfo *info.ServerInfo, log *logger.Logger, cache caches.FunctionCache) *MetadataManagerImpl {
	return &MetadataManagerImpl{
		redis:                        redis,
		log:                          log,
		templateCache:                cache,
		serverInfo:                   serverInfo,
		createMetadataPropagator:     NewCreateMetadataPropagator(serverInfo, log),
		queryPropagator:              NewQueryPropagator(serverInfo, log),
		groupUpdateRequestPropagator: NewGroupUpdateRequestPropagator(serverInfo, log),
		customMetadataKeys:           []string{},
	}
}

func (m *MetadataManagerImpl) RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder) error {
	err := server.AddCustomRoute("/pvt-lookup", true, m.handleRouteLookup(tokenDecoder))
	if err != nil {
		return fmt.Errorf("Error while registering /pvt-lookup route: %v\n", err)
	}
	err = server.AddCustomRoute("/pvt-create-metadata/{collection_id}", true, m.handleRouteCreateMetadata())
	if err != nil {
		return fmt.Errorf("Error while registering /pvt-create-metadata/{collection_id} route: %v\n", err)
	}
	err = server.AddCustomRoute("/pvt-request-update-group/{collection_id}", true, m.handleRouteRequestUpdateGroups())
	if err != nil {
		return fmt.Errorf("Error while registering /pvt-request-update-group/{collection_id} route: %v\n", err)
	}
	err = server.AddCustomRoute("/pvt-propagate-groups/{collection_id}", true, m.handleRoutePropagateGroups())
	if err != nil {
		return fmt.Errorf("Error while registering /pvt-propagate-groups/{collection_id} route: %v\n", err)
	}

	return nil
}

func (m *MetadataManagerImpl) CreateMetadata(collectionId string, metadata model.Metadata, reqOrigin PropagationOrigin) error {
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
				m.log.Errorf("Error altering index in Redis: %v", err)
				return err
			}
			m.customMetadataKeys = append(m.customMetadataKeys, newMetadata...)
		}

		err := m.redis.SetMetadata(collectionId, metadata)
		if err != nil {
			m.log.Errorf("Error setting metadata in Redis: %v", err)
			return err
		}
	}

	err := m.createMetadataPropagator.Propagate(collectionId, metadata, reqOrigin)
	if err != nil {
		m.log.Errorf("Error propagating metadata: %v", err)
		return err
	}

	return nil
}

func (m *MetadataManagerImpl) AddGroupToCollection(collectionId string, userGroups []string, groups ...string) (*UpdateGroupResponse, error) {
	if m.serverInfo.Father == nil {
		exists, err := m.redis.MetadataExists(collectionId)
		if err != nil {
			return nil, err
		}

		if exists {
			authorized, err := m.redis.IsInMetadataGroups(collectionId, userGroups...)
			if err != nil {
				return nil, err
			}
			if !authorized {
				return nil, &dataManagerShared.UnauthorizedError{}
			}

			newGroups, seq, err := m.redis.AddGroupsToMetadata(collectionId, groups...)
			if err != nil {
				return nil, err
			}
			respMessage := &UpdateGroupResponse{
				Groups:   newGroups,
				Sequence: seq,
			}

			go func() {
				metadata, err := m.redis.GetMetadata(collectionId)
				if err != nil {
					m.log.Errorf("Error getting metadata for propagation: %v", err)
					return
				}

				err = m.updateGroupsPropagator.Propagate(collectionId, *respMessage, metadata.Geolocation)
				if err != nil {
					m.log.Errorf("Error propagating group update: %v", err)
				}
			}()

			return respMessage, nil
		}
		return nil, errors.New("collection does not exists")
	}

	return m.groupUpdateRequestPropagator.Propagate(collectionId, GroupMessage{Groups: groups, Action: Add})
}

func (m *MetadataManagerImpl) RemoveGroupFromCollection(collectionId string, userGroups []string, groups ...string) (*UpdateGroupResponse, error) {
	if m.serverInfo.Father == nil {
		exists, err := m.redis.MetadataExists(collectionId)
		if err != nil {
			return nil, err
		}

		if exists {
			authorized, err := m.redis.IsInMetadataGroups(collectionId, userGroups...)
			if err != nil {
				return nil, err
			}
			if !authorized {
				return nil, &dataManagerShared.UnauthorizedError{}
			}

			newGroups, seq, err := m.redis.RemoveGroupsFromMetadata(collectionId, groups...)
			if err != nil {
				return nil, err
			}
			respMessage := &UpdateGroupResponse{
				Groups:   newGroups,
				Sequence: seq,
			}

			go func() {
				metadata, err := m.redis.GetMetadata(collectionId)
				if err != nil {
					m.log.Errorf("Error getting metadata for propagation: %v", err)
					return
				}

				err = m.updateGroupsPropagator.Propagate(collectionId, *respMessage, metadata.Geolocation)
				if err != nil {
					m.log.Errorf("Error propagating group update: %v", err)
				}
			}()

			return respMessage, nil
		}
		return nil, errors.New("collection does not exists")
	}

	return m.groupUpdateRequestPropagator.Propagate(collectionId, GroupMessage{Groups: groups, Action: Remove})
}

func (m *MetadataManagerImpl) SetCollectionGroups(collectionId string, message UpdateGroupResponse) error {
	metadata, err := m.redis.GetMetadata(collectionId)
	if err != nil {
		m.log.Errorf("Error getting metadata for propagation: %v", err)
		return err
	}
	if metadata == nil {
		return errors.New("collection does not exists")
	}

	err = m.redis.SetGroupsInMetadata(collectionId, message.Sequence, message.Groups...)
	if err != nil {
		m.log.Errorf("Error setting groups: %v", err)
		return err
	}

	return m.updateGroupsPropagator.Propagate(collectionId, message, metadata.Geolocation)
}

func (m *MetadataManagerImpl) ExecuteQueries(functionName string, functionVersion string, templateParamsMapping string, groups []string, groupToken string, geolocation string) (map[uint64]types.QueryResult, error) {
	// Execute the query locally if the server's metadata domain match the geolocation, it is its prefix or if the local node is a cloud node.
	//Otherwise, propagate the query to the cloud node or the father node.
	if strings.HasPrefix(geolocation, m.serverInfo.MetadataDomain) || m.serverInfo.Father == nil {
		result := make(map[uint64]types.QueryResult)

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
					m.log.Errorf("Error replacing placeholders in templateParamsMapping: %v", err)
					return nil, err
				}

				if err = query.Validate(); err != nil {
					m.log.Errorf("Error validating templateParamsMapping: %v", err)
					return nil, err
				}

				collInfo, err := m.redis.SearchOnMetadata(*query, groups)
				if err != nil {
					m.log.Errorf("Error executing templateParamsMapping: %v", err)
					return nil, err
				}
				if len(collInfo) > 0 {
					hash := types.NewQueryIdentifier(templateName, params).Hash()
					result[hash] = types.QueryResult{
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
