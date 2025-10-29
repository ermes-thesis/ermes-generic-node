package metadataManager

import (
	"caches"
	dataManagerShared "dataManager/shared"
	"db"
	"encoding/json"
	"errors"
	"fmt"
	"metadataManager/propagation"
	"metadataManager/types"
	"server"
	"strings"
	"sync"
	"tokenDecoder"

	"github.com/rs/xid"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
)

// QueryTemplatesParamsMapping map[tmpl_name]map[param_name]param_value
type QueryTemplatesParamsMapping[T any] map[string]map[string]T

type MetadataManager interface {
	CreateMetadata(collectionId string, metadata model.Metadata, reqOrigin propagation.PropagationOrigin) error
	DeleteMetadata(collectionId string, geolocation string, reqOrigin propagation.PropagationOrigin, isCollRecreated bool) error
	UpdateGroupsInCollection(collectionId string, userGroups []string, userId string, groupsToAdd []string, groupsToRemove []string) (*propagation.UpdateGroupResponse, error)
	SetCollectionGroups(collectionId string, message propagation.UpdateGroupResponse) error
	ExecuteQueries(functionName string, functionVersion string, templateParamsMapping string, groups []string, userId string, groupToken string, geolocation string) (map[uint64]types.QueryResult, error)
}

type MetadataManagerImpl struct {
	redis                        db.GenericDB
	log                          *logger.Logger
	templateCache                caches.FunctionCache
	serverInfo                   *server.ServerInfo
	queryPropagator              propagation.QueryPropagator
	createMetadataPropagator     propagation.CreateMetadataPropagator
	deleteMetadataPropagator     propagation.DeleteMetadataPropagator
	groupUpdateRequestPropagator propagation.GroupUpdateRequestPropagator
	updateGroupsPropagator       propagation.UpdateGroupsPropagator
	customMetadataKeys           sync.Map
}

func NewMetadataManager(redis db.GenericDB, serverInfo *server.ServerInfo, log *logger.Logger, cache caches.FunctionCache) (*MetadataManagerImpl, error) {
	log.Info("Initializing MetadataManager", "metadata_domain", serverInfo.MetadataDomain, "has_father", serverInfo.Father != nil)

	err := redis.CreateIndex()
	if err != nil {
		if strings.Contains(err.Error(), "Index already exists") {
			log.Warn("Redis index for metadata already exists")
		} else {
			log.Error("Error creating initial Redis index", "metadata_domain", serverInfo.MetadataDomain, "has_father", serverInfo.Father != nil, "error", err)
			return nil, err
		}
	}

	return &MetadataManagerImpl{
		redis:                        redis,
		log:                          log,
		templateCache:                cache,
		serverInfo:                   serverInfo,
		createMetadataPropagator:     propagation.NewCreateMetadataPropagator(serverInfo, log),
		deleteMetadataPropagator:     propagation.NewDeleteMetadataPropagator(serverInfo, log),
		queryPropagator:              propagation.NewQueryPropagator(serverInfo, log),
		groupUpdateRequestPropagator: propagation.NewGroupUpdateRequestPropagator(serverInfo, log),
		updateGroupsPropagator:       propagation.NewUpdateGroupsPropagator(serverInfo, log),
		customMetadataKeys:           sync.Map{},
	}, nil
}

func (m *MetadataManagerImpl) RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder) error {
	m.log.Info("Registering MetadataManager routes")

	err := server.AddCustomRoute("/pvt-lookup", true, m.handleRouteLookup(tokenDecoder))
	if err != nil {
		m.log.Error("Failed to register custom routes", "path", "/pvt-lookup", "error", err)
		return fmt.Errorf("Error while registering /pvt-lookup route: %v\n", err)
	}
	err = server.AddCustomRoute("/pvt-create-metadata/{collection_id}", true, m.handleRouteCreateMetadata())
	if err != nil {
		m.log.Error("Failed to register custom routes", "path", "/pvt-create-metadata/{collection_id}", "error", err)
		return fmt.Errorf("Error while registering /pvt-create-metadata/{collection_id} route: %v\n", err)
	}
	err = server.AddCustomRoute("/pvt-delete-metadata/{collection_id}", true, m.handleRouteDeleteMetadata())
	if err != nil {
		m.log.Error("Failed to register custom routes", "path", "/pvt-delete-metadata/{collection_id}", "error", err)
		return fmt.Errorf("Error while registering /pvt-delete-metadata/{collection_id} route: %v\n", err)
	}
	err = server.AddCustomRoute("/pvt-request-update-group/{collection_id}", true, m.handleRouteRequestUpdateGroups())
	if err != nil {
		m.log.Error("Failed to register custom routes", "path", "/pvt-request-update-group/{collection_id}", "error", err)
		return fmt.Errorf("Error while registering /pvt-request-update-group/{collection_id} route: %v\n", err)
	}
	err = server.AddCustomRoute("/pvt-propagate-groups/{collection_id}", true, m.handleRoutePropagateGroups())
	if err != nil {
		m.log.Error("Failed to register custom routes", "path", "/pvt-propagate-groups/{collection_id}", "error", err)
		return fmt.Errorf("Error while registering /pvt-propagate-groups/{collection_id} route: %v\n", err)
	}

	m.log.Info("MetadataManager routes successfully registered")
	return nil
}

func (m *MetadataManagerImpl) CreateMetadata(collectionId string, metadata model.Metadata, reqOrigin propagation.PropagationOrigin) error {
	m.log.Info("Creating metadata", "collection_id", collectionId, "origin", reqOrigin, "geolocation", metadata.Geolocation)
	m.log.Debug("Creating metadata", "metadata", fmt.Sprintf("%+v", metadata))

	if strings.HasPrefix(metadata.Geolocation, m.serverInfo.MetadataDomain) {
		for key := range metadata.CustomMetadata {
			if _, ok := m.customMetadataKeys.LoadOrStore(key, true); !ok {
				m.log.Info("New custom metadata added. Altering Redis index", "new_custom_metadata", key)
				err := m.redis.AlterIndex(key)
				if err != nil {
					m.log.Error("Error altering index in Redis", "collection_id", collectionId, "origin", reqOrigin, "geolocation", metadata.Geolocation, "metadata", fmt.Sprintf("%+v", metadata), "new_custom_metadata", key, "error", err)
					m.customMetadataKeys.Delete(key)
					return err
				}
			}
		}

		err := m.redis.SetMetadata(collectionId, metadata)
		if err != nil {
			m.log.Error("Error setting metadata in Redis", "collection_id", collectionId, "origin", reqOrigin, "geolocation", metadata.Geolocation, "metadata", fmt.Sprintf("%+v", metadata), "error", err)
			return err
		}
		m.log.Info("Metadata created locally successfully", "collection_id", collectionId)
	}

	err := m.createMetadataPropagator.Propagate(collectionId, metadata, reqOrigin)
	if err != nil {
		m.log.Error("Error propagating metadata", "collection_id", collectionId, "origin", reqOrigin, "geolocation", metadata.Geolocation, "metadata", fmt.Sprintf("%+v", metadata), "error", err)
		return err
	}

	m.log.Info("Metadata propagation completed", "collection_id", collectionId)
	return nil
}

func (m *MetadataManagerImpl) DeleteMetadata(collectionId string, geolocation string, reqOrigin propagation.PropagationOrigin, isCollRecreated bool) error {
	m.log.Info("Deleting metadata", "collection_id", collectionId)

	if strings.HasPrefix(geolocation, m.serverInfo.MetadataDomain) {
		err := m.redis.DeleteMetadata(collectionId, !isCollRecreated)
		if err != nil {
			m.log.Error("Error deleting metadata in Redis", "collection_id", collectionId, "geolocation", geolocation, "error", err)
			return err
		}
		m.log.Info("Metadata deleted locally successfully", "collection_id", collectionId)
	}

	err := m.deleteMetadataPropagator.Propagate(collectionId, geolocation, reqOrigin, isCollRecreated)
	if err != nil {
		m.log.Error("Error propagating metadata deletion", "collection_id", collectionId, "origin", reqOrigin, "metadata_geolocation", geolocation, "error", err)
		return err
	}

	m.log.Info("Metadata deletion propagation completed", "collection_id", collectionId)
	return nil
}

func (m *MetadataManagerImpl) UpdateGroupsInCollection(collectionId string, userGroups []string, userId string, groupsToAdd []string, groupsToRemove []string) (*propagation.UpdateGroupResponse, error) {
	m.log.Info("Updating groups in collection", "collection_id", collectionId, "user_access_groups", userGroups, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove)
	if m.serverInfo.Father == nil {
		exists, err := m.redis.MetadataExists(collectionId)
		if err != nil {
			m.log.Error("Error checking if metadata exists", "collection_id", collectionId, "error", err)
			return nil, err
		}

		if exists {
			authorized, err := m.redis.IsAuthorizedInMetadata(collectionId, userId, userGroups...)
			if err != nil {
				m.log.Error("Error verifying user group authorization", "collection_id", collectionId, "user_access_groups", userGroups, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "error", err)
				return nil, err
			}
			if !authorized {
				m.log.Warn("Unauthorized group update attempt", "collection_id", collectionId, "user_groups", userGroups)
				return nil, &dataManagerShared.UnauthorizedError{}
			}

			newGroups, seq, err := m.redis.UpdateGroupsInMetadata(collectionId, groupsToAdd, groupsToRemove)
			if err != nil {
				m.log.Error("Error updating metadata groups", "collection_id", collectionId, "user_access_groups", userGroups, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "error", err)
				return nil, err
			}
			respMessage := &propagation.UpdateGroupResponse{Groups: newGroups, Sequence: seq}
			m.log.Info("Groups in metadata updated successfully", "collection_id", collectionId)
			m.log.Debug("Groups in metadata updated successfully", "new_groups_set", newGroups, "new_groups_seq_num", seq)

			m.log.Info("Starting propagation of the group update to lower metadata domains", "collection_id", collectionId)
			go func() {
				routinePrefix := fmt.Sprintf("GO#%s", xid.New().String())

				metadata, err := m.redis.GetMetadata(collectionId)
				if err != nil {
					m.log.ErrorfWith("[%s] Error getting metadata for propagation", []any{routinePrefix}, "collection_id", collectionId, "user_access_groups", userGroups, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "error", err)
					return
				}

				err = m.updateGroupsPropagator.Propagate(collectionId, *respMessage, metadata.Geolocation)
				if err != nil {
					m.log.ErrorfWith("[%s] Error propagating group update", []any{routinePrefix}, "collection_id", collectionId, "response_message_groups", respMessage.Groups, "response_message_seq_num", respMessage.Sequence, "metadata_geolocation", metadata.Geolocation, "error", err)
					return
				}

				m.log.InfofWith("[%s] Groups update propagation completed", []any{routinePrefix}, "collection_id", collectionId)
			}()

			return respMessage, nil
		}

		m.log.Warn("Attempted to update non-existent metadata", "collection_id", collectionId)
		return nil, errors.New("collection does not exists")
	}

	m.log.Info("Offloading group update request to parent node", "collection_id", collectionId, "user_access_groups", userGroups, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove)
	return m.groupUpdateRequestPropagator.Propagate(collectionId, propagation.GroupMessage{UserGroups: userGroups, GroupsToAdd: groupsToAdd, GroupsToRemove: groupsToRemove, UserId: userId})
}

func (m *MetadataManagerImpl) SetCollectionGroups(collectionId string, message propagation.UpdateGroupResponse) error {
	m.log.Info("Setting collection groups in metadata", "collection_id", collectionId)
	m.log.Debug("Setting collection groups in metadata", "groups_set", message.Groups, "groups_seq_num", message.Sequence)

	metadata, err := m.redis.GetMetadata(collectionId)
	if err != nil {
		m.log.Error("Error getting metadata for propagation", "collection_id", collectionId, "groups_set", message.Groups, "groups_seq_num", message.Sequence, "error", err)
		return err
	}
	if metadata == nil {
		m.log.Warn("Metadata of the searched collection does not exists", "collection_id", collectionId)
		return errors.New("collection does not exists")
	}

	err = m.redis.SetGroupsInMetadata(collectionId, message.Sequence, message.Groups...)
	if err != nil {
		m.log.Error("Error setting metadata groups", "collection_id", collectionId, "groups_set", message.Groups, "groups_seq_num", message.Sequence, "metadata", fmt.Sprintf("%+v", metadata), "error", err)
		return err
	}

	m.log.Info("Metadata groups for collection successfully updated in the current node", "collection_id", collectionId, "metadata_domain", m.serverInfo.MetadataDomain)
	m.log.Info("Starting propagation of the groups update to lower metadata domains", "collection_id", collectionId)
	return m.updateGroupsPropagator.Propagate(collectionId, message, metadata.Geolocation)
}

func (m *MetadataManagerImpl) ExecuteQueries(functionName string, functionVersion string, templateParamsMapping string, groups []string, userId string, groupToken string, geolocation string) (map[uint64]types.QueryResult, error) {
	m.log.Info("Trying to execute queries", "fn_name", functionName, "fn_version", functionVersion, "user_id", userId)
	m.log.Debug("Trying to execute queries", "tmpl_param_mapping", templateParamsMapping, "user_access_groups", groups, "node_geolocation", m.serverInfo.MetadataDomain, "queries_geolocation", geolocation, "in_cloud", m.serverInfo.Father == nil)

	// Execute the query locally if the server's metadata domain match the geolocation, it is its prefix or if the local node is a cloud node.
	//Otherwise, propagate the query to the cloud node or the father node.
	if strings.HasPrefix(geolocation, m.serverInfo.MetadataDomain) || m.serverInfo.Father == nil {
		m.log.Info("Executing queries locally", "fn_name", functionName, "fn_version", functionVersion)
		result := make(map[uint64]types.QueryResult)

		var queryMap QueryTemplatesParamsMapping[string]
		err := json.Unmarshal([]byte(templateParamsMapping), &queryMap)
		if err != nil {
			m.log.Error("Error when unmarshalling template parameters mapping", "fn_name", functionName, "fn_version", functionVersion, "tmpl_param_mapping", templateParamsMapping, "user_access_groups", groups, "user_id", userId, "node_geolocation", m.serverInfo.MetadataDomain, "queries_geolocation", geolocation, "in_cloud", m.serverInfo.Father == nil, "error", err)
			return nil, err
		}

		// For each template specified in the HTTP request
		for templateName, params := range queryMap {
			m.log.InfofWith("Processing query template '%s'", []any{templateName}, "fn_name", functionName, "fn_version", functionVersion)
			m.log.DebugfWith("Processing query template '%s'", []any{templateName}, "tmpl_parameters", params)

			// Getting from the cache (or remotely) the templates of the function name
			if template, err := m.templateCache.GetTemplate(functionName, functionVersion, templateName); err == nil {
				query, err := template.ReplacePlaceholderFromStrings(params)
				if err != nil {
					m.log.ErrorfWith("Error replacing placeholders in query template '%s'", []any{templateName}, "fn_name", functionName, "fn_version", functionVersion, "tmpl_param_mapping", templateParamsMapping, "user_access_groups", groups, "user_id", userId, "node_geolocation", m.serverInfo.MetadataDomain, "queries_geolocation", geolocation, "in_cloud", m.serverInfo.Father == nil, "tmpl_parameters", params, "error", err)
					return nil, err
				}

				if err = query.Validate(); err != nil {
					m.log.ErrorfWith("Validation failed for query template '%s'", []any{templateName}, "fn_name", functionName, "fn_version", functionVersion, "tmpl_param_mapping", templateParamsMapping, "user_access_groups", groups, "user_id", userId, "node_geolocation", m.serverInfo.MetadataDomain, "queries_geolocation", geolocation, "in_cloud", m.serverInfo.Father == nil, "tmpl_parameters", params, "query", query, "error", err)
					return nil, err
				}

				collInfo, err := m.redis.SearchOnMetadata(*query, userId, groups)
				if err != nil {
					m.log.ErrorfWith("Error executing query template '%s'", []any{templateName}, "fn_name", functionName, "fn_version", functionVersion, "tmpl_param_mapping", templateParamsMapping, "user_access_groups", groups, "user_id", userId, "node_geolocation", m.serverInfo.MetadataDomain, "queries_geolocation", geolocation, "in_cloud", m.serverInfo.Father == nil, "query", query, "user_access_groups", groups, "error", err)
					return nil, err
				}

				m.log.InfofWith("Query template '%s' executed successfully", []any{templateName}, "fn_name", functionName, "fn_version", functionVersion)
				m.log.DebugfWith("Query template '%s' executed successfully", []any{templateName}, "query_AS", query.As, "retrieved_collections", fmt.Sprintf("%+v", collInfo))

				if len(collInfo) > 0 {
					hash := types.NewQueryIdentifier(templateName, params).Hash()
					result[hash] = types.QueryResult{
						As:          query.As,
						Collections: collInfo,
					}
				}
			} else {
				m.log.ErrorfWith("Error retrieving query template '%s'", []any{templateName}, "fn_name", functionName, "fn_version", functionVersion, "tmpl_param_mapping", templateParamsMapping, "user_access_groups", groups, "user_id", userId, "node_geolocation", m.serverInfo.MetadataDomain, "queries_geolocation", geolocation, "in_cloud", m.serverInfo.Father == nil, "error", err)
				return nil, fmt.Errorf("error while retrieving template %s for function %s:%s, with error: %v", templateName, functionName, functionVersion, err)
			}
		}

		m.log.Info("Query templates execution completed successfully", "fn_name", functionName, "fn_version", functionVersion)
		m.log.Debug("Query templates execution completed successfully", "queries_results", fmt.Sprintf("%+v", result))

		return result, nil
	}

	m.log.Info("Cannot execute queries locally, propagating the request", "fn_name", functionName, "fn_version", functionVersion)
	return m.queryPropagator.Propagate(functionName, functionVersion, templateParamsMapping, groupToken, geolocation)
}
