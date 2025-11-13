package scheduler

import (
	"bytes"
	"caches"
	"config"
	datamanager "dataManager"
	"dataManager/messages"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"metadataManager"
	"metadataManager/propagation"
	"net/http"
	"server"
	"sync"
	"time"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
	"github.com/rs/xid"
)

type ScheduleRequest struct {
	FunctionName    string
	FunctionVersion string

	Collections         map[string][]model.CollectionInfo
	SequentialSeqNumber uint64
	GroupToken          string

	HTTPMethod        string
	Payload           []byte
	CumulativeLatency int
	NumRedirects      int
}

type fnResBody []byte

func (f fnResBody) String() string {
	return string(f)
}

type FunctionResult struct {
	StatusCode int
	Hostname   string
	Body       fnResBody
}

type Scheduler interface {
	RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder) error
	Schedule(scheduleRequest ScheduleRequest, reqHostname string, groups []string, userId string) (*FunctionResult, uint64, error)
}

type SchedulerImpl struct {
	Scheduler
	*baseScheduler
}

func NewScheduler(stateful bool, dataManager datamanager.DataManager, metadataManager metadataManager.MetadataManager, cache caches.FunctionCache, wasmExecutorPool wasm.ExecutorPool, serverInfo *server.ServerInfo, config config.SchedulerConfig, logger logger.Logger) Scheduler {
	var policy Scheduler

	sched := &baseScheduler{
		dataManager:      dataManager,
		metadataManager:  metadataManager,
		cache:            cache,
		wasmExecutorPool: wasmExecutorPool,
		serverInfo:       serverInfo,
		cfg:              config,
		log:              logger,
	}
	if stateful {
		policy = newStatefulPolicy(sched)
	} else {
		policy = newStatelessPolicy(sched)
	}
	return &SchedulerImpl{
		Scheduler:     policy,
		baseScheduler: sched,
	}
}

func (s *SchedulerImpl) RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder) error {
	s.log.Info("Registering custom routes for SchedulerImpl...")
	err := server.AddCustomRoute("/invoke-function/{name}/{version}", false, s.handleFunctionInvocation(tokenDecoder))
	if err != nil {
		s.log.Error("Failed to register custom routes", "path", "/invoke-function/{name}/{version}", "error", err)
		return fmt.Errorf("Error while registering /invoke-function/{name}/{version}: %v\n", err)
	}
	err = server.AddCustomRoute("/invoke-function/{name}", false, s.handleFunctionInvocation(tokenDecoder))
	if err != nil {
		s.log.Error("Failed to register custom routes", "path", "/invoke-function/{name}", "error", err)
		return fmt.Errorf("Error while registering /invoke-function/{name}: %v\n", err)
	}

	return s.Scheduler.RegisterRoutes(server, tokenDecoder)
}

type baseScheduler struct {
	log              logger.Logger
	dataManager      datamanager.DataManager
	metadataManager  metadataManager.MetadataManager
	cache            caches.FunctionCache
	wasmExecutorPool wasm.ExecutorPool
	serverInfo       *server.ServerInfo
	cfg              config.SchedulerConfig
}

func (s *baseScheduler) executeLocally(scheduleRequest ScheduleRequest, reqHostname string, groups []string, userId string, sequentialCollId string, seqColLock *sync.RWMutex, writeSeqCollection bool) (*FunctionResult, uint64, error) {
	var collIds []string
	for _, cols := range scheduleRequest.Collections {
		for _, col := range cols {
			collIds = append(collIds, col.Id)
		}
	}
	s.log.Info("Updating collections latest usage before function execution", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collections_involved", collIds)
	s.dataManager.GetMigrationManager().UpdateCollectionsLatestUsage(collIds)

	s.log.Debug("Acquiring wasm executor from pool", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
	executor := s.wasmExecutorPool.Acquire()

	backoff := s.cfg.StartingExecutorsBackoff
	for i := 0; i < s.cfg.MaxExecutorsRetries && executor == nil; i++ {
		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		time.Sleep(backoff + jitter)
		s.log.Debug("Retry acquiring wasm executor from pool", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)

		backoff *= 2
		if backoff > s.cfg.MaxExecutorsBackoff {
			backoff = s.cfg.MaxExecutorsBackoff
		}
		executor = s.wasmExecutorPool.Acquire()
	}
	if executor == nil {
		s.log.Error("Local execution failed. The WASM pool was too busy and there weren't any WASM executor available", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
		return nil, 0, fmt.Errorf("local execution failed. The WASM pool was too busy and there weren't any WASM executor available")
	}

	defer func() {
		executor.Release()
		s.log.Debug("Released wasm executor back to pool", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
	}()

	ctx := WasmContext{
		UserId:      userId,
		GroupList:   groups,
		Collections: scheduleRequest.Collections,
	}
	ctxJson, err := json.Marshal(ctx)
	if err != nil {
		s.log.Error("Failed to marshal wasm context for function execution", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "request_hostname", reqHostname, "with_sequential_collection", sequentialCollId != "", "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "error", err)
		return nil, 0, fmt.Errorf("error marshalling wasm context: %v", err)
	}

	wasmBinary, err := s.cache.GetBinary(scheduleRequest.FunctionName, scheduleRequest.FunctionVersion)
	if err != nil {
		s.log.Error("Failed to retrieve function binary", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "request_hostname", reqHostname, "with_sequential_collection", sequentialCollId != "", "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "error", err)
		return nil, 0, fmt.Errorf("error retrieving function binary: %v", err)
	}

	s.log.Info("Invoking function locally", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
	startTime := time.Now()
	resp := executor.Invoke(wasmBinary, "entry", scheduleRequest.Payload, ctxJson)
	s.log.Info("Function executed successfully", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_duration", time.Since(startTime).Nanoseconds())
	s.log.Debug("Function executed successfully", "fn_response", fmt.Sprintf("%+v", resp))

	repBody, err := json.Marshal(resp.Response)
	if err != nil {
		s.log.Error("Failed to marshal function response", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "request_hostname", reqHostname, "with_sequential_collection", sequentialCollId != "", "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "error", err)
		return nil, 0, fmt.Errorf("error marshalling response body: %v", err)
	}
	result := &FunctionResult{
		StatusCode: resp.HttpCode,
		Hostname:   s.serverInfo.HostName,
		Body:       repBody,
	}

	if resp.CacheUpdates == nil {
		if sequentialCollId != "" {
			seqNumber, err := s.dataManager.GetCollectionVectorClockValue(sequentialCollId)
			s.log.Info("No function cache updates to commit, returning directly the result", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_result", fmt.Sprintf("%+v", result), "new_sequential_collection_seq_num", seqNumber)
			return result, seqNumber, err
		}
		s.log.Info("No function cache updates to commit, returning directly the result", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_result", fmt.Sprintf("%+v", result))
		return result, 0, nil
	}

	var seqNumber uint64
	if sequentialCollId != "" {
		s.log.Debug("Processing sequential collection cache updates", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId)
		if update, ok := resp.CacheUpdates.CollectionUpdates[sequentialCollId]; ok {
			if !writeSeqCollection {
				s.log.Warn("DEV_ERROR: Function tried to update sequential collection without having write access")
				return nil, 0, fmt.Errorf("DEV_ERROR: Function tried to update sequential collection without having write access")
			}

			if update.Deleted {
				// TODO: Capire se è worth farla asincronamente (DeleteMetadata)
				s.log.Debug("Requested deletion for sequential collection in executed function", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId)
				err = s.metadataManager.DeleteMetadata(sequentialCollId, update.Geolocation, propagation.FROM_LOCAL, update.Recreated)
				if err != nil {
					s.log.Error("Failed to delete sequential collection metadata", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "request_hostname", reqHostname, "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "is_coll_recreated", update.Recreated, "error", err)
					return nil, 0, fmt.Errorf("error deleting sequential collection metadata: %v", err)
				}

				err = s.dataManager.RemoveCollectionAndPropagate(sequentialCollId, groups, userId)
				if err != nil {
					s.log.Error("Failed to remove sequential collection", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "request_hostname", reqHostname, "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "error", err)
					return nil, 0, fmt.Errorf("error removing sequential collection: %v", err)
				}

			} else {
				var recordNames []string
				var recordValues []string
				for recordName, recordValue := range update.DirtyRecords {
					recordNames = append(recordNames, recordName)
					recordValues = append(recordValues, recordValue)
				}
				if len(recordNames) > 0 {
					updates := []messages.CollectionUpdate{{
						CollectionId: sequentialCollId,
						RecordNames:  recordNames,
						RecordValues: recordValues,
						IsDelete:     false,
					}}
					s.log.Debug("Sequential collection dirty records commits prepared", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "record_updates", fmt.Sprintf("%+v", updates))
					err = s.dataManager.UpdateCollectionsRecordsAndPropagate(updates, groups, userId)
					if err != nil {
						s.log.Error("Failed to update sequential collection records", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "collection_updates", fmt.Sprintf("%+v", updates), "sequential_collection_write_access", writeSeqCollection, "error", err)
						return nil, 0, fmt.Errorf("error updating sequential collection records: %v", err)
					}
					s.log.Debug("Sequential collection records updated and propagated", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "record_updates", fmt.Sprintf("%+v", updates))
				}
				if len(update.DeletedRecords) > 0 {
					updates := []messages.CollectionUpdate{{
						CollectionId: sequentialCollId,
						RecordNames:  update.DeletedRecords,
						IsDelete:     true,
					}}
					s.log.Debug("Sequential collection delete records commits prepared", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "records_to_delete", fmt.Sprintf("%+v", updates))
					err = s.dataManager.UpdateCollectionsRecordsAndPropagate(updates, groups, userId)
					if err != nil {
						s.log.Error("Failed to delete sequential collection records", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "collection_updates", fmt.Sprintf("%+v", updates), "sequential_collection_write_access", writeSeqCollection, "error", err)
						return nil, 0, fmt.Errorf("error deleting sequential collection records: %v", err)
					}
					s.log.Debug("Sequential collection delete records deleted and propagated", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "records_to_delete", fmt.Sprintf("%+v", updates))
				}

				var groupsToAdd []string
				var groupsToRemove []string
				for group, toAdd := range update.CollectionGroups {
					if toAdd {
						groupsToAdd = append(groupsToAdd, group)
					} else {
						groupsToRemove = append(groupsToRemove, group)
					}
				}

				if len(groupsToAdd) > 0 || len(groupsToRemove) > 0 {
					s.log.Debug("Sequential collection group changes calculated", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove)
					updateResp, err := s.metadataManager.UpdateGroupsInCollection(sequentialCollId, groups, userId, groupsToAdd, groupsToRemove)
					if err != nil {
						s.log.Error("Failed to update sequential collection groups", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "sequential_collection_write_access", writeSeqCollection, "error", err)
						return nil, 0, fmt.Errorf("error updating sequential collection groups: %v", err)
					}
					s.log.Debug("Metadata groups updated successfully for sequential collection", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "groups_update_response", fmt.Sprintf("%+v", updateResp))

					s.log.Debug("Starting sequential collection groups update and propagation", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "groups_update_response", fmt.Sprintf("%+v", updateResp))
					err = s.dataManager.UpdateCollectionsGroupsAndPropagate([]messages.CollectionGroupUpdate{{
						CollectionId: sequentialCollId,
						GroupNames:   updateResp.Groups,
						Sequence:     updateResp.Sequence,
					}})
					if err != nil {
						s.log.Error("Failed to propagate the updates to sequential collection groups", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "sequential_collection_write_access", writeSeqCollection, "error", err)
						return nil, 0, s.log.ErrorfErr("error propagating sequential collection group updates: %v", err)
					}
				}
			}
			delete(resp.CacheUpdates.CollectionUpdates, sequentialCollId)
			s.log.Debug("Removed sequential collection updates from cache updates map since it has been processed")
		}

		seqNumber, err = s.dataManager.GetCollectionVectorClockValue(sequentialCollId)
		if err != nil {
			s.log.Error("Error getting sequential collection vector clock value", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "request_hostname", reqHostname, "sequential_collection_id", sequentialCollId, "sequential_collection_access_write", writeSeqCollection, "error", err)
			return nil, 0, s.log.ErrorfErr("error getting sequential collection vector clock value: %v", err)
		}
		s.log.Debug("Obtained new sequential collection sequence number", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "new_sequence_number", seqNumber)
	}

	// Process eventual updates asynchronously
	go func() {
		routinePrefix := fmt.Sprintf("GO#%s", xid.New().String())

		s.log.InfofWith("[%s] Starting goroutine to propagate eventual collection updates", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups)
		var updateMessage []messages.CollectionUpdate
		var groupUpdateMessage []messages.CollectionGroupUpdate

		for collId, update := range resp.CacheUpdates.CollectionUpdates {
			s.log.DebugfWith("[%s] Processing collection cache update", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", userId, "collection_id", collId, "update", fmt.Sprintf("%+v", update))
			if update.Deleted {
				go func(collId string) {
					routinePrefix := fmt.Sprintf("GO#%s", xid.New().String())

					// TODO: Capire se è worth farla asincronamente (DeleteMetadata)
					s.log.Debug("Requested deletion for sequential collection in executed function", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId)
					err = s.metadataManager.DeleteMetadata(sequentialCollId, update.Geolocation, propagation.FROM_LOCAL, update.Recreated)
					if err != nil {
						s.log.Error("Failed to delete sequential collection metadata", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(scheduleRequest.Payload), "request_hostname", reqHostname, "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "is_coll_recreated", update.Recreated, "error", err)
					}

					err := s.dataManager.RemoveCollectionAndPropagate(collId, groups, userId)
					if err != nil {
						s.log.ErrorfWith("[%s] Failed to delete or propagate the deletion of collection", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collection_id", collId, "update", fmt.Sprintf("%+v", update), "error", err)
					}
					s.log.DebugfWith("[%s] Removed collection asynchronously", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collection_id", collId, "update", fmt.Sprintf("%+v", update))
				}(collId)
				continue
			}

			if len(update.DeletedRecords) > 0 {
				updateMessage = append(updateMessage, messages.CollectionUpdate{
					CollectionId: collId,
					RecordNames:  update.DeletedRecords,
					IsDelete:     true,
				})
			}

			var recordNames []string
			var recordValues []string
			for recordName, recordValue := range update.DirtyRecords {
				recordNames = append(recordNames, recordName)
				recordValues = append(recordValues, recordValue)
			}
			if len(recordNames) > 0 {
				updateMessage = append(updateMessage, messages.CollectionUpdate{
					CollectionId: collId,
					RecordNames:  recordNames,
					RecordValues: recordValues,
					IsDelete:     false,
				})
			}

			var groupsToAdd []string
			var groupsToRemove []string
			for group, toAdd := range update.CollectionGroups {
				if toAdd {
					groupsToAdd = append(groupsToAdd, group)
				} else {
					groupsToRemove = append(groupsToRemove, group)
				}
			}

			if len(groupsToAdd) > 0 || len(groupsToRemove) > 0 {
				updateResp, err := s.metadataManager.UpdateGroupsInCollection(collId, groups, userId, groupsToAdd, groupsToRemove)
				if err != nil {
					s.log.ErrorfWith("[%s] Failed updating groups in eventual collection", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collection_id", collId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "error", err)
					continue
				}
				groupUpdateMessage = append(groupUpdateMessage, messages.CollectionGroupUpdate{
					CollectionId: sequentialCollId,
					GroupNames:   updateResp.Groups,
					Sequence:     updateResp.Sequence,
				})
			}
		}

		if len(updateMessage) > 0 {
			s.log.DebugfWith("[%s] Propagating eventual collection updates", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collection_updates", fmt.Sprintf("%+v", updateMessage))
			err = s.dataManager.UpdateCollectionsRecordsAndPropagate(updateMessage, groups, userId)
			if err != nil {
				s.log.ErrorfWith("[%s] Failed to update or propagate eventual collections' updates", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collection_updates", fmt.Sprintf("%+v", updateMessage), "error", err)
			}
		}
		if len(groupUpdateMessage) > 0 {
			s.log.DebugfWith("[%s] Propagating eventual collection group updates", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collection_groups_updates", fmt.Sprintf("%+v", groupUpdateMessage))
			err = s.dataManager.UpdateCollectionsGroupsAndPropagate(groupUpdateMessage)
			if err != nil {
				s.log.ErrorfWith("[%s] Failed to update or propagate eventual collections' groups update", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collection_groups_updates", fmt.Sprintf("%+v", groupUpdateMessage), "error", err)
			}
		}

		s.sendUpdateToGTP(resp.CacheUpdates.Groups)
		s.log.InfofWith("[%s] Finished propagation of updates on eventual collections", []any{routinePrefix}, "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collection_updates")
	}()

	// Update requests info synchronously
	var collRequestInfo []messages.CollectionRequestInfo
	for _, colls := range scheduleRequest.Collections {
		for _, coll := range colls {
			collRequestInfo = append(collRequestInfo, messages.CollectionRequestInfo{
				CollectionId:   coll.Id,
				IsWrite:        writeSeqCollection,
				RequestLatency: scheduleRequest.CumulativeLatency,
			})
		}
	}

	if seqColLock != nil {
		seqColLock.Unlock()
	}

	s.log.Info("Starting to update migration manager request infos for the used collections", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "collections_request_infos_updates", fmt.Sprintf("%+v", collRequestInfo))
	s.dataManager.GetMigrationManager().UpdatesRequestsInfo(collRequestInfo, reqHostname)

	s.log.Info("Local function execution completed successfully. Returning result", "fn_name", scheduleRequest.FunctionName, "fn_version", scheduleRequest.FunctionVersion, "executing_user", userId, "user_access_groups", groups, "fn_result", fmt.Sprintf("%+v", result), "new_sequence_number", seqNumber)
	return result, seqNumber, nil
}

func (s *baseScheduler) sendUpdateToGTP(groupsCache model.GroupsCache) {
	s.log.Info("Sending update to GTP", "groups_cache", fmt.Sprintf("%v", groupsCache))
	for groupName, toCreate := range groupsCache.Groups {
		s.log.Debug("Processing group update", "group_name", groupName, "to_create", toCreate)
		if toCreate {
			var participants []string
			for user, toAdd := range groupsCache.UsersInGroups[groupName] {
				if toAdd {
					participants = append(participants, user)
					delete(groupsCache.UsersInGroups[groupName], user)
				}
			}
			body, err := json.Marshal(participants)
			if err != nil {
				s.log.Warn("Failed to marshal group participants", "participants", participants, "group_name", groupName, "to_create", toCreate, "groups_cache", fmt.Sprintf("%v", groupsCache), "error", err)
				continue
			}

			url := fmt.Sprintf("http://%s/group/%s", s.serverInfo.GroupTokenProviderHostName, groupName)
			s.log.Debug("Creating group at GTP", "group_name", groupName, "to_create", toCreate)
			statusCode, err := s.makeHTTPRequest(http.MethodPost, url, bytes.NewReader(body))
			if err != nil {
				s.log.Warn("Failed to perform HTTP request towards GTP", "url", url, "method", http.MethodPost, "participants", participants, "group_name", groupName, "to_create", toCreate, "groups_cache", fmt.Sprintf("%v", groupsCache), "error", err)
				continue
			}

			if statusCode != http.StatusCreated {
				s.log.Warn("Received an unexpected status code as response", "url", url, "method", http.MethodPost, "received_status_code", statusCode, "expected_status_code", http.StatusCreated, "group_name", groupName, "to_create", toCreate, "groups_cache", fmt.Sprintf("%v", groupsCache))
				continue
			}
			s.log.Info("Group created at GTP", "group_name", groupName, "to_create", toCreate)
		} else {
			url := fmt.Sprintf("http://%s/group/%s", s.serverInfo.GroupTokenProviderHostName, groupName)
			s.log.Debug("Deleting group at GTP", "group_name", groupName, "to_create", toCreate)
			statusCode, err := s.makeHTTPRequest(http.MethodDelete, url, nil)
			if err != nil {
				s.log.Warn("Failed to perform HTTP request towards GTP", "url", url, "method", http.MethodDelete, "group_name", groupName, "to_create", toCreate, "groups_cache", fmt.Sprintf("%v", groupsCache), "error", err)
				continue
			}

			delete(groupsCache.UsersInGroups, groupName)
			if statusCode != http.StatusOK {
				s.log.Warn("Received an unexpected status code as response", "url", url, "method", http.MethodDelete, "received_status_code", statusCode, "expected_status_code", http.StatusOK, "group_name", groupName, "to_create", toCreate, "groups_cache", fmt.Sprintf("%v", groupsCache))
				continue
			}

			s.log.Info("Group deleted at GPT", "group_name", groupName, "to_create", toCreate)
		}
	}

	for groupName, users := range groupsCache.UsersInGroups {
		for user, toAdd := range users {
			var method string
			if toAdd {
				method = http.MethodPost
			} else {
				method = http.MethodDelete
			}
			url := fmt.Sprintf("http://%s/group/%s/%s", s.serverInfo.GroupTokenProviderHostName, groupName, user)
			s.log.Debug("Updating user membership at GTP", "group_name", groupName, "user", user, "to_add", toAdd, "method", method, "url", url)
			statusCode, err := s.makeHTTPRequest(method, url, nil)
			if err != nil {
				s.log.Warn("Failed to perform HTTP request towards GTP", "url", url, "method", method, "group_name", groupName, "user", user, "to_add", toAdd, "groups_cache", fmt.Sprintf("%v", groupsCache), "error", err)
				continue
			}
			if statusCode != http.StatusOK {
				s.log.Warn("Received an unexpected status code as response", "url", url, "method", method, "received_status_code", statusCode, "expected_status_code", http.StatusOK, "group_name", groupName, "user", user, "to_add", toAdd, "groups_cache", fmt.Sprintf("%v", groupsCache))
				continue
			}
			s.log.Info("User membership updated at GTP", "group_name", groupName, "user", user, "to_add", toAdd)
		}
	}

	s.log.Info("Completed group changes propagation to GTP")
}

func (s *baseScheduler) makeHTTPRequest(method string, url string, body io.Reader) (int, error) {
	s.log.Debug("Preparing HTTP request to GTP", "method", method, "url", url)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		s.log.Error("Failed creating a request to GTP", "method", method, "url", url, "error", err)
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", s.serverInfo.Secret)
	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		s.log.Error("Failed to send request to GTP", "method", method, "url", url, "headers", fmt.Sprintf("%v", req.Header), "error", err)
		return 0, err
	}
	defer resp.Body.Close()

	s.log.Debug("Received response from GTP", "method", method, "url", url, "headers", fmt.Sprintf("%v", req.Header), "status_code", resp.StatusCode)
	return resp.StatusCode, nil
}
