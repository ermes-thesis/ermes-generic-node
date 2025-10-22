package scheduler

import (
	"bytes"
	"caches"
	datamanager "dataManager"
	"dataManager/messages"
	shared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"metadataManager"
	"metadataManager/types"
	"net/http"
	"server"
	"strconv"
	"time"
	"tokenDecoder"

	"github.com/rs/xid"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
)

type Scheduler interface {
	Schedule(functionName string, functionVersion string, collections map[string][]types.CollectionInfo, sequentialSeqNumber uint64, reqHostname string, reqBody []byte, groups []string, userId string, groupToken string, cumulativeLatency int) (*FunctionResult, uint64, error)
}

type FunctionResult struct {
	StatusCode int
	Body       []byte
}

type SchedulerImpl struct {
	dataManager      datamanager.DataManager
	metadataManager  metadataManager.MetadataManager
	cache            caches.FunctionCache
	wasmExecutorPool wasm.ExecutorPool
	serverInfo       *server.ServerInfo
	log              *logger.Logger
}

func NewScheduler(dataManager datamanager.DataManager, metadataManager metadataManager.MetadataManager, cache caches.FunctionCache, wasmExecutorPool wasm.ExecutorPool, serverInfo *server.ServerInfo, logger *logger.Logger) *SchedulerImpl {
	return &SchedulerImpl{
		dataManager:      dataManager,
		metadataManager:  metadataManager,
		cache:            cache,
		wasmExecutorPool: wasmExecutorPool,
		serverInfo:       serverInfo,
		log:              logger,
	}
}

func (s *SchedulerImpl) RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder, metadataManager metadataManager.MetadataManager) error {
	err := server.AddCustomRoute("/invoke-function/{name}/{version}", false, s.handleFunctionInvocation(tokenDecoder, metadataManager))
	if err != nil {
		s.log.Error("Failed to register custom routes", "path", "/invoke-function/{name}/{version}", "error", err)
		return fmt.Errorf("Error while registering /invoke-function/{name}/{version}: %v\n", err)
	}
	err = server.AddCustomRoute("/invoke-function/{name}", false, s.handleFunctionInvocation(tokenDecoder, metadataManager))
	if err != nil {
		s.log.Error("Failed to register custom routes", "path", "/invoke-function/{name}", "error", err)
		return fmt.Errorf("Error while registering /invoke-function/{name}: %v\n", err)
	}

	err = server.AddCustomRoute("/pvt-offload-function/{name}/{version}", true, s.handleOffloadFunctionInvocation(tokenDecoder))
	if err != nil {
		s.log.Error("Failed to register custom routes", "path", "/pvt-offload-function/{name}/{version}", "error", err)
		return fmt.Errorf("Error while registering /pvt-offload-function/{name}/{version}: %v\n", err)
	}
	err = server.AddCustomRoute("/pvt-offload-function/{name}", true, s.handleOffloadFunctionInvocation(tokenDecoder))
	if err != nil {
		s.log.Error("Failed to register custom routes", "path", "/pvt-offload-function/{name}", "error", err)
		return fmt.Errorf("Error while registering /pvt-offload-function/{name}: %v\n", err)
	}

	return nil
}

func (s *SchedulerImpl) retryIsCollectionLocallyPresent(
	colId string,
	seqNum uint64,
	retries int,
	delay time.Duration,
) (bool, shared.Collection, error) {
	var (
		found      bool
		collection shared.Collection
		err        error
	)

	for i := 0; i < retries; i++ {
		found, collection, err = s.dataManager.GetCollectionsDiscoverer().IsCollectionLocallyPresent(colId, seqNum)
		if err == nil {
			s.log.Info("Collection existence known locally", "collection_id", colId, "seq_num", seqNum, "collection", fmt.Sprintf("%+v", collection))
			return found, collection, nil
		}
		s.log.Debug("Retry collection presence check", "collection_id", colId, "seq_num", seqNum, "current_attempt", i, "error", err)
		if i < retries-1 {
			time.Sleep(delay)
		}
	}
	s.log.Info("Collection not found locally after retries", "collection_id", colId, "seq_num", seqNum)
	return found, collection, fmt.Errorf("after %d retries: %v", retries, err)
}

func (s *SchedulerImpl) Schedule(functionName string, functionVersion string, collections map[string][]types.CollectionInfo, sequentialSeqNumber uint64, reqHostname string, reqBody []byte, groups []string, userId string, groupToken string, cumulativeLatency int) (*FunctionResult, uint64, error) {
	s.log.Info("Scheduling function", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups)
	s.log.Debug("Scheduling function", "with_collections", fmt.Sprintf("%+v", collections), "with_request_body", string(reqBody), "sequential_seq_num", sequentialSeqNumber, "req_hostname", reqHostname)

	if len(collections) == 0 {
		s.log.Info("No collections specified, executing locally", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups)
		s.log.Debug("No collections specified, executing locally", "with_collections", fmt.Sprintf("%+v", collections), "with_request_body", string(reqBody))
		return s.executeLocally(functionName, functionVersion, collections, reqHostname, reqBody, groups, userId, "", false, cumulativeLatency)
	}

	s.log.Info("Start to analyze collections for scheduling decision", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId)
	// Initializing vars for function returned collections analysis
	var sequentialCollection *shared.StrongCollection
	sequentialCollectionNeeded := false
	hasEventualLocal := false
	eventualHostCounts := make(map[string]uint)
	maxEventualCount := uint(0)
	hostWithMoreEventual := ""
	if s.serverInfo.Father != nil {
		hostWithMoreEventual = s.serverInfo.Father.HostName
	}

	// Cycling over all the returned collections (they are grouped based on their "AS" defined by the dev)
	for _, cols := range collections {
		// Cycling over all the collections with the same "AS"
		for _, col := range cols {
			// Checking if the collection is locally present, the check is retried maximum 10 times, every 500ms if the
			// collection is a sequential consistent one. (In case of an eventual consistent the seqNumber passed is ignored)
			found, collection, err := s.retryIsCollectionLocallyPresent(col.Id, sequentialSeqNumber, 10, 500*time.Millisecond)
			if err != nil {
				s.log.Error("Failed to verify collection presence", "collection_id", col.Id, "sequential_seq_num", sequentialSeqNumber, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "with_collections", fmt.Sprintf("%+v", collections), "with_request_body", reqBody, "req_hostname", reqHostname, "error", err)
				return nil, 0, s.log.ErrorfErr("wrong sequence number for collection: %v, %v", col.Id, err)
			}

			if found {
				defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RUnlock()
			}

			if col.Metadata.Consistency == model.SequentialConsistent {
				sequentialCollectionNeeded = true
				if sequentialCollection != nil {
					s.log.Error("Multiple sequential collections requested for execution, which is not supported.", "sequential_collection_needed", sequentialCollectionNeeded, "already_found_seq_collection", fmt.Sprintf("%+v", sequentialCollection), "just_found_seq_collection", fmt.Sprintf("%+v", col), "has_eventual_local", hasEventualLocal, "eventual_host_counts", eventualHostCounts, "max_eventual_count", maxEventualCount, "host_with_more_eventual", hostWithMoreEventual, "error", err)
					return nil, 0, fmt.Errorf("multiple sequentially consistent collections are not supported")
				}
				// If collection is NIL, it means that the current node does not know its existence
				if collection != nil {
					sequentialCollection = collection.(*shared.StrongCollection)
				}
			} else {
				if found {
					hasEventualLocal = true
				}

				// If the currently iterated collection is known by the current node, we count the host for this collection
				if collection != nil {
					for hostId := range collection.HostsId() {
						eventualHostCounts[hostId]++
						if eventualHostCounts[hostId] > maxEventualCount {
							maxEventualCount = eventualHostCounts[hostId]
							hostWithMoreEventual = hostId
						}
					}
				} else if s.serverInfo.Father != nil {
					hostId := s.serverInfo.Father.HostName
					eventualHostCounts[hostId]++
					if eventualHostCounts[hostId] > maxEventualCount {
						maxEventualCount = eventualHostCounts[hostId]
						hostWithMoreEventual = hostId
					}
				}
			}
		}
	}

	s.log.Info("Collection analysis completed", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "sequential_coll_needed", sequentialCollectionNeeded, "sequential_collection_found", fmt.Sprintf("%+v", sequentialCollection), "has_eventual_local", hasEventualLocal, "eventual_host_counts", fmt.Sprintf("%+v", eventualHostCounts), "max_eventual_count", maxEventualCount, "host_with_more_eventual", hostWithMoreEventual)
	// Route based on collection analysis
	// Priority: Sequential > Local Eventual > Remote Eventual
	if sequentialCollectionNeeded {
		s.log.Debug("Sequential collection requested for execution. Scheduling accordingly", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "with_collections", fmt.Sprintf("%+v", collections), "with_request_body", reqBody, "sequential_seq_num", sequentialSeqNumber, "req_hostname", reqHostname)

		if sequentialCollection == nil {
			if s.serverInfo.Father == nil {
				s.log.Error("Couldn't find in the architecture the requested sequential collection", "fn_name", functionName, "fn_version", functionVersion, "with_collections", fmt.Sprintf("%+v", collections), "with_request_body", reqBody, "sequential_seq_num", sequentialSeqNumber, "req_hostname", reqHostname)
				return nil, 0, errors.New("sequential collection requested does not exists")
			}
			s.log.Info("Sequential collection existence not found. Trying to forward the request to father node",
				"fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, userId, "forwarded_to", s.serverInfo.Father.HostName)
			return s.forwardRequest(s.serverInfo.Father.HostName, functionName, functionVersion, collections, groupToken, sequentialSeqNumber, cumulativeLatency)
		}

		// Sequential collection present - check if it is requested a write or read access
		functionMetadata, err := s.cache.GetMetadata(functionName, functionVersion)
		if err != nil {
			s.log.Error("Failed to retrieve function metadata", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "with_collections", fmt.Sprintf("%+v", collections), "with_request_body", reqBody, "sequential_seq_num", sequentialSeqNumber, "req_hostname", reqHostname, "error", err)
			return nil, 0, fmt.Errorf("error retrieving function metadata: %v", err)
		}

		if sequentialCollection.IsLocal() {
			// Re-check if the collection is still local, after acquiring the lock
			// This is needed because the collection could be migrated while we were waiting for the lock
			// If the collection is not local anymore, we need to forward the request
			//if !sequentialCollection.IsLocal() {
			//	return s.forwardRequestWithSequential(functionName, functionVersion, collections, groupToken, sequentialSeqNumber, functionMetadata.StrongCollWriteAccess, sequentialCollection)
			//}

			// If Sequential collection is present, and it's the leader execute locally.
			// If Sequential collection is present, and the function needs only to read it, execute locally
			if sequentialCollection.IsLeader || !functionMetadata.StrongCollWriteAccess {
				if functionMetadata.StrongCollWriteAccess {
					sequentialCollection.WriteMutex.Lock()
					defer sequentialCollection.WriteMutex.Unlock()
				}
				s.log.Info("Executing function locally with sequential collection", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups)
				s.log.Debug("Executing function locally with sequential collection", "seq_collection_id", sequentialCollection.CollectionId(), "sequential_seq_num", sequentialSeqNumber, "is_leader", sequentialCollection.IsLeader, "write_access_requested", functionMetadata.StrongCollWriteAccess)
				return s.executeLocally(functionName, functionVersion, collections, reqHostname, reqBody, groups, userId, sequentialCollection.CollectionId(), functionMetadata.StrongCollWriteAccess, cumulativeLatency)
			}

			s.log.Info("Sequential collection must be accessed in write but the local sequential collection is not the leader. Forwarding request to the host containing the leader sequential collection",
				"fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "forwarded_to", sequentialCollection.LeaderId)
			// In all the other cases forward to the leader replica of the sequential collection
			return s.forwardRequest(sequentialCollection.LeaderId, functionName, functionVersion, collections, groupToken, sequentialSeqNumber, cumulativeLatency)
		}

		s.log.Info("Sequential collection found, but we are in a parent node since it is not local. Forwarding to the correct child node.", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups)
		return s.forwardRequestWithSequential(functionName, functionVersion, collections, groupToken, sequentialSeqNumber, functionMetadata.StrongCollWriteAccess, sequentialCollection, cumulativeLatency)
	}

	// If is not requested a Sequential Collection, and the current node has at least one of the eventual collections
	// requested, we execute the function locally
	if hasEventualLocal {
		s.log.Info("Executing function locally with eventual", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId)
		s.log.Debug("Executing function locally with eventual", "with_collections", fmt.Sprintf("%+v", collections), "with_request_body", string(reqBody), "user_access_groups", groups, "request_hostname", reqHostname)
		return s.executeLocally(functionName, functionVersion, collections, reqHostname, reqBody, groups, userId, "", false, cumulativeLatency)
	}

	// We have only Eventual Collection requested, and no one of them is local, we forward the scheduling request
	// towards the DIRECTION where we have more Eventual Collections
	s.log.Info("Only eventual collections detected. None is local, forwarding request to host with more eventual collections detected", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "forwarded_to", hostWithMoreEventual)
	return s.forwardRequest(hostWithMoreEventual, functionName, functionVersion, collections, groupToken, sequentialSeqNumber, cumulativeLatency)
}

func (s *SchedulerImpl) forwardRequestWithSequential(functionName string, functionVersion string, collections map[string][]types.CollectionInfo, groupToken string, sequentialSeqNumber uint64, writeAccess bool, sequentialCollection *shared.StrongCollection, cumulativeLatency int) (*FunctionResult, uint64, error) {
	// If the Sequential collection is not locally present, and the function needs to access it in write. Forward towards
	// the leader.
	if writeAccess {
		s.log.Debug("Forwarding function scheduling to the sequential collection leader since we have to access it in write", "fn_name", functionName, "fn_version", functionVersion, "forward_to", sequentialCollection.LeaderId)
		return s.forwardRequest(sequentialCollection.LeaderId, functionName, functionVersion, collections, groupToken, sequentialSeqNumber, cumulativeLatency)
	}

	// If the Sequential collection is not locally present, and the function require only reading it. Forward towards to
	// one of the available replicas
	minLatency := math.MaxInt
	var hostWithMinLatency string
	for hostId := range sequentialCollection.HostsId() {
		currLatency := s.serverInfo.GetLatencyFor(hostId)
		s.log.Debug("Checked latency for host", "host_id", hostId, "curr_latency", currLatency, "min_latency", minLatency)
		if currLatency < minLatency {
			hostWithMinLatency = hostId
			minLatency = currLatency
		}
	}

	s.log.Debug("Replica selection for sequential collection read", "host_with_min_latency", hostWithMinLatency, "min_latency", minLatency)
	if hostWithMinLatency != "" {
		s.log.Debug("Forwarding function scheduling to one of the available sequential collection replicas, based on latency, since we have to access it in read", "fn_name", functionName, "fn_version", functionVersion, "forward_to", hostWithMinLatency)
		return s.forwardRequest(hostWithMinLatency, functionName, functionVersion, collections, groupToken, sequentialSeqNumber, cumulativeLatency)
	}

	// If "HostId" is empty we fall back to the father (shouldn't happen, since if we are in this branch, it means that
	// the node knows the existence of the collection, therefore the collection should be local to the node, or in
	// one of its children, meaning that "HostId" should NOT be empty)
	if s.serverInfo.Father != nil {
		s.log.Debug("Forwarding function scheduling to father as fallback", "fn_name", functionName, "fn_version", functionVersion, "forward_to", s.serverInfo.Father.HostName)
		return s.forwardRequest(s.serverInfo.Father.HostName, functionName, functionVersion, collections, groupToken, sequentialSeqNumber, cumulativeLatency)
	}

	s.log.Error("No available host for sequential collection (unexpected)", "fn_name", functionName, "fn_version", functionVersion, "collection_id", sequentialCollection.CollectionId())
	// If we are here, it means that something wrong has happened in the scheduling process, and we are in the Cloud Node
	panic("No available host for sequential collection")
}

func (s *SchedulerImpl) executeLocally(functionName string, functionVersion string, collections map[string][]types.CollectionInfo, reqHostname string, reqBody []byte, groups []string, userId string, sequentialCollId string, writeSeqCollection bool, cumulativeLatency int) (*FunctionResult, uint64, error) {
	s.log.Debug("Acquiring wasm executor from pool", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups)
	executor := s.wasmExecutorPool.Acquire()
	defer func() {
		executor.Release()
		s.log.Debug("Released wasm executor back to pool", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups)
	}()

	ctx := WasmContext{
		UserId:      userId,
		GroupList:   groups,
		Collections: collections,
	}
	ctxJson, err := json.Marshal(ctx)
	if err != nil {
		s.log.Error("Failed to marshal wasm context for function execution", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(reqBody), "request_hostname", reqHostname, "with_sequential_collection", sequentialCollId != "", "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "error", err)
		return nil, 0, fmt.Errorf("error marshalling wasm context: %v", err)
	}

	wasmBinary, err := s.cache.GetBinary(functionName, functionVersion)
	if err != nil {
		s.log.Error("Failed to retrieve function binary", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(reqBody), "request_hostname", reqHostname, "with_sequential_collection", sequentialCollId != "", "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "error", err)
		return nil, 0, fmt.Errorf("error retrieving function binary: %v", err)
	}

	s.log.Info("Invoking function locally", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups)
	s.log.Debug("Invoking function locally", "fn_start_time", time.Now().String())
	resp := executor.Invoke(wasmBinary, "entry", reqBody, ctxJson)
	s.log.Info("Function executed successfully", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups)
	s.log.Debug("Function executed successfully", "fn_end_time", time.Now().String(), "fn_response", fmt.Sprintf("%+v", resp))

	repBody, err := json.Marshal(resp.Response)
	if err != nil {
		s.log.Error("Failed to marshal function response", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(reqBody), "request_hostname", reqHostname, "with_sequential_collection", sequentialCollId != "", "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "error", err)
		return nil, 0, fmt.Errorf("error marshalling response body: %v", err)
	}
	result := &FunctionResult{
		StatusCode: resp.HttpCode,
		Body:       repBody,
	}

	if resp.CacheUpdates == nil {
		if sequentialCollId != "" {
			seqNumber, err := s.dataManager.GetMigrationManager().GetCollectionVectorClockValue(sequentialCollId)
			s.log.Info("No function cache updates to commit, returning directly the result", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "fn_result", fmt.Sprintf("%+v", result), "new_sequential_collection_seq_num", seqNumber)
			return result, seqNumber, err
		}
		s.log.Info("No function cache updates to commit, returning directly the result", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "fn_result", fmt.Sprintf("%+v", result))
		return result, 0, nil
	}

	var seqNumber uint64
	if sequentialCollId != "" {
		s.log.Debug("Processing sequential collection cache updates", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId)
		if update, ok := resp.CacheUpdates.CollectionUpdates[sequentialCollId]; ok {
			if !writeSeqCollection {
				s.log.Warn("DEV_ERROR: Function tried to update sequential collection without having write access")
				return nil, 0, fmt.Errorf("DEV_ERROR: Function tried to update sequential collection without having write access")
			}

			if update.Deleted {
				s.log.Debug("Sequential collection requested deletion in executed function", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId)
				err = s.dataManager.RemoveCollectionAndPropagate(sequentialCollId, groups, userId)
				if err != nil {
					s.log.Error("Failed to remove sequential collection", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(reqBody), "request_hostname", reqHostname, "sequential_collection_id", sequentialCollId, "sequential_collection_write_access", writeSeqCollection, "error", err)
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
					s.log.Debug("Sequential collection dirty records commits prepared", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "record_updates", fmt.Sprintf("%+v", updates))
					err = s.dataManager.UpdateCollectionsRecordsAndPropagate(updates, groups, userId)
					if err != nil {
						s.log.Error("Failed to update sequential collection records", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(reqBody), "collection_updates", fmt.Sprintf("%+v", updates), "sequential_collection_write_access", writeSeqCollection, "error", err)
						return nil, 0, fmt.Errorf("error updating sequential collection records: %v", err)
					}
					s.log.Debug("Sequential collection records updated and propagated", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "record_updates", fmt.Sprintf("%+v", updates))
				}
				if len(update.DeletedRecords) > 0 {
					updates := []messages.CollectionUpdate{{
						CollectionId: sequentialCollId,
						RecordNames:  update.DeletedRecords,
						IsDelete:     true,
					}}
					s.log.Debug("Sequential collection delete records commits prepared", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "records_to_delete", fmt.Sprintf("%+v", updates))
					err = s.dataManager.UpdateCollectionsRecordsAndPropagate(updates, groups, userId)
					if err != nil {
						s.log.Error("Failed to delete sequential collection records", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(reqBody), "collection_updates", fmt.Sprintf("%+v", updates), "sequential_collection_write_access", writeSeqCollection, "error", err)
						return nil, 0, fmt.Errorf("error deleting sequential collection records: %v", err)
					}
					s.log.Debug("Sequential collection delete records deleted and propagated", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "records_to_delete", fmt.Sprintf("%+v", updates))
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
					s.log.Debug("Sequential collection group changes calculated", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove)
					updateResp, err := s.metadataManager.UpdateGroupsInCollection(sequentialCollId, groups, userId, groupsToAdd, groupsToRemove)
					if err != nil {
						s.log.Error("Failed to update sequential collection groups", "fn_name", functionName, "fn_version", functionVersion, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(reqBody), "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "sequential_collection_write_access", writeSeqCollection, "error", err)
						return nil, 0, fmt.Errorf("error updating sequential collection groups: %v", err)
					}
					s.log.Debug("Metadata groups updated successfully for sequential collection", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "groups_update_response", fmt.Sprintf("%+v", updateResp))

					s.log.Debug("Starting sequential collection groups update and propagation", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "groups_update_response", fmt.Sprintf("%+v", updateResp))
					err = s.dataManager.UpdateCollectionsGroupsAndPropagate([]messages.CollectionGroupUpdate{{
						CollectionId: sequentialCollId,
						GroupNames:   updateResp.Groups,
						Sequence:     updateResp.Sequence,
					}})
					if err != nil {
						s.log.Error("Failed to propagate the updates to sequential collection groups", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "fn_response", fmt.Sprintf("%+v", resp), "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(reqBody), "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "sequential_collection_write_access", writeSeqCollection, "error", err)
						return nil, 0, s.log.ErrorfErr("error propagating sequential collection group updates: %v", err)
					}
				}
			}
			delete(resp.CacheUpdates.CollectionUpdates, sequentialCollId)
			s.log.Debug("Removed sequential collection updates from cache updates map since it has been processed")
		}

		seqNumber, err = s.dataManager.GetMigrationManager().GetCollectionVectorClockValue(sequentialCollId)
		if err != nil {
			s.log.Error("Error getting sequential collection vector clock value", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "with_context", fmt.Sprintf("%+v", ctx), "with_request_body", string(reqBody), "request_hostname", reqHostname, "sequential_collection_id", sequentialCollId, "sequential_collection_access_write", writeSeqCollection, "error", err)
			return nil, 0, s.log.ErrorfErr("error getting sequential collection vector clock value: %v", err)
		}
		s.log.Debug("Obtained new sequential collection sequence number", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "sequential_collection_id", sequentialCollId, "new_sequence_number", seqNumber)
	}

	// Process eventual updates asynchronously
	go func() {
		routinePrefix := fmt.Sprintf("GO#%s", xid.New().String())

		s.log.InfofWith("[%s] Starting goroutine to propagate eventual collection updates", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups)
		var updateMessage []messages.CollectionUpdate
		var groupUpdateMessage []messages.CollectionGroupUpdate

		for collId, update := range resp.CacheUpdates.CollectionUpdates {
			s.log.DebugfWith("[%s] Processing collection cache update", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", userId, "collection_id", collId, "update", fmt.Sprintf("%+v", update))
			if update.Deleted {
				go func(collId string) {
					routinePrefix := fmt.Sprintf("GO#%s", xid.New().String())
					err := s.dataManager.RemoveCollectionAndPropagate(collId, groups, userId)
					if err != nil {
						s.log.ErrorfWith("[%s] Failed to delete or propagate the deletion of collection", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "collection_id", collId, "update", fmt.Sprintf("%+v", update), "error", err)
					}
					s.log.DebugfWith("[%s] Removed collection asynchronously", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "collection_id", collId, "update", fmt.Sprintf("%+v", update))
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
					s.log.ErrorfWith("[%s] Failed updating groups in eventual collection", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "collection_id", collId, "groups_to_add", groupsToAdd, "groups_to_remove", groupsToRemove, "error", err)
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
			s.log.Debug("[%s] Propagating eventual collection updates", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "collection_updates", fmt.Sprintf("%+v", updateMessage))
			err := s.dataManager.UpdateCollectionsRecordsAndPropagate(updateMessage, groups, userId)
			if err != nil {
				s.log.ErrorfWith("[%s] Failed to update or propagate eventual collections' updates", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "collection_updates", fmt.Sprintf("%+v", updateMessage), "error", err)
			}
		}
		if len(groupUpdateMessage) > 0 {
			s.log.DebugfWith("[%s] Propagating eventual collection group updates", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "collection_groups_updates", fmt.Sprintf("%+v", groupUpdateMessage))
			err = s.dataManager.UpdateCollectionsGroupsAndPropagate(groupUpdateMessage)
			if err != nil {
				s.log.ErrorfWith("[%s] Failed to update or propagate eventual collections' groups update", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "collection_groups_updates", fmt.Sprintf("%+v", groupUpdateMessage), "error", err)
			}
		}

		s.sendUpdateToGTP(resp.CacheUpdates.Groups)
		s.log.InfofWith("[%s] Finished propagation of updates on eventual collections", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "collection_updates")
	}()

	// Update requests info asynchronously
	go func() {
		routinePrefix := fmt.Sprintf("GO#%s", xid.New().String())
		var collRequestInfo []messages.CollectionRequestInfo
		for _, colls := range collections {
			for _, coll := range colls {
				collRequestInfo = append(collRequestInfo, messages.CollectionRequestInfo{
					CollectionId:   coll.Id,
					NodeName:       reqHostname,
					IsWrite:        writeSeqCollection,
					RequestLatency: cumulativeLatency,
				})
			}
		}

		s.log.InfofWith("[%s] Starting to update migration manager request infos for the used collections", []any{routinePrefix}, "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "collections_request_infos_updates", fmt.Sprintf("%+v", collRequestInfo))
		s.dataManager.GetMigrationManager().UpdatesRequestsInfo(collRequestInfo)
	}()

	s.log.Info("Local function execution completed successfully. Returning result", "fn_name", functionName, "fn_version", functionVersion, "executing_user", userId, "user_access_groups", groups, "fn_result", fmt.Sprintf("%+v", result), "new_sequence_number", seqNumber)
	return result, seqNumber, nil
}

func (s *SchedulerImpl) sendUpdateToGTP(groupsCache model.GroupsCache) {
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

func (s *SchedulerImpl) makeHTTPRequest(method string, url string, body io.Reader) (int, error) {
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

func (s *SchedulerImpl) forwardRequest(dest string, functionName string, functionVersion string, collections map[string][]types.CollectionInfo, groupToken string, seqNumber uint64, cumulativeLatency int) (*FunctionResult, uint64, error) {
	s.log.Debug("Preparing request to offload scheduling", "fn_name", functionName, "fn_version", functionVersion, "destination", dest, "with_collections", fmt.Sprintf("%+v", collections), "sequential_coll_seq_num", seqNumber, "cumulative_latency", cumulativeLatency)

	url := fmt.Sprintf("http://%s/pvt-offload-function/%s/%s", dest, functionName, functionVersion)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		s.log.Error("Error creating offloading request", "fn_name", functionName, "fn_version", functionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", collections), "sequential_coll_seq_num", seqNumber, "cumulative_latency", cumulativeLatency, "error", err)
		return nil, 0, fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Set("X-ERMES-GROUPS", groupToken)
	collectionsJson, err := json.Marshal(collections)
	if err != nil {
		s.log.Error("Error marshalling collections for offloading request", "fn_name", functionName, "fn_version", functionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", collections), "sequential_coll_seq_num", seqNumber, "cumulative_latency", cumulativeLatency, "error", err)
		return nil, 0, fmt.Errorf("error marshalling collections: %v", err)
	}
	req.Header.Set("X-ERMES-COLLECTIONS", string(collectionsJson))
	req.Header.Set("X-ERMES-SECRET", s.serverInfo.Secret)
	req.Header.Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", seqNumber))
	req.Header.Set("X-ERMES-HOSTNAME", s.serverInfo.HostName)
	req.Header.Set("X-ERMES-LATENCY", strconv.Itoa(cumulativeLatency+s.serverInfo.GetLatencyFor(dest)))

	s.log.Debug("Forward request prepared with headers", "headers", fmt.Sprintf("%v", req.Header))
	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		s.log.Error("Error forwarding request", "fn_name", functionName, "fn_version", functionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", collections), "sequential_coll_seq_num", seqNumber, "cumulative_latency", cumulativeLatency, "error", err)
		return nil, 0, fmt.Errorf("error forwarding request: %v", err)
	}
	defer resp.Body.Close()

	sequentialSeqNumberStr := resp.Header.Get("X-ERMES-SEQUENTIAL-NUMBER")
	var sequentialSeqNumber uint64
	if sequentialSeqNumberStr != "" {
		_, err = fmt.Sscanf(sequentialSeqNumberStr, "%d", &sequentialSeqNumber)
		if err != nil {
			s.log.Error("Error parsing sequential sequence number", "fn_name", functionName, "fn_version", functionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", collections), "sequential_coll_seq_num", seqNumber, "cumulative_latency", cumulativeLatency, "error", err)
			return nil, 0, fmt.Errorf("error parsing sequential sequence number: %v", err)
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.log.Error("Error reading response", "fn_name", functionName, "fn_version", functionVersion, "url", url, "with_collections", fmt.Sprintf("%+v", collections), "sequential_coll_seq_num", seqNumber, "cumulative_latency", cumulativeLatency, "error", err)
		return nil, 0, fmt.Errorf("error reading response: %v", err)
	}

	fnRes := FunctionResult{
		StatusCode: resp.StatusCode,
		Body:       body,
	}
	s.log.Debug("Received response from offloaded scheduling", "fn_response", fmt.Sprintf("%+v", fnRes), "new_sequential_number", sequentialSeqNumber, "old_sequential_number", seqNumber)
	return &fnRes, sequentialSeqNumber, nil
}
