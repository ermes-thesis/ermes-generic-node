package scheduler

import (
	"bytes"
	"caches"
	datamanager "dataManager"
	"dataManager/messages"
	shared "dataManager/shared"
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"metadataManager/types"
	"net/http"
	"server"
	"server/info"
	"time"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"github.com/ermes-thesis/ermes-common/wasm-pool"
)

type Scheduler interface {
	Schedule(functionName string, functionVersion string, collections map[string][]types.CollectionInfo, sequentialSeqNumber uint64, reqHostname string, reqBody []byte, groups []string, userId string, groupToken string) (*FunctionResult, uint64, error)
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
	serverInfo       *info.ServerInfo
	log              *logger.Logger
}

func NewScheduler(dataManager datamanager.DataManager, metadataManager metadataManager.MetadataManager, cache caches.FunctionCache, wasmExecutorPool wasm.ExecutorPool, serverInfo *info.ServerInfo, logger *logger.Logger) *SchedulerImpl {
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
		return s.log.ErrorfErr("error adding route /invoke-function/{name}/{version}/: %v", err)
	}
	err = server.AddCustomRoute("/invoke-function/{name}", false, s.handleFunctionInvocation(tokenDecoder, metadataManager))
	if err != nil {
		return s.log.ErrorfErr("error adding route /invoke-function/{name}: %v", err)
	}

	err = server.AddCustomRoute("/pvt-offload-function/{name}/{version}", true, s.handleOffloadFunctionInvocation(tokenDecoder))
	if err != nil {
		return s.log.ErrorfErr("error adding route /pvt-offload-function/{name}/{version}/: %v", err)
	}
	err = server.AddCustomRoute("/pvt-offload-function/{name}", true, s.handleOffloadFunctionInvocation(tokenDecoder))
	if err != nil {
		return s.log.ErrorfErr("error adding route /pvt-offload-function/{name}: %v", err)
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
			return found, collection, nil
		}
		if i < retries-1 {
			time.Sleep(delay)
		}
	}
	return found, collection, s.log.ErrorfErr("after %d retries: %v", retries, err)
}

func (s *SchedulerImpl) Schedule(functionName string, functionVersion string, collections map[string][]types.CollectionInfo, sequentialSeqNumber uint64, reqHostname string, reqBody []byte, groups []string, userId string, groupToken string) (*FunctionResult, uint64, error) {
	s.log.Infof("Scheduling function: %v, version: %v", functionName, functionVersion)
	s.log.Infof("With collections: %v\n", collections)

	if len(collections) == 0 {
		return s.executeLocally(functionName, functionVersion, collections, reqHostname, reqBody, groups, userId, "", false)
	}

	// Initializing vars for function returned collections analysis
	var sequentialCollection *shared.StrongCollection
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
				return nil, 0, s.log.ErrorfErr("wrong sequence number for collection: %v, %v", col.Id, err)
			}

			if found {
				defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RUnlock()
			}

			if col.Metadata.Consistency == model.SequentialConsistent {
				if sequentialCollection != nil {
					return nil, 0, s.log.ErrorfErr("multiple sequentially consistent collections are not supported")
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
				// TODO: Should replace this counter with a check to find the host with lower latency
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

	// Route based on collection analysis
	// Priority: Sequential > Local Eventual > Remote Eventual
	if sequentialCollection != nil {

		// Sequential collection present - check if it is requested a write or read access
		functionMetadata, err := s.cache.GetMetadata(functionName, functionVersion)
		if err != nil {
			return nil, 0, s.log.ErrorfErr("error retrieving function metadata: %v", err)
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
				return s.executeLocally(functionName, functionVersion, collections, reqHostname, reqBody, groups, userId, sequentialCollection.CollectionId(), functionMetadata.StrongCollWriteAccess)
			}

			// In all the other cases forward to the leader replica of the sequential collection
			return s.forwardRequest(sequentialCollection.LeaderId, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
		}

		return s.forwardRequestWithSequential(functionName, functionVersion, collections, groupToken, sequentialSeqNumber, functionMetadata.StrongCollWriteAccess, sequentialCollection)
	}

	// If is not requested a Sequential Collection, and the current node has at least one of the eventual collections
	// requested, we execute the function locally
	if hasEventualLocal {
		return s.executeLocally(functionName, functionVersion, collections, reqHostname, reqBody, groups, userId, "", false)
	}

	// We have only Eventual Collection requested, and no one of them is local, we forward the scheduling request
	// towards the DIRECTION where we have more Eventual Collections
	return s.forwardRequest(hostWithMoreEventual, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
}

func (s *SchedulerImpl) forwardRequestWithSequential(functionName string, functionVersion string, collections map[string][]types.CollectionInfo, groupToken string, sequentialSeqNumber uint64, writeAccess bool, sequentialCollection *shared.StrongCollection) (*FunctionResult, uint64, error) {

	// If the Sequential collection is not locally present, and the function needs to access it in write. Forward towards
	// the leader.
	if writeAccess {
		return s.forwardRequest(sequentialCollection.LeaderId, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
	}

	// If the Sequential collection is not locally present, and the function require only reading it. Forward towards to
	// one of the available replicas
	// TODO: Should replace this forwarding with a check to find the host with lower latency
	for hostId := range sequentialCollection.HostsId() {
		return s.forwardRequest(hostId, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
	}

	// If "HostId" is empty we fall back to the father (shouldn't happen, since if we are in this branch, it means that
	// the node knows the existence of the collection, therefore the collection should be local to the node, or in
	// one of its children, meaning that "HostId" should NOT be empty)
	if s.serverInfo.Father != nil {
		return s.forwardRequest(s.serverInfo.Father.HostName, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
	}

	// If we are here, it means that something wrong has happened in the scheduling process and we are in the Cloud Node
	panic("No available host for sequential collection")
}

func (s *SchedulerImpl) executeLocally(functionName string, functionVersion string, collections map[string][]types.CollectionInfo, reqHostname string, reqBody []byte, groups []string, userId string, sequentialCollId string, writeSeqCollection bool) (*FunctionResult, uint64, error) {
	executor := s.wasmExecutorPool.Acquire()
	defer executor.Release()
	ctx := WasmContext{
		UserId:      userId,
		GroupList:   groups,
		Collections: collections,
	}
	ctxJson, err := json.Marshal(ctx)
	if err != nil {
		s.log.Errorf("error marshalling wasm context: %v", err)
		return nil, 0, s.log.ErrorfErr("error marshalling wasm context: %v", err)
	}

	wasmBinary, err := s.cache.GetBinary(functionName, functionVersion)
	if err != nil {
		s.log.Errorf("error retrieving function binary: %v", err)
		return nil, 0, s.log.ErrorfErr("error retrieving function binary: %v", err)
	}

	s.log.Infof("Invoking function %s, version %s", functionName, functionVersion)
	resp := executor.Invoke(wasmBinary, "entry", reqBody, ctxJson)
	s.log.Infof("Function executed with response: %v", resp)

	repBody, err := json.Marshal(resp.Response)
	if err != nil {
		s.log.Errorf("error marshalling response body: %v", err)
		return nil, 0, s.log.ErrorfErr("error marshalling response body: %v", err)
	}
	result := &FunctionResult{
		StatusCode: resp.HttpCode,
		Body:       repBody,
	}

	if resp.CacheUpdates == nil {
		s.log.Infof("No cache updates, returning result: %v", result)
		if sequentialCollId != "" {
			seqNumber, err := s.dataManager.GetMigrationManager().GetCollectionVectorClockValue(sequentialCollId)
			return result, seqNumber, err
		}
		return result, 0, nil
	}

	var seqNumber uint64
	if sequentialCollId != "" {
		if update, ok := resp.CacheUpdates.CollectionUpdates[sequentialCollId]; ok {
			if !writeSeqCollection {
				s.log.Errorf("error: function tried to update sequential collection without having write access")
				return nil, 0, s.log.ErrorfErr("error: function tried to update sequential collection without having write access")
			}

			if update.Deleted {
				err := s.dataManager.RemoveCollectionAndPropagate(sequentialCollId, groups, userId)
				if err != nil {
					s.log.Errorf("error removing sequential collection: %v", err)
					return nil, 0, s.log.ErrorfErr("error removing sequential collection: %v", err)
				}
			} else {
				var recordNames []string
				var recordValues []string
				for recordName, recordValue := range update.DirtyRecords {
					recordNames = append(recordNames, recordName)
					recordValues = append(recordValues, recordValue)
				}
				if len(recordNames) > 0 {
					err := s.dataManager.UpdateCollectionsRecordsAndPropagate([]messages.CollectionUpdate{{
						CollectionId: sequentialCollId,
						RecordNames:  recordNames,
						RecordValues: recordValues,
						IsDelete:     false,
					}}, groups, userId)
					if err != nil {
						s.log.Errorf("error updating sequential collection records: %v", err)
						return nil, 0, s.log.ErrorfErr("error updating sequential collection records: %v", err)
					}
				}
				if len(update.DeletedRecords) > 0 {
					err := s.dataManager.UpdateCollectionsRecordsAndPropagate([]messages.CollectionUpdate{{
						CollectionId: sequentialCollId,
						RecordNames:  update.DeletedRecords,
						IsDelete:     false,
					}}, groups, userId)
					if err != nil {
						s.log.Errorf("error deleting sequential collection records: %v", err)
						return nil, 0, s.log.ErrorfErr("error deleting sequential collection records: %v", err)
					}
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
					updateResp, err := s.metadataManager.UpdateGroupsInCollection(sequentialCollId, groups, userId, groupsToAdd, groupsToRemove)
					if err != nil {
						s.log.Errorf("error updating sequential collection groups: %v", err)
						return nil, 0, s.log.ErrorfErr("error updating sequential collection groups: %v", err)
					}

					err = s.dataManager.UpdateCollectionsGroupsAndPropagate([]messages.CollectionGroupUpdate{{
						CollectionId: sequentialCollId,
						GroupNames:   updateResp.Groups,
						Sequence:     updateResp.Sequence,
					}})
					if err != nil {
						s.log.Errorf("error propagating sequential collection group updates: %v", err)
						return nil, 0, s.log.ErrorfErr("error propagating sequential collection group updates: %v", err)
					}
				}
			}
			delete(resp.CacheUpdates.CollectionUpdates, sequentialCollId)
		}

		seqNumber, err = s.dataManager.GetMigrationManager().GetCollectionVectorClockValue(sequentialCollId)
		if err != nil {
			return nil, 0, s.log.ErrorfErr("error getting sequential collection vector clock value: %v", err)
		}
	}

	go func() {
		var updateMessage []messages.CollectionUpdate
		var groupUpdateMessage []messages.CollectionGroupUpdate

		for collId, update := range resp.CacheUpdates.CollectionUpdates {
			if update.Deleted {
				go func() {
					err := s.dataManager.RemoveCollectionAndPropagate(collId, groups, userId)
					if err != nil {
						s.log.Errorf("error removing collection: %v", err)
					}
				}()
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
				updateResp, err := s.metadataManager.UpdateGroupsInCollection(sequentialCollId, groups, userId, groupsToAdd, groupsToRemove)
				if err != nil {
					s.log.Errorf("error updating eventual collection groups: %v", err)
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
			err := s.dataManager.UpdateCollectionsRecordsAndPropagate(updateMessage, groups, userId)
			if err != nil {
				s.log.Errorf("error propagating collection updates: %v", err)
			}
		}
		if len(groupUpdateMessage) > 0 {
			err = s.dataManager.UpdateCollectionsGroupsAndPropagate(groupUpdateMessage)
			if err != nil {
				s.log.Errorf("error propagating sequential collection group updates: %v", err)
			}
		}

		s.sendUpdateToGTP(resp.CacheUpdates.Groups)
	}()

	go func() {
		var collRequestInfo []messages.CollectionRequestInfo
		for _, colls := range collections {
			for _, coll := range colls {
				collRequestInfo = append(collRequestInfo, messages.CollectionRequestInfo{
					CollectionId:   coll.Id,
					NodeName:       reqHostname,
					IsWrite:        writeSeqCollection,
					RequestLatency: s.serverInfo.GetLatencyFor(reqHostname),
				})
			}
		}

		s.dataManager.GetMigrationManager().UpdatesRequestsInfo(collRequestInfo)
	}()

	s.log.Infof("Returning result: %v", result)

	return result, seqNumber, nil
}

func (s *SchedulerImpl) sendUpdateToGTP(groupsCache model.GroupsCache) {
	for groupName, toCreate := range groupsCache.Groups {
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
				s.log.Errorf("error marshalling group participants: %v", err)
				continue
			}
			statusCode, err := s.makeHTTPRequest(http.MethodPost, fmt.Sprintf("http://%s/group/%s", s.serverInfo.GroupTokenProviderHostName, groupName), bytes.NewReader(body))
			if err != nil {
				s.log.Errorf("error creating request to GTP: %v", err)
				continue
			}

			if statusCode != http.StatusCreated {
				s.log.Errorf("error creating group %s: received status code %d", groupName, statusCode)
				continue
			}
		} else {
			statusCode, err := s.makeHTTPRequest(http.MethodDelete, fmt.Sprintf("http://%s/group/%s", s.serverInfo.GroupTokenProviderHostName, groupName), nil)
			if err != nil {
				s.log.Errorf("error creating request to GTP: %v", err)
				continue
			}

			if statusCode != http.StatusOK {
				s.log.Errorf("error creating group %s: received status code %d", groupName, statusCode)
				continue
			}

			delete(groupsCache.UsersInGroups, groupName)
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
			statusCode, err := s.makeHTTPRequest(method, fmt.Sprintf("http://%s/group/%s/%s", s.serverInfo.GroupTokenProviderHostName, groupName, user), nil)
			if err != nil {
				s.log.Errorf("error updating group %s: %v", groupName, err)
				continue
			}
			if statusCode != http.StatusOK {
				s.log.Errorf("error updating group %s: received status code %d", groupName, statusCode)
				continue
			}
		}
	}
}

func (s *SchedulerImpl) makeHTTPRequest(method string, url string, body io.Reader) (int, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		s.log.Errorf("error creating request to GTP: %v", err)
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", s.serverInfo.Secret)
	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		s.log.Errorf("error sending request to GTP: %v", err)
		return 0, err
	}
	err = resp.Body.Close()
	if err != nil {
		s.log.Errorf("error closing response body: %v", err)
		return 0, err
	}
	return resp.StatusCode, nil
}

func (s *SchedulerImpl) forwardRequest(dest string, functionName string, functionVersion string, collections map[string][]types.CollectionInfo, groupToken string, seqNumber uint64) (*FunctionResult, uint64, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/pvt-offload-function/%s/%s", dest, functionName, functionVersion), nil)
	if err != nil {
		return nil, 0, s.log.ErrorfErr("error creating request to leader: %v", err)
	}
	req.Header.Set("X-ERMES-GROUPS", groupToken)
	collectionsJson, err := json.Marshal(collections)
	if err != nil {
		return nil, 0, s.log.ErrorfErr("error marshalling collections: %v", err)
	}
	req.Header.Set("X-ERMES-COLLECTIONS", string(collectionsJson))
	req.Header.Set("X-ERMES-SECRET", s.serverInfo.Secret)
	req.Header.Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", seqNumber))
	req.Header.Set("X-ERMES-HOSTNAME", s.serverInfo.HostName)

	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		return nil, 0, s.log.ErrorfErr("error forwarding request to leader: %v", err)
	}

	sequentialSeqNumberStr := resp.Header.Get("X-ERMES-SEQUENTIAL-NUMBER")
	var sequentialSeqNumber uint64
	if sequentialSeqNumberStr != "" {
		_, err = fmt.Sscanf(sequentialSeqNumberStr, "%d", &sequentialSeqNumber)
		if err != nil {
			return nil, 0, s.log.ErrorfErr("error parsing sequential sequence number: %v", err)
		}
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, s.log.ErrorfErr("error reading response from leader: %v", err)
	}
	return &FunctionResult{
		StatusCode: resp.StatusCode,
		Body:       body,
	}, sequentialSeqNumber, nil
}
