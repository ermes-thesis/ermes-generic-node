package scheduler

import (
	"caches"
	datamanager "dataManager"
	shared "dataManager/shared"
	"encoding/json"
	"fmt"
	"io"
	"metadataManager"
	"metadataManager/utils"
	"net/http"
	"server"
	"server/client"
	"server/info"
	"time"
	"tokenDecoder"

	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
)

type Scheduler interface {
	Schedule(functionName string, functionVersion string, collections map[string][]utils.CollectionInfo, sequentialSeqNumber uint64, reqBody []byte, groups []string, userId string, groupToken string) (*FunctionResult, error)
}

type FunctionResult struct {
	StatusCode int
	Body       []byte
}

type SchedulerImpl struct {
	dataManager      datamanager.DataManager
	cache            caches.FunctionCache
	wasmExecutorPool wasm.ExecutorPool
	serverInfo       *info.ServerInfo
	logger           io.Writer
}

func NewScheduler(dataManager datamanager.DataManager, cache caches.FunctionCache, wasmExecutorPool wasm.ExecutorPool, serverInfo *info.ServerInfo, logger io.Writer) *SchedulerImpl {
	return &SchedulerImpl{
		dataManager:      dataManager,
		cache:            cache,
		wasmExecutorPool: wasmExecutorPool,
		serverInfo:       serverInfo,
		logger:           logger,
	}
}

func (s *SchedulerImpl) RegisterRoutes(server server.ServerType, tokenDecoder tokenDecoder.TokenDecoder, metadataManager metadataManager.MetadataManager) error {
	err := server.AddCustomRoute("/invoke-function/{name}/{version}/", false, s.handleFunctionInvocation(tokenDecoder, metadataManager))
	if err != nil {
		return fmt.Errorf("error adding route /invoke-function/{name}/{version}/: %v", err)
	}
	err = server.AddCustomRoute("/invoke-function/{name}", false, s.handleFunctionInvocation(tokenDecoder, metadataManager))
	if err != nil {
		return fmt.Errorf("error adding route /invoke-function/{name}: %v", err)
	}

	err = server.AddCustomRoute("/pvt-offload-function/{name}/{version}/", true, s.handleOffloadFunctionInvocation(tokenDecoder))
	if err != nil {
		return fmt.Errorf("error adding route /pvt-offload-function/{name}/{version}/: %v", err)
	}
	err = server.AddCustomRoute("/pvt-offload-function/{name}", true, s.handleOffloadFunctionInvocation(tokenDecoder))
	if err != nil {
		return fmt.Errorf("error adding route /pvt-offload-function/{name}: %v", err)
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
	return found, collection, fmt.Errorf("after %d retries: %w", retries, err)
}

func (s *SchedulerImpl) Schedule(functionName string, functionVersion string, collections map[string][]utils.CollectionInfo, sequentialSeqNumber uint64, reqBody []byte, groups []string, userId string, groupToken string) (*FunctionResult, uint64, error) {
	fmt.Println("Scheduling function:", functionName, "version:", functionVersion)
	fmt.Printf("With collections: %v\n", collections)

	if len(collections) == 0 {
		return s.executeLocally(functionName, functionVersion, collections, reqBody, groups, userId, "")
	}

	var sequentialCollection shared.Collection
	sequentialCollectionNeeded := false
	eventualCollectionPresent := false
	eventualHostIds := make(map[string]uint)
	hostWithMoreEventual := ""
	if s.serverInfo.Father != nil {
		hostWithMoreEventual = s.serverInfo.Father.HostName
	}
	maxEventual := uint(0)
	for _, cols := range collections {
		for _, col := range cols {
			found, collection, err := s.retryIsCollectionLocallyPresent(col.Id, sequentialSeqNumber, 10, 500*time.Millisecond)
			if err != nil {
				return nil, 0, fmt.Errorf("wrong sequence number for collection: %v, %w", col.Id, err)
			}
			if col.Metadata.Consistency == utils.SequentialConsistent {
				if sequentialCollectionNeeded {
					return nil, 0, fmt.Errorf("multiple sequentially consistent collections are not supported")
				}
				sequentialCollection = collection
				sequentialCollectionNeeded = true
			} else {
				if found {
					eventualCollectionPresent = true
				}
				if collection != nil {
					for hostId := range collection.HostsId() {
						eventualHostIds[hostId]++
						if eventualHostIds[hostId] > maxEventual {
							maxEventual = eventualHostIds[hostId]
							hostWithMoreEventual = hostId
						}
					}
				} else {
					eventualHostIds[s.serverInfo.Father.HostName]++
					if eventualHostIds[s.serverInfo.Father.HostName] > maxEventual {
						maxEventual = eventualHostIds[s.serverInfo.Father.HostName]
						hostWithMoreEventual = s.serverInfo.Father.HostName
					}
				}
			}
		}
	}

	if sequentialCollectionNeeded {
		if sequentialCollection == nil {
			return s.forwardRequest(s.serverInfo.Father.HostName, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
		}

		functionMetadata, err := s.cache.GetMetadata(functionName, functionVersion)
		if err != nil {
			return nil, 0, fmt.Errorf("error retrieving function metadata: %v", err)
		}

		seqCollectionCasted, ok := sequentialCollection.(*shared.StrongCollection)
		if sequentialCollection.IsLocal() {
			if !ok {
				return nil, 0, fmt.Errorf("collection %s is not a strong collection", sequentialCollection)
			}

			if seqCollectionCasted.IsLeader || !functionMetadata.StrongCollWriteAccess {
				return s.executeLocally(functionName, functionVersion, collections, reqBody, groups, userId, seqCollectionCasted.CollectionId())
			} else {
				// Forward to seqCollectionCasted.LeaderId
				return s.forwardRequest(seqCollectionCasted.LeaderId, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
			}
		} else {
			// Forward to the leader or to a replica based on the functionMetadata.StrongCollWriteAccess
			if functionMetadata.StrongCollWriteAccess {
				// Forward to seqCollectionCasted.LeaderId
				return s.forwardRequest(seqCollectionCasted.LeaderId, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
			} else {
				// Forward to any node that has the collection
				//TODO: check if maps in Go are iterated consistently --> No, it matters here?
				for hostId := range seqCollectionCasted.HostsId() {
					return s.forwardRequest(hostId, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
				}
				return s.forwardRequest(s.serverInfo.Father.HostName, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
			}
		}
	} else if eventualCollectionPresent {
		return s.executeLocally(functionName, functionVersion, collections, reqBody, groups, userId, "")
	} else {
		return s.forwardRequest(hostWithMoreEventual, functionName, functionVersion, collections, groupToken, sequentialSeqNumber)
	}
}

func (s *SchedulerImpl) executeLocally(functionName string, functionVersion string, collections map[string][]utils.CollectionInfo, reqBody []byte, groups []string, userId string, sequentialCollId string) (*FunctionResult, uint64, error) {
	executor := s.wasmExecutorPool.Acquire()
	defer executor.Release()
	ctx := WasmContext{
		UserId:      userId,
		GroupList:   groups,
		Collections: collections,
	}
	ctxJson, err := json.Marshal(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("error marshalling wasm context: %v", err)
	}

	resp := executor.Invoke(functionName+"@"+functionVersion, "entry", reqBody, ctxJson)
	repBody, err := json.Marshal(resp.Response)

	// TODO: Use resp.CollectionsToCommit to create goroutines that calls dataManager remote write method
	// dataManager.RemoteWrite(resp.CollectionsToCommit)

	// TODO: Use resp.DeleteRecords to create goroutines that calls dataManager remote delete method
	// dataManager.RemoteDelete(resp.DeletedRecords)

	if err != nil {
		return nil, 0, fmt.Errorf("error marshalling response body: %v", err)
	}

	var seqNumber uint64
	if sequentialCollId != "" {
		seqNumber, err = s.dataManager.GetMigrationManager().GetCollectionVectorClockValue(sequentialCollId)
		if err != nil {
			return nil, 0, fmt.Errorf("error getting sequential collection vector clock value: %v", err)
		}
	}

	return &FunctionResult{
		StatusCode: resp.HttpCode,
		Body:       repBody,
	}, seqNumber, nil
}

func (s *SchedulerImpl) forwardRequest(dest string, functionName string, functionVersion string, collections map[string][]utils.CollectionInfo, groupToken string, seqNumber uint64) (*FunctionResult, uint64, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/pvt-offload-function/%s/%s", dest, functionName, functionVersion), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("error creating request to leader: %v", err)
	}
	req.Header.Set("X-ERMES-GROUPS", groupToken)
	collectionsJson, err := json.Marshal(collections)
	if err != nil {
		return nil, 0, fmt.Errorf("error marshalling collections: %v", err)
	}
	req.Header.Set("X-ERMES-COLLECTIONS", string(collectionsJson))
	req.Header.Set("X-ERMES-SECRET", s.serverInfo.Secret)
	req.Header.Set("X-ERMES-SEQUENTIAL-NUMBER", fmt.Sprintf("%d", seqNumber))

	resp, err := client.CustomClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("error forwarding request to leader: %v", err)
	}

	sequentialSeqNumberStr := resp.Header.Get("X-ERMES-SEQUENTIAL-NUMBER")
	var sequentialSeqNumber uint64
	if sequentialSeqNumberStr != "" {
		_, err = fmt.Sscanf(sequentialSeqNumberStr, "%d", &sequentialSeqNumber)
		if err != nil {
			return nil, 0, fmt.Errorf("error parsing sequential sequence number: %v", err)
		}
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("error reading response from leader: %v", err)
	}
	return &FunctionResult{
		StatusCode: resp.StatusCode,
		Body:       body,
	}, sequentialSeqNumber, nil
}
