package scheduler

import (
	"caches"
	"dataManager/collectionsDiscoverer"
	shared "dataManager/shared"
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/utils"
	"net/http"
	"scheduler/wasm-pool"
	"server/info"
)

type Scheduler interface {
	Schedule(functionName string, functionVersion string, collections map[string][]utils.CollectionInfo, reqBody []byte, groups []string, userId string, groupToken string) (*FunctionResult, error)
}

type FunctionResult struct {
	StatusCode int
	Body       []byte
}

type SchedulerImpl struct {
	collectionDiscoverer collectionsDiscoverer.CollectionsDiscoverer
	cache                caches.FunctionCache
	wasmExecutorPool     wasm.ExecutorPool
	serverInfo           *info.ServerInfo
}

func NewScheduler(collectionDiscoverer collectionsDiscoverer.CollectionsDiscoverer, cache caches.FunctionCache, wasmExecutorPool wasm.ExecutorPool, serverInfo *info.ServerInfo) Scheduler {
	return &SchedulerImpl{
		collectionDiscoverer: collectionDiscoverer,
		cache:                cache,
		wasmExecutorPool:     wasmExecutorPool,
		serverInfo:           serverInfo,
	}
}

func (s *SchedulerImpl) Schedule(functionName string, functionVersion string, collections map[string][]utils.CollectionInfo, reqBody []byte, groups []string, userId string, groupToken string) (*FunctionResult, error) {
	fmt.Println("Scheduling function:", functionName, "version:", functionVersion)
	fmt.Printf("With collections: %v\n", collections)

	if len(collections) == 0 {
		return s.executeLocally(functionName, functionVersion, collections, reqBody, groups, userId)
	}

	var sequentialCollection shared.Collection
	eventualCollectionPresent := false
	eventualHostIds := make(map[string]uint)
	hostWithMoreEventual := s.serverInfo.Father.Address
	maxEventual := uint(0)
	for _, cols := range collections {
		for _, col := range cols {
			found, collection := s.collectionDiscoverer.IsCollectionLocallyPresent(col.Id)
			if col.Metadata.Consistency == utils.SequentialConsistent {
				if sequentialCollection != nil {
					return nil, fmt.Errorf("multiple sequentially consistent collections are not supported")
				}
				sequentialCollection = collection
			} else {
				if found {
					eventualCollectionPresent = true
				}
				for hostId := range collection.HostsId() {
					eventualHostIds[hostId]++
					if eventualHostIds[hostId] > maxEventual {
						maxEventual = eventualHostIds[hostId]
						hostWithMoreEventual = hostId
					}
				}
			}
		}
	}

	functionMetadata, err := s.cache.GetMetadata(functionName, functionVersion)
	if err != nil {
		return nil, fmt.Errorf("error retrieving function metadata: %v", err)
	}

	if sequentialCollection != nil {
		seqCollectionCasted, ok := sequentialCollection.(*shared.StrongCollection)
		if sequentialCollection.IsLocal() {
			if !ok {
				return nil, fmt.Errorf("collection %s is not a strong collection", sequentialCollection)
			}

			if seqCollectionCasted.IsLeader || !functionMetadata.StrongCollWriteAccess {
				return s.executeLocally(functionName, functionVersion, collections, reqBody, groups, userId)
			} else {
				// Forward to seqCollectionCasted.LeaderId
				return s.forwardRequest(seqCollectionCasted.LeaderId, functionName, functionVersion, collections, groupToken)
			}
		} else {
			// Forward to the leader or to a replica based on the functionMetadata.StrongCollWriteAccess
			if functionMetadata.StrongCollWriteAccess {
				// Forward to seqCollectionCasted.LeaderId
				return s.forwardRequest(seqCollectionCasted.LeaderId, functionName, functionVersion, collections, groupToken)
			} else {
				// Forward to any node that has the collection
				//TODO: check if maps in Go are iterated consistently
				for hostId := range seqCollectionCasted.HostsId() {
					return s.forwardRequest(hostId, functionName, functionVersion, collections, groupToken)
				}
				return s.forwardRequest(s.serverInfo.Father.Address, functionName, functionVersion, collections, groupToken)
			}
		}
	} else if eventualCollectionPresent {
		return s.executeLocally(functionName, functionVersion, collections, reqBody, groups, userId)
	} else {
		return s.forwardRequest(hostWithMoreEventual, functionName, functionVersion, collections, groupToken)
	}
}

func (s *SchedulerImpl) executeLocally(functionName string, functionVersion string, collections map[string][]utils.CollectionInfo, reqBody []byte, groups []string, userId string) (*FunctionResult, error) {
	executor := s.wasmExecutorPool.Acquire()
	defer executor.Release()
	ctx := wasmContext{
		UserId:      userId,
		GroupList:   groups,
		Collections: collections,
	}
	ctxJson, err := json.Marshal(ctx)
	if err != nil {
		return nil, fmt.Errorf("error marshalling wasm context: %v", err)
	}

	response, httpCode := executor.InvokeFn(functionName+"@"+functionVersion, string(reqBody), string(ctxJson))
	return &FunctionResult{
		StatusCode: httpCode,
		Body:       response,
	}, nil
}

func (s *SchedulerImpl) forwardRequest(dest string, functionName string, functionVersion string, collections map[string][]utils.CollectionInfo, groupToken string) (*FunctionResult, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/invoke-function/%s/%s", dest, functionName, functionVersion), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request to leader: %v", err)
	}
	req.Header.Set("X-ERMES-GROUPS", groupToken)
	collectionsJson, err := json.Marshal(collections)
	if err != nil {
		return nil, fmt.Errorf("error marshalling collections: %v", err)
	}
	req.Header.Set("X-ERMES-CACHED-QUERY", string(collectionsJson))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error forwarding request to leader: %v", err)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response from leader: %v", err)
	}
	return &FunctionResult{
		StatusCode: resp.StatusCode,
		Body:       body,
	}, nil
}
