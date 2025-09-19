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
	"server/info"

	"github.com/ermes-thesis/ermes-wasm-pool/wasm-pool"
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
	sequentialCollectionNeeded := false
	eventualCollectionPresent := false
	eventualHostIds := make(map[string]uint)
	hostWithMoreEventual := ""
	if s.serverInfo.Father != nil {
		hostWithMoreEventual = s.serverInfo.Father.Address
	}
	maxEventual := uint(0)
	for _, cols := range collections {
		for _, col := range cols {
			found, collection := s.collectionDiscoverer.IsCollectionLocallyPresent(col.Id)
			if col.Metadata.Consistency == utils.SequentialConsistent {
				if sequentialCollectionNeeded {
					return nil, fmt.Errorf("multiple sequentially consistent collections are not supported")
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
					eventualHostIds[s.serverInfo.Father.Address]++
					if eventualHostIds[s.serverInfo.Father.Address] > maxEventual {
						maxEventual = eventualHostIds[s.serverInfo.Father.Address]
						hostWithMoreEventual = s.serverInfo.Father.Address
					}
				}
			}
		}
	}

	if sequentialCollectionNeeded {
		if sequentialCollection == nil {
			return s.forwardRequest(s.serverInfo.Father.Address, functionName, functionVersion, collections, groupToken)
		}

		functionMetadata, err := s.cache.GetMetadata(functionName, functionVersion)
		if err != nil {
			return nil, fmt.Errorf("error retrieving function metadata: %v", err)
		}

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
	ctx := WasmContext{
		UserId:      userId,
		GroupList:   groups,
		Collections: collections,
	}
	ctxJson, err := json.Marshal(ctx)
	if err != nil {
		return nil, fmt.Errorf("error marshalling wasm context: %v", err)
	}

	resp := executor.Invoke(functionName+"@"+functionVersion, "entry", reqBody, ctxJson)
	repBody, err := json.Marshal(resp.Response)
	if err != nil {
		return nil, fmt.Errorf("error marshalling response body: %v", err)
	}
	return &FunctionResult{
		StatusCode: resp.HttpCode,
		Body:       repBody,
	}, nil
}

func (s *SchedulerImpl) forwardRequest(dest string, functionName string, functionVersion string, collections map[string][]utils.CollectionInfo, groupToken string) (*FunctionResult, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/offload-function/%s/%s", dest, functionName, functionVersion), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request to leader: %v", err)
	}
	req.Header.Set("X-ERMES-GROUPS", groupToken)
	collectionsJson, err := json.Marshal(collections)
	if err != nil {
		return nil, fmt.Errorf("error marshalling collections: %v", err)
	}
	req.Header.Set("X-ERMES-COLLECTIONS", string(collectionsJson))
	req.Header.Set("X-ERMES-SECRET", s.serverInfo.Secret)

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
