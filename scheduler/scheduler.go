package scheduler

import (
	"caches"
	"dataManager/collectionsDiscoverer"
	shared "dataManager/shared"
	"encoding/json"
	"fmt"
	"metadataManager/utils"

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
	wasmExecutorPool     *wasm.ExecutorPool
}

func NewScheduler(collectionDiscoverer collectionsDiscoverer.CollectionsDiscoverer, cache caches.FunctionCache, wasmExecutorPool *wasm.ExecutorPool) Scheduler {
	return &SchedulerImpl{
		collectionDiscoverer: collectionDiscoverer,
		cache:                cache,
		wasmExecutorPool:     wasmExecutorPool,
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
	for _, cols := range collections {
		for _, col := range cols {
			found, collection := s.collectionDiscoverer.IsCollectionLocallyPresent(col.Id)
			if col.Metadata.Consistency == utils.SequentialConsistent {
				if sequentialCollection != nil {
					return nil, fmt.Errorf("multiple sequentially consistent collections are not supported")
				}
				sequentialCollection = collection
			} else if found {
				eventualCollectionPresent = true
			}
		}
	}

	functionMetadata, err := s.cache.GetMetadata(functionName, functionVersion)
	if err != nil {
		return nil, fmt.Errorf("error retrieving function metadata: %v", err)
	}

	if sequentialCollection != nil {
		if sequentialCollection.IsLocal() {
			seqCollectionCasted, ok := sequentialCollection.(*shared.StrongCollection)
			if !ok {
				return nil, fmt.Errorf("collection %s is not a strong collection", sequentialCollection)
			}

			if seqCollectionCasted.IsLeader || !functionMetadata.StrongCollWriteAccess {
				return s.executeLocally(functionName, functionVersion, collections, reqBody, groups, userId)
			} else {
				// Forward to seqCollectionCasted.LeaderId
			}
		} else {
			// Forward to the leader or to a replica based on the functionMetadata.StrongCollWriteAccess
		}
	} else if eventualCollectionPresent {
		return s.executeLocally(functionName, functionVersion, collections, reqBody, groups, userId)
	} else {
		// Forward to any node that has one of the eventual collections
	}

	result := &FunctionResult{
		StatusCode: 500,
		Body:       []byte(fmt.Sprintf("Function %s@%s not executed!!!!", functionName, functionVersion)),
	}
	return result, nil

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
