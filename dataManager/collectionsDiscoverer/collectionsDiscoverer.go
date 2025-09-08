package collectionsDiscoverer

import (
	shared "dataManager/shared"
	"fmt"
	log "logger"
	"sync"
)

type CollectionsDiscoverer interface {
	IsCollectionLocallyPresent(collectionId string) (bool, shared.Collection)
	Init(*shared.CollectionPartialView)
}

type CollectionsDiscovererImp struct {
	//data
	collectionsPartialView *shared.CollectionPartialView
}

// Singleton pattern
var collectionsDiscovererInstance CollectionsDiscoverer
var once sync.Once
var initOnce sync.Once

func GetCollectionsDiscovererInstance(logger log.Logger) CollectionsDiscoverer {
	once.Do(func() {
		fmt.Println("create singleton instance")
		collectionsDiscovererInstance = &CollectionsDiscovererImp{}
	})
	return collectionsDiscovererInstance
}

func (m *CollectionsDiscovererImp) Init(collections *shared.CollectionPartialView) {
	initOnce.Do(func() {
		m.collectionsPartialView = collections
	})
}

// methods
func (m *CollectionsDiscovererImp) IsCollectionLocallyPresent(collectionId string) (bool, shared.Collection) {
	//sync
	m.collectionsPartialView.Mutex.Lock()
	defer m.collectionsPartialView.Mutex.Unlock()
	//imp
	collection, exists := m.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		return false, nil
	}
	if !collection.IsLocal() {
		return false, collection
	}
	return true, collection
}
