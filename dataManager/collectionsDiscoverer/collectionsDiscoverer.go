package collectionsDiscoverer

import (
	shared "dataManager/shared"
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
var lock = &sync.Mutex{}

func GetCollectionsDiscovererInstance(logger log.Logger) CollectionsDiscoverer {
	if collectionsDiscovererInstance == nil {
		lock.Lock()
		defer lock.Unlock()
		if collectionsDiscovererInstance == nil {
			logger.Println("COLLECTION-DISCOVERER", "create new collection discoverer instance", log.INFO)
			collectionsDiscovererInstance = &CollectionsDiscovererImp{}
		}
	}
	return collectionsDiscovererInstance
}

func (m *CollectionsDiscovererImp) Init(collections *shared.CollectionPartialView) {
	//sync
	lock.Lock()
	defer lock.Unlock()
	//imp
	if m.collectionsPartialView != nil { //if singleton already initialized
		return
	}
	m.collectionsPartialView = collections
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
