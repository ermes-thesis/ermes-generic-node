package collectionsDiscoverer

import (
	shared "dataManager/shared"
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

// singleton pattern
var collectionsDiscovererInstance *CollectionsDiscovererImp
var lock = &sync.Mutex{}

func GetCollectionsDiscovererInstance() *CollectionsDiscovererImp {
	if collectionsDiscovererInstance == nil {
		lock.Lock()
		defer lock.Unlock()
		if collectionsDiscovererInstance == nil {
			//fmt.Println("Creating single instance now.")
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
	if m.collectionsPartialView != nil { //if singleton allready inizialized
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
	collection, exists := m.collectionsPartialView.KnowedCollections[collectionId]
	if !exists {
		return false, nil
	}
	if !collection.IsLocal() {
		return false, collection
	}
	return true, collection
}
