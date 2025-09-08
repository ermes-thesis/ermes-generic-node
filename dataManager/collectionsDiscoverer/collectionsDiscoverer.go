package collectionsDiscoverer

import (
	shared "dataManager/shared"
	"errors"
	log "logger"
	"sync"
)

type CollectionsDiscoverer interface {
	IsCollectionLocallyPresent(collectionId string, consistencyClock uint) (bool, shared.Collection, error)
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
func (m *CollectionsDiscovererImp) IsCollectionLocallyPresent(collectionId string, consistencyClock uint) (bool, shared.Collection, error) {
	//sync
	m.collectionsPartialView.Mutex.Lock()
	defer m.collectionsPartialView.Mutex.Unlock()
	//imp
	collection, exists := m.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		return false, nil, nil
	}
	if !collection.IsLocal() {
		return false, collection, nil
	}
	if collection.IsStrong() {
		if collection.(*shared.StrongCollection).ConsistencyClock < consistencyClock {
			return true, collection, errors.New("outdated data in a strong collection replica")
		}
	}

	return true, collection, nil
}
