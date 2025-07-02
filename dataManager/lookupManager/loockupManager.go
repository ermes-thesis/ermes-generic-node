package lookupmanager

import (
	shared "dataManager/shared"
	"fmt"
	"sync"
)

type LookupManager interface {
	IsCollectionLocallyPresent(collectionId string) (bool, shared.Collection)
}

type LookupManagerImp struct {
	//sync

	//data
	collectionsPartialView *shared.CollectionPartialView
}

// singleton pattern
var lookupManagerInstance *LookupManagerImp
var lock = &sync.Mutex{}

func GetLokupManagerMigrationManagerImp() *LookupManagerImp {
	if lookupManagerInstance == nil {
		lock.Lock()
		defer lock.Unlock()
		if lookupManagerInstance == nil {
			fmt.Println("Creating single instance now.")
			lookupManagerInstance = &LookupManagerImp{}
		}
	}
	return lookupManagerInstance
}

func (m *LookupManagerImp) Init(collections *shared.CollectionPartialView) {
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
func (m *LookupManagerImp) IsCollectionLocallyPresent(collectionId string) (bool, shared.Collection) {
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
