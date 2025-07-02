package lookupmanager

import (
	shared "dataManager/shared"
	"fmt"
	"sync"
)

type LookupManager interface {
	//TODO
}

type LookupManagerImp struct {
	//sync
	mutex sync.Mutex
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
	m.mutex.Lock()
	defer m.mutex.Unlock()
	//imp
	//imp
	if m.collectionsPartialView != nil { //if singleton allready inizialized
		return
	}
	m.collectionsPartialView = collections
}
