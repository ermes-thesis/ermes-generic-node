package lookupmanager

import (
	"sync"
)

type LookupManager interface {
	//TODO
}

type LookupManagerImp struct {
	//sync
	mutex sync.Mutex
	//data
	//TODO
}

var lookupManagerInstance *LookupManagerImp
var lock = &sync.Mutex{}
