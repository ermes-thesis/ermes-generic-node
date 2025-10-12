//go:build test
// +build test

package datamanager

import (
	cld "dataManager/collectionsDiscoverer"
	mig "dataManager/migrationManager"
	"sync"
)

func ResetSingleton() {
	mig.ResetSingleton()
	cld.ResetSingleton()

	dataManagerInstance = nil
	once = sync.Once{}
	initOnce = sync.Once{}
}
