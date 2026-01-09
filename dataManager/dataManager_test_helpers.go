//go:build test
// +build test

package datamanager

import (
	"config"
	cld "dataManager/collectionsDiscoverer"
	"dataManager/lru"
	mig "dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	shared "dataManager/shared"
	"db"
	"server"
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

func ResetSingleton() {
	mig.ResetSingleton()
	cld.ResetSingleton()

	dataManagerInstance = nil
	once = sync.Once{}
	initOnce = sync.Once{}
}

func NewDataManagerForTest(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo server.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger logger.Logger) DataManager {
	mutex := &sync.Mutex{}
	return &DataManagerImp{
		migrationManager:      mig.NewMigrationManagerForTest(collections, penalityFunc, config, lruConfig, server, serverInfo, lruCache, dbInterface, logger.Component("migration_manager"), mutex),
		collectionsDiscoverer: cld.NewCollectionsDiscovererForTest(collections, serverInfo, server, dbInterface, logger.Component("collections_discoverer"), lruCache, mutex),
		dbInterface:           dbInterface,
		serverInfo:            serverInfo,
	}
}
