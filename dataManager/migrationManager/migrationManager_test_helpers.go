//go:build test
// +build test

package migrationManager

import (
	"config"
	lru "dataManager/lru"
	penality "dataManager/migrationManager/penalityFunction"
	"dataManager/migrationManager/strongPolicy"
	weakpolicy "dataManager/migrationManager/weakPolicy"
	"dataManager/sender"
	shared "dataManager/shared"
	"db"
	"server"
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

func ResetSingleton() {
	migrationManagerInstance = nil
	once = sync.Once{}
	initOnce = sync.Once{}
}

func (m *MigrationManagerImp) GetLru() *lru.LRUCache[shared.Collection] {
	return m.lruCache
}

func NewMigrationManagerForTest(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo server.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger logger.Logger) MigrationManager {
	m := &MigrationManagerImp{
		config:                 config,
		serverInfo:             serverInfo,
		log:                    logger,
		collectionsPartialView: collections,
		collectionsLock:        &collections.Mutex,
		lruCache:               lruCache,
		dbInterface:            dbInterface,
		sender:                 sender.NewMigrationManagerSender(logger, server, serverInfo, collections, dbInterface, lruCache),
	}
	server.AddCustomRoute(m.sender.GossipPath(), true, m.handleIncomingGossipMessages().ServeHTTP)
	server.AddCustomRoute(m.sender.RepoPath(), true, m.handleIncomingCollection().ServeHTTP)
	server.AddCustomRoute(m.sender.RequestInfoPath(), true, m.handleIncomingRequestInfoMessages().ServeHTTP)

	wp := weakpolicy.GetPolicy(config.WeakPolicy)
	sp := strongPolicy.GetPolicy(config.StrongPolicy)

	if strongPolicy.GetPolicy(config.StrongPolicy) == strongPolicy.DONOTHINGPOLICY {
		m.StrongCollectionPolicy = strongPolicy.NewDoNothingPolicy()
	} else {
		m.StrongCollectionPolicy = strongPolicy.NewDefaultPolicy(config, lruConfig, collections, penalityFunc, dbInterface, serverInfo, m.sender, m.lruCache, m.log)
	}

	if weakpolicy.GetPolicy(config.WeakPolicy) == weakpolicy.DONOTHINGPOLICY {
		m.doNothingPolicy = weakpolicy.NewDoNothingPolicy()
	} else {
		m.WeakCollectionPolicy = weakpolicy.NewAlwaysCopyPolicy(config, m.sender, m.log, collections, dbInterface, serverInfo)
	}

	//policy update strategy
	if wp == weakpolicy.DONOTHINGPOLICY && sp == strongPolicy.DONOTHINGPOLICY {
		m.policyUpdateStrategy = newNoUpdateStrategy()
	} else {
		m.policyUpdateStrategy = newErmesPolicyUpdateStrategy()
	}
	return m
}
