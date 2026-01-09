//go:build test
// +build test

package collectionsDiscoverer

import (
	"dataManager/lru"
	"dataManager/sender"
	shared "dataManager/shared"
	"db"
	"server"
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

func ResetSingleton() {
	collectionsDiscovererInstance = nil
	once = sync.Once{}
	initOnce = sync.Once{}
}

func NewCollectionsDiscovererForTest(collections *shared.CollectionPartialView, serverInfo server.ServerInfo, server server.ServerType, dbInterface db.GenericDB, logger logger.Logger, lruCache *lru.LRUCache[shared.Collection], mutex *sync.Mutex) CollectionsDiscoverer {
	c := &CollectionsDiscovererImp{
		collectionsPartialView: collections,
		serverInfo:             serverInfo,
		sender:                 sender.NewCollectionDiscovererSender(logger, server),
		log:                    logger,
		dbInterface:            dbInterface,
		lruCache:               lruCache,
		mutex:                  mutex,
	}

	server.AddCustomRoute(c.sender.RemoteAccessPath(), true, c.handleIncomingRemoteAccess().ServeHTTP)
	server.AddCustomRoute(c.sender.UpdatePropagationPath(), true, c.handleIncomingUpdatePropagation().ServeHTTP)
	server.AddCustomRoute(c.sender.UpdateGroupPropagationPath(), true, c.handleIncomingUpdateGroupPropagation().ServeHTTP)
	server.AddCustomRoute(c.sender.CreateCollectionPropagationPath(), true, c.handleIncomingNewCollection().ServeHTTP)
	server.AddCustomRoute(c.sender.RemoveCollectionPropagationPath(), true, c.handleIncomingRemoveCollection().ServeHTTP)
	return c
}
