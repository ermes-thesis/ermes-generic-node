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

func NewCollectionsDiscovererForTest(collections *shared.CollectionPartialView, serverInfo server.ServerInfo, server server.ServerType, dbInterface db.GenericDB, logger logger.Logger, lruCache *lru.LRUCache[shared.Collection]) CollectionsDiscoverer {
	c := &CollectionsDiscovererImp{
		collectionsPartialView: collections,
		serverInfo:             serverInfo,
		sender:                 sender.NewCollectionDiscovererSender(logger, server),
		log:                    logger,
		dbInterface:            dbInterface,
		lruCache:               lruCache,
	}
	server.AddCustomRoute(c.sender.RemoteAccessPath(), true, "GET,POST", c.handleIncomingRemoteAccess)
	server.AddCustomRoute(c.sender.UpdatePropagationPath(), true, "GET,POST", c.handleIncomingUpdatePropagation)
	server.AddCustomRoute(c.sender.UpdateGroupPropagationPath(), true, "GET,POST", c.handleIncomingUpdateGroupPropagation)
	server.AddCustomRoute(c.sender.CreateCollectionPropagationPath(), true, "GET,POST", c.handleIncomingNewCollection)
	server.AddCustomRoute(c.sender.RemoveCollectionPropagationPath(), true, "GET,POST,DELETE", c.handleIncomingRemoveCollection)
	return c
}
