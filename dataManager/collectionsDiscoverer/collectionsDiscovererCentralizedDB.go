package collectionsDiscoverer

import (
	"dataManager/lru"
	"dataManager/messages"
	"dataManager/migrationManager"
	shared "dataManager/shared"
	"db"
	"errors"
	"server"
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

// decorator pattern
type CollectionsDiscovererCentralizedDB struct {
	wrapped          *CollectionsDiscovererImp
	migrationManager migrationManager.MigrationManager
}

func SetCollectionsDiscovererCentralizedDBInstance(wrapped *CollectionsDiscovererImp, migrationManager migrationManager.MigrationManager) CollectionsDiscoverer {
	c := &CollectionsDiscovererCentralizedDB{}
	c.wrapped = wrapped
	c.migrationManager = migrationManager
	return c
}

func (c *CollectionsDiscovererCentralizedDB) Init(collections *shared.CollectionPartialView, serverInfo server.ServerInfo, server server.ServerType, dbInterface db.GenericDB, logger logger.Logger, lruCache *lru.LRUCache[shared.Collection], mutex *sync.Mutex) {
	logger.Info("Init collections discoverer as centralized DB...")
	c.wrapped.Init(collections, serverInfo, server, dbInterface, logger, lruCache, mutex)
	server.AddCustomRoute(c.wrapped.getSender().RemoteCollectionSetPath(), true, c.wrapped.handleIncomingRemoteCollectionSet(c.migrationManager).ServeHTTP)
}

func (c *CollectionsDiscovererCentralizedDB) IsCollectionLocallyPresent(collectionId string, consistencyClock uint64) (bool, shared.Collection, error) {
	return true, nil, nil
}

func (c *CollectionsDiscovererCentralizedDB) AccessRecords(collectionId string, dbFn func() (any, error), remoteFn func(addr string) (any, error)) (any, error) {
	return c.wrapped.AccessRecords(collectionId, dbFn, remoteFn)
}

func (c *CollectionsDiscovererCentralizedDB) WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, userId string, recordName string) (string, error) {
	return c.wrapped.WeakCollectionRemoteRecordAccess(hostId, collectionId, groups, userId, recordName)
}

func (c *CollectionsDiscovererCentralizedDB) WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, groups []string, userId string, recordNames ...string) ([]any, error) {
	return c.wrapped.WeakCollectionRemoteRecordsAccess(hostId, collectionId, groups, userId, recordNames...)
}

func (c *CollectionsDiscovererCentralizedDB) UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdateMessage, groups []string, userId string, isInitiator bool, ignoredHostName string) error {
	return c.wrapped.UpdateCollectionsRecordsAndPropagate(updates, groups, userId, isInitiator, ignoredHostName)
}

func (c *CollectionsDiscovererCentralizedDB) UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error {
	return c.wrapped.UpdateCollectionsGroupsAndPropagate(updates, isInitiator, ignoredHostName)
}

func (c *CollectionsDiscovererCentralizedDB) SetCollectionAndPropagate(collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, migrationManager migrationManager.MigrationManager, isStrong bool, skipUniquenessCheck bool) error {
	return c.wrapped.SetCollectionAndPropagate(collectionId, collectionName, records, groups, userId, migrationManager, isStrong, skipUniquenessCheck)
}

func (c *CollectionsDiscovererCentralizedDB) RemoveCollectionAndPropagate(collectionId string, migrationManager migrationManager.MigrationManager, groups []string, userId string, isInitiator bool, ignoredHostName string) error {
	return c.wrapped.RemoveCollectionAndPropagate(collectionId, migrationManager, groups, userId, isInitiator, ignoredHostName)
}

func (c *CollectionsDiscovererCentralizedDB) GetCollectionVectorClockValue(collectionId string) (uint64, error) {
	return 0, errors.New("cannot execute GetCollectionVectorClockValue on centralized DB")
}
