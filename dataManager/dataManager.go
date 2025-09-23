package datamanager

import (
	"config"
	"dataManager/collectionsDiscoverer"
	"dataManager/lru"
	messages "dataManager/messages"
	"dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	shared "dataManager/shared"
	"db"
	log "logger"
	"server"
	"server/info"
	"sync"
)

type DataManager interface {
	// Init initialize the singleton pattern
	Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger log.Logger)

	//sub components
	GetMigrationManager() migrationManager.MigrationManager
	setMigrationManager(migrationManager.MigrationManager)

	GetCollectionsDiscoverer() collectionsDiscoverer.CollectionsDiscoverer
	setCollectionsDiscoverer(collectionsDiscoverer.CollectionsDiscoverer)

	//methods
	GetRecord(collectionId string, recordName string) (string, error)
	SetRecord(collectionId string, recordName string, recordValue string) error
	GetRecords(collectionId string, recordNames ...string) ([]any, error)
	SetRecords(collectionId string, recordNames []string, recordValues []string) error
	PropagateUpdates(updates []messages.CollectionUpdate)
}

type DataManagerImp struct {
	migrationManager      migrationManager.MigrationManager
	collectionsDiscoverer collectionsDiscoverer.CollectionsDiscoverer

	dbInterface db.GenericDB
}

// Singleton pattern
var dataManagerInstance DataManager
var once sync.Once
var initOnce sync.Once

func GetDataManagerInstance(logger log.Logger) DataManager {
	once.Do(func() {
		dataManagerInstance = &DataManagerImp{}
		dataManagerInstance.setMigrationManager(migrationManager.GetMigrationManagerInstance(logger))
		dataManagerInstance.setCollectionsDiscoverer(collectionsDiscoverer.GetCollectionsDiscovererInstance(logger))
	})
	return dataManagerInstance
}

func (m *DataManagerImp) Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger log.Logger) {
	initOnce.Do(func() {
		m.migrationManager.Init(collections, penalityFunc, config, lruConfig, server, serverInfo, lruCache, dbInterface, logger)
		m.collectionsDiscoverer.Init(collections, serverInfo, server, logger, dbInterface)

		m.dbInterface = dbInterface
	})
}

func (m *DataManagerImp) setMigrationManager(mig migrationManager.MigrationManager) {
	m.migrationManager = mig
}

func (m *DataManagerImp) GetMigrationManager() migrationManager.MigrationManager {
	return m.migrationManager
}

func (m *DataManagerImp) setCollectionsDiscoverer(cl collectionsDiscoverer.CollectionsDiscoverer) {
	m.collectionsDiscoverer = cl
}

func (m *DataManagerImp) GetCollectionsDiscoverer() collectionsDiscoverer.CollectionsDiscoverer {
	return m.collectionsDiscoverer
}

func (m *DataManagerImp) GetRecord(collectionId string, recordName string) (string, error) {
	return collectionsDiscoverer.AccessCollection(
		m.collectionsDiscoverer.(*collectionsDiscoverer.CollectionsDiscovererImp),
		collectionId,
		func() (string, error) {
			return m.dbInterface.GetRecord(collectionId, recordName)
		},
		func(addr string) (string, error) {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordAccess(addr, collectionId, recordName)
		},
	)
}

func (m *DataManagerImp) SetRecord(collectionId string, recordName string, recordValue string) error {
	return collectionsDiscoverer.UpdateCollection(
		m.collectionsDiscoverer.(*collectionsDiscoverer.CollectionsDiscovererImp),
		collectionId,
		func() error {
			return m.dbInterface.SetRecord(collectionId, recordName, recordValue)
		},
		func(addr string) error {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordWrite(addr, collectionId, recordName, recordValue)
		},
	)
}

func (m *DataManagerImp) GetRecords(collectionId string, recordNames ...string) ([]any, error) {
	return collectionsDiscoverer.AccessCollection(
		m.collectionsDiscoverer.(*collectionsDiscoverer.CollectionsDiscovererImp),
		collectionId,
		func() ([]any, error) {
			return m.dbInterface.GetRecords(collectionId, recordNames...)
		},
		func(addr string) ([]any, error) {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordsAccess(addr, collectionId, recordNames...)
		},
	)
}

func (m *DataManagerImp) SetRecords(collectionId string, recordNames []string, recordValues []string) error {
	return collectionsDiscoverer.UpdateCollection(
		m.collectionsDiscoverer.(*collectionsDiscoverer.CollectionsDiscovererImp),
		collectionId,
		func() error {
			return m.dbInterface.SetRecords(collectionId, recordNames, recordValues)
		},
		func(addr string) error {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordsWrite(addr, collectionId, recordNames, recordValues)
		},
	)
}

func (m *DataManagerImp) PropagateUpdates(updates []messages.CollectionUpdate) {
	m.collectionsDiscoverer.PropagateUpdates(updates)
}
