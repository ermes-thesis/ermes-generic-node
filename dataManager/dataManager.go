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
	UpdateCollectionsRecorsAndPropogate(updates []messages.CollectionUpdate) error

	SetCollectionAndProgate(collectionId string, records map[string]string, groups []string, isStrong bool) error
	RemoveCollectionAndProgate(collectionId string) error
}

type DataManagerImp struct {
	migrationManager      migrationManager.MigrationManager
	collectionsDiscoverer collectionsDiscoverer.CollectionsDiscoverer

	dbInterface db.GenericDB
	serverInfo  info.ServerInfo
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
		m.serverInfo = serverInfo
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
	return collectionsDiscoverer.AccessRecords(
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

func (m *DataManagerImp) UpdateCollectionsRecorsAndPropogate(updates []messages.CollectionUpdate) error {
	return m.collectionsDiscoverer.UpdateCollectionsRecorsAndPropogate(messages.ToCollectionUpdateMessages(
		updates,
		m.serverInfo.HostName,
		m.serverInfo.HostName,
	), true, m.serverInfo.HostName,
	)
}

func (m *DataManagerImp) SetCollectionAndProgate(collectionId string, records map[string]string, groups []string, isStrong bool) error {
	return m.collectionsDiscoverer.SetCollectionAndProgate(
		collectionId,
		records,
		groups,
		m.migrationManager,
		isStrong,
	)
}

func (m *DataManagerImp) RemoveCollectionAndProgate(collectionId string) error {
	return m.collectionsDiscoverer.RemoveCollectionAndProgate(collectionId, m.migrationManager)
}
