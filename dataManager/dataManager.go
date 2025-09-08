package datamanager

import (
	"config"
	"dataManager/collectionsDiscoverer"
	"dataManager/lru"
	"dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	shared "dataManager/shared"
	"db"
	"errors"
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
}

type DataManagerImp struct {
	MigrationManager      migrationManager.MigrationManager
	CollectionsDiscoverer collectionsDiscoverer.CollectionsDiscoverer

	collectionsPartialView *shared.CollectionPartialView
	dbInterface            db.GenericDB
	serverInfo             info.ServerInfo
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
		m.MigrationManager.Init(collections, penalityFunc, config, lruConfig, server, serverInfo, lruCache, dbInterface, logger)
		m.CollectionsDiscoverer.Init(collections)

		m.collectionsPartialView = collections
		m.dbInterface = dbInterface
		m.serverInfo = serverInfo
	})
}

func (m *DataManagerImp) setMigrationManager(mig migrationManager.MigrationManager) {
	m.MigrationManager = mig
}

func (m *DataManagerImp) GetMigrationManager() migrationManager.MigrationManager {
	return m.MigrationManager
}

func (m *DataManagerImp) setCollectionsDiscoverer(cl collectionsDiscoverer.CollectionsDiscoverer) {
	m.CollectionsDiscoverer = cl
}

func (m *DataManagerImp) GetCollectionsDiscoverer() collectionsDiscoverer.CollectionsDiscoverer {
	return m.CollectionsDiscoverer
}

func (m *DataManagerImp) GetRecord(collectionId string, recordName string) (string, error) {
	//sync
	m.collectionsPartialView.Mutex.Lock()
	defer m.collectionsPartialView.Mutex.Unlock()
	//imp
	collection, exists := m.collectionsPartialView.KnownCollections[collectionId]

	if !exists {
		//if not exist => not placed locally or in one child => search in father
		return m.weakCollectionRemoteRecordAccess(m.serverInfo.Father.Address, collectionId, recordName) //TODO: no father case
	}
	if collection.IsLocal() {
		return m.dbInterface.GetRecord(collectionId, recordName)
	}

	if collection.IsStrong() {
		return "", errors.New("strong collection can't be accessed remotely")
	} else {
		var oneKey string
		for k := range collection.HostsId() { //TODO: If multiple hosts are present, where should the request be sent? min latency?
			oneKey = k
			break
		}
		return m.weakCollectionRemoteRecordAccess(oneKey, collectionId, recordName)
	}
}

func (m *DataManagerImp) SetRecord(collectionId string, recordName string, recordValue string) error {
	//sync
	m.collectionsPartialView.Mutex.Lock()
	defer m.collectionsPartialView.Mutex.Unlock()
	//imp
	collection, exists := m.collectionsPartialView.KnownCollections[collectionId]

	if !exists || !collection.IsLocal() {
		return errors.New("write operations can't be done remotely")
	}

	if collection.IsStrong() {
		collection.(*shared.StrongCollection).ConsistencyClock++
	}
	return m.dbInterface.SetRecord(collectionId, recordName, recordValue)
}

func (m *DataManagerImp) GetRecords(collectionId string, recordNames ...string) ([]any, error) {
	//sync
	m.collectionsPartialView.Mutex.Lock()
	defer m.collectionsPartialView.Mutex.Unlock()
	//imp
	collection, exists := m.collectionsPartialView.KnownCollections[collectionId]

	if !exists {
		//if not exist => not placed locally or in one child => search in father
		return m.weakCollectionRemoteRecordsAccess(m.serverInfo.Father.Address, collectionId, recordNames...) //TODO: no father case
	}
	if collection.IsLocal() {
		return m.dbInterface.GetRecords(collectionId, recordNames...)
	}

	if collection.IsStrong() {
		return nil, errors.New("strong collection can't be accessed remotely")
	} else {
		var oneKey string
		for k := range collection.HostsId() { //TODO: If multiple hosts are present, where should the request be sent? min latency?
			oneKey = k
			break
		}
		return m.weakCollectionRemoteRecordsAccess(oneKey, collectionId, recordNames...)
	}
}

func (m *DataManagerImp) SetRecords(collectionId string, recordNames []string, recordValues []string) error {
	//sync
	m.collectionsPartialView.Mutex.Lock()
	defer m.collectionsPartialView.Mutex.Unlock()
	//imp
	collection, exists := m.collectionsPartialView.KnownCollections[collectionId]

	if !exists || !collection.IsLocal() {
		return errors.New("write operations can't be done remotely")
	}

	if collection.IsStrong() {
		collection.(*shared.StrongCollection).ConsistencyClock++
	}
	return m.dbInterface.SetRecords(collectionId, recordNames, recordValues)
}

func (m *DataManagerImp) weakCollectionRemoteRecordAccess(hostId string, collectionId string, recordNames string) (string, error) {
	return "", nil //TODO
}
func (m *DataManagerImp) weakCollectionRemoteRecordsAccess(hostId string, collectionId string, recordNames ...string) ([]any, error) {
	return nil, nil //TODO
}
