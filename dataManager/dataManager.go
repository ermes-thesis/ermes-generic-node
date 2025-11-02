package datamanager

import (
	"config"
	"dataManager/collectionsDiscoverer"
	"dataManager/lru"
	"dataManager/messages"
	"dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	shared "dataManager/shared"
	"db"
	"server"
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

type DataManager interface {
	// Init initialize the singleton pattern
	Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo server.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger *logger.Logger)

	//sub components
	GetMigrationManager() migrationManager.MigrationManager
	GetCollectionsDiscoverer() collectionsDiscoverer.CollectionsDiscoverer

	//methods
	GetRecord(collectionId string, groups []string, userId string, recordName string) (string, error)
	GetRecords(collectionId string, groups []string, userId string, recordNames ...string) ([]any, error)
	UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdate, groups []string, userId string) error
	UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdate) error
	SetCollectionAndPropagate(collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, isStrong bool, skipUniquenessCheck bool) error
	RemoveCollectionAndPropagate(collectionId string, groups []string, userId string) error
}

type DataManagerImp struct {
	migrationManager      migrationManager.MigrationManager
	collectionsDiscoverer collectionsDiscoverer.CollectionsDiscoverer

	dbInterface db.GenericDB
	serverInfo  server.ServerInfo
}

// Singleton pattern
var dataManagerInstance *DataManagerImp
var once sync.Once
var initOnce sync.Once

func GetDataManagerInstance() DataManager {
	once.Do(func() {
		dataManagerInstance = &DataManagerImp{}
		dataManagerInstance.setMigrationManager(migrationManager.GetMigrationManagerInstance())
		dataManagerInstance.setCollectionsDiscoverer(collectionsDiscoverer.GetCollectionsDiscovererInstance())
	})
	return dataManagerInstance
}

func (m *DataManagerImp) Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo server.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger *logger.Logger) {
	initOnce.Do(func() {
		m.migrationManager.Init(collections, penalityFunc, config, lruConfig, server, serverInfo, lruCache, dbInterface, logger)
		m.collectionsDiscoverer.Init(collections, serverInfo, server, dbInterface, logger)

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

func (m *DataManagerImp) GetRecord(collectionId string, groups []string, userId string, recordName string) (string, error) {
	res, err := m.collectionsDiscoverer.AccessRecords(
		collectionId,
		func() (any, error) {
			authorized, err := m.dbInterface.IsAuthorized(collectionId, userId, groups...)
			if err != nil {
				return "", err
			}
			if !authorized {
				return "", &shared.UnauthorizedError{}
			}

			return m.dbInterface.GetRecord(collectionId, recordName)
		},
		func(addr string) (any, error) {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordAccess(addr, collectionId, groups, userId, recordName)
		},
	)

	return res.(string), err
}

func (m *DataManagerImp) GetRecords(collectionId string, groups []string, userId string, recordNames ...string) ([]any, error) {
	res, err := m.collectionsDiscoverer.AccessRecords(
		collectionId,
		func() (any, error) {
			authorized, err := m.dbInterface.IsAuthorized(collectionId, userId, groups...)
			if err != nil {
				return nil, err
			}
			if !authorized {
				return nil, &shared.UnauthorizedError{}
			}

			return m.dbInterface.GetRecords(collectionId, recordNames...)
		},
		func(addr string) (any, error) {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordsAccess(addr, collectionId, groups, userId, recordNames...)
		},
	)
	return res.([]any), err
}

func (m *DataManagerImp) UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdate, groups []string, userId string) error {
	return m.collectionsDiscoverer.UpdateCollectionsRecordsAndPropagate(messages.ToCollectionUpdateMessages(
		updates,
		m.serverInfo.HostName,
		m.serverInfo.HostName,
	), groups, userId, true, m.serverInfo.HostName,
	)
}

func (m *DataManagerImp) UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdate) error {
	return m.collectionsDiscoverer.UpdateCollectionsGroupsAndPropagate(messages.ToCollectionGroupUpdateMessages(
		updates,
		m.serverInfo.HostName,
	), true, m.serverInfo.HostName)
}

func (m *DataManagerImp) SetCollectionAndPropagate(collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, isStrong bool, skipUniquenessCheck bool) error {
	return m.collectionsDiscoverer.SetCollectionAndPropagate(
		collectionId,
		collectionName,
		records,
		groups,
		userId,
		m.migrationManager,
		isStrong,
		skipUniquenessCheck,
	)
}

func (m *DataManagerImp) RemoveCollectionAndPropagate(collectionId string, groups []string, userId string) error {
	return m.collectionsDiscoverer.RemoveCollectionAndPropagate(collectionId, m.migrationManager, groups, userId, true, m.serverInfo.HostName)
}
