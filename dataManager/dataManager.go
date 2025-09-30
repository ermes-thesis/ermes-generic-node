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
	"logger"
	"server"
	"server/info"
	"sync"
)

type DataManager interface {
	// Init initialize the singleton pattern
	Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger *logger.Logger)

	//sub components
	GetMigrationManager() migrationManager.MigrationManager

	GetCollectionsDiscoverer() collectionsDiscoverer.CollectionsDiscoverer

	//methods
	GetRecord(collectionId string, recordName string, groups []string) (string, error)
	SetRecord(collectionId string, recordName string, recordValue string, groups []string) error
	GetRecords(collectionId string, groups []string, recordNames ...string) ([]any, error)
	SetRecords(collectionId string, groups []string, recordNames []string, recordValues []string) error
	PropagateUpdates(updates []messages.CollectionUpdate)
}

type DataManagerImp struct {
	migrationManager      migrationManager.MigrationManager
	collectionsDiscoverer collectionsDiscoverer.CollectionsDiscoverer

	dbInterface db.GenericDB
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

func (m *DataManagerImp) Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger *logger.Logger) {
	initOnce.Do(func() {
		m.migrationManager.Init(collections, penalityFunc, config, lruConfig, server, serverInfo, lruCache, dbInterface, logger)
		m.collectionsDiscoverer.Init(collections, serverInfo, server, dbInterface, logger)

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

func (m *DataManagerImp) GetRecord(collectionId string, recordName string, groups []string) (string, error) {
	return collectionsDiscoverer.AccessCollection(
		m.collectionsDiscoverer.(*collectionsDiscoverer.CollectionsDiscovererImp),
		collectionId,
		func() (string, error) {
			authorized, err := m.dbInterface.IsInGroups(collectionId, groups...)
			if err != nil {
				return "", err
			}
			if !authorized {
				return "", &shared.UnauthorizedError{}
			}

			return m.dbInterface.GetRecord(collectionId, recordName)
		},
		func(addr string) (string, error) {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordAccess(addr, collectionId, groups, recordName)
		},
	)
}

func (m *DataManagerImp) SetRecord(collectionId string, recordName string, recordValue string, groups []string) error {
	return collectionsDiscoverer.UpdateCollection(
		m.collectionsDiscoverer.(*collectionsDiscoverer.CollectionsDiscovererImp),
		collectionId,
		func() error {
			authorized, err := m.dbInterface.IsInGroups(collectionId, groups...)
			if err != nil {
				return err
			}
			if !authorized {
				return &shared.UnauthorizedError{}
			}
			return m.dbInterface.SetRecord(collectionId, recordName, recordValue)
		},
		func(addr string) error {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordWrite(addr, collectionId, groups, recordName, recordValue)
		},
	)
}

func (m *DataManagerImp) GetRecords(collectionId string, groups []string, recordNames ...string) ([]any, error) {
	return collectionsDiscoverer.AccessCollection(
		m.collectionsDiscoverer.(*collectionsDiscoverer.CollectionsDiscovererImp),
		collectionId,
		func() ([]any, error) {
			authorized, err := m.dbInterface.IsInGroups(collectionId, groups...)
			if err != nil {
				return nil, err
			}
			if !authorized {
				return nil, &shared.UnauthorizedError{}
			}

			return m.dbInterface.GetRecords(collectionId, recordNames...)
		},
		func(addr string) ([]any, error) {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordsAccess(addr, collectionId, groups, recordNames...)
		},
	)
}

func (m *DataManagerImp) SetRecords(collectionId string, groups []string, recordNames []string, recordValues []string) error {
	return collectionsDiscoverer.UpdateCollection(
		m.collectionsDiscoverer.(*collectionsDiscoverer.CollectionsDiscovererImp),
		collectionId,
		func() error {
			authorized, err := m.dbInterface.IsInGroups(collectionId, groups...)
			if err != nil {
				return err
			}
			if !authorized {
				return &shared.UnauthorizedError{}
			}

			return m.dbInterface.SetRecords(collectionId, recordNames, recordValues)
		},
		func(addr string) error {
			return m.collectionsDiscoverer.WeakCollectionRemoteRecordsWrite(addr, collectionId, groups, recordNames, recordValues)
		},
	)
}

func (m *DataManagerImp) PropagateUpdates(updates []messages.CollectionUpdate) {
	m.collectionsDiscoverer.PropagateUpdates(updates)
}
