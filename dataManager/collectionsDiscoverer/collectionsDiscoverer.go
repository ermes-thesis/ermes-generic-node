package collectionsDiscoverer

import (
	"dataManager/messages"
	"dataManager/migrationManager"
	"dataManager/sender"
	shared "dataManager/shared"
	"db"
	"errors"
	log "logger"
	"server"
	"server/info"
	"sync"
	"time"
)

type CollectionsDiscoverer interface {
	IsCollectionLocallyPresent(collectionId string, consistencyClock uint) (bool, shared.Collection, error)
	Init(*shared.CollectionPartialView, info.ServerInfo, server.ServerType, log.Logger, db.GenericDB)

	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, recordNames string) (string, error)
	WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, recordNames ...string) ([]any, error)

	WeakCollectionRemoteRecordWrite(hostId string, collectionId string, recordNames string, value string) error
	WeakCollectionRemoteRecordsWrite(hostId string, collectionId string, recordNames []string, recordValues []string) error

	PropagateUpdates(updates []messages.CollectionUpdate, isDelete bool)

	SetCollectionAndProgate(collectionId string, records map[string]string, groups []string, migrationManager migrationManager.MigrationManager, isStrong bool) error
}

type CollectionsDiscovererImp struct {
	//data
	collectionsPartialView *shared.CollectionPartialView
	serverInfo             info.ServerInfo
	sender                 sender.CollectionDiscovererSender
	logger                 log.Logger
	dbInterface            db.GenericDB
}

// Singleton pattern
var collectionsDiscovererInstance CollectionsDiscoverer
var once sync.Once
var initOnce sync.Once

func GetCollectionsDiscovererInstance(logger log.Logger) CollectionsDiscoverer {
	once.Do(func() {
		collectionsDiscovererInstance = &CollectionsDiscovererImp{}
	})
	return collectionsDiscovererInstance
}

func (c *CollectionsDiscovererImp) Init(collections *shared.CollectionPartialView, serverInfo info.ServerInfo, server server.ServerType, logger log.Logger, dbInterface db.GenericDB) {
	initOnce.Do(func() {
		c.collectionsPartialView = collections
		c.serverInfo = serverInfo
		c.logger = logger
		c.sender = sender.NewCollectionDiscovererSender(logger, server)
		c.dbInterface = dbInterface

		server.AddCustomRoute(c.sender.RemoteAccessPath(), true, c.handleIncomingRemoteAccess(logger).ServeHTTP)
		server.AddCustomRoute(c.sender.RemoteUpdatePath(), true, c.handleIncomingRemoteUpdate(logger).ServeHTTP)
		server.AddCustomRoute(c.sender.UpdatePropagationPath(), true, c.handleIncomingUpdatePropagation(logger).ServeHTTP)
		server.AddCustomRoute(c.sender.CreateCollectionPropagationPath(), true, c.handleIncomingNewCollection(logger).ServeHTTP)
	})
}

// methods
func (c *CollectionsDiscovererImp) IsCollectionLocallyPresent(collectionId string, consistencyClock uint) (bool, shared.Collection, error) {
	//sync
	c.collectionsPartialView.Mutex.Lock()
	defer c.collectionsPartialView.Mutex.Unlock()
	//imp
	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		return false, nil, nil
	}
	if !collection.IsLocal() {
		return false, collection, nil
	}
	if collection.IsStrong() {
		if collection.(*shared.StrongCollection).ConsistencyClock < consistencyClock {
			return true, collection, errors.New("outdated data in a strong collection replica")
		}
	}

	return true, collection, nil
}

func (m *CollectionsDiscovererImp) WeakCollectionRemoteRecordAccess(hostId string, collectionId string, recordName string) (string, error) {
	res, error := m.sender.WeakCollectionRemoteRecordAccess(hostId, collectionId, recordName)
	if len(res) != 0 {
		return "", errors.New("unexpected output")
	}
	return res[0].(string), error
}
func (m *CollectionsDiscovererImp) WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, recordNames ...string) ([]any, error) {
	return m.sender.WeakCollectionRemoteRecordAccess(hostId, collectionId, recordNames...)
}

func (m *CollectionsDiscovererImp) WeakCollectionRemoteRecordWrite(hostId string, collectionId string, recordNames string, value string) error {
	return m.sender.WeakCollectionRemoteRecordUpdate(hostId, collectionId, []string{recordNames}, []string{value})
}

func (m *CollectionsDiscovererImp) WeakCollectionRemoteRecordsWrite(hostId string, collectionId string, recordNames []string, recordValues []string) error {
	return m.sender.WeakCollectionRemoteRecordUpdate(hostId, collectionId, recordNames, recordValues)
}

// use generics to preserve type-safety, unfortunately generics are not supported for methods.
func AccessCollection[T any](
	c *CollectionsDiscovererImp,
	collectionId string,
	dbFn func() (T, error),
	remoteFn func(addr string) (T, error),
) (T, error) {
	//sync
	c.collectionsPartialView.Mutex.Lock()
	//imp

	var zero T
	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		if c.serverInfo.Father == nil {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return zero, errors.New("collection not found")
		}
		addr := c.serverInfo.Father.HostName
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return remoteFn(addr)
	}
	if collection.IsLocal() {
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return dbFn()
	}
	if collection.IsStrong() {
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return zero, errors.New("strong collection can't be accessed remotely")
	}

	// fallback su un host a caso (TODO: scegliere in base a latenza)
	var oneKey string
	for k := range collection.HostsId() {
		oneKey = k
		break
	}

	c.collectionsPartialView.Mutex.Unlock() // lock release
	return remoteFn(oneKey)
}

func UpdateCollection(
	c *CollectionsDiscovererImp,
	collectionId string,
	dbFn func() error,
	remoteFn func(addr string) error,
) error {
	//sync
	c.collectionsPartialView.Mutex.Lock()
	//imp

	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		if c.serverInfo.Father == nil {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return errors.New("collection not found")
		}
		if collection.IsStrong() {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return errors.New("strong collection can't be updated remotely")
		}
		addr := c.serverInfo.Father.HostName
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return remoteFn(addr)
	}

	if !collection.IsLocal() {
		if collection.IsStrong() {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return errors.New("strong collection can't be updated remotely")
		}

		// fallback su un host a caso (TODO: scegliere in base a latenza)
		var oneKey string
		for k := range collection.HostsId() {
			oneKey = k
			break
		}

		c.collectionsPartialView.Mutex.Unlock() // lock release
		return remoteFn(oneKey)
	}

	if collection.IsStrong() {
		collection.(*shared.StrongCollection).ConsistencyClock++
	}

	c.collectionsPartialView.Mutex.Unlock() // lock release
	return dbFn()
}

func (c *CollectionsDiscovererImp) PropagateUpdates(updates []messages.CollectionUpdate, isDelete bool) {
	time := time.Now().Unix()
	for _, update := range updates {
		updateMessage := messages.CollectionUpdateMessage{
			CollectionId: update.CollectionId,
			RecordNames:  update.RecordNames,
			RecordValues: update.RecordValues,
			Time:         time,
			UpdateHostIp: c.serverInfo.HostName,
			SenderIp:     c.serverInfo.HostName,
			IsDelete:     isDelete,
		}
		if isDelete {
			updateMessage.RecordValues = []string{}
		}
		//sync
		c.collectionsPartialView.Mutex.Lock()
		//imp
		c.propagateUpdate(updateMessage, false, []string{}, isDelete)

		c.collectionsPartialView.Mutex.Unlock() // lock release
	}
}

func (c *CollectionsDiscovererImp) propagateUpdate(updateMessage messages.CollectionUpdateMessage, toUpdate bool, ignoredIp []string, isDelete bool) {
	updateMessage.SenderIp = c.serverInfo.HostName
	collection, exists := c.collectionsPartialView.KnownCollections[updateMessage.CollectionId]
	if exists {
		if collection.IsLocal() && !collection.IsStrong() { //check for conflitcs
			res := c.CheckUpdateConflicts(collection, updateMessage, isDelete)
			if !res {
				return
			}
		}
		if toUpdate && !isDelete {
			c.dbInterface.SetRecords(updateMessage.CollectionId, updateMessage.RecordNames, updateMessage.RecordValues)
		} else if toUpdate && isDelete {
			c.dbInterface.DeleteRecords(updateMessage.CollectionId, updateMessage.RecordNames...)
		}

		hosts := collection.HostsId()
		for host := range hosts {
			if !contains(ignoredIp, host) {
				c.sender.PropagateUpdate(host, updateMessage)
			}
		}
	}

	//always send update to father
	if c.serverInfo.Father == nil {
		return
	}
	addr := c.serverInfo.Father.HostName
	if !contains(ignoredIp, addr) {
		c.sender.PropagateUpdate(addr, updateMessage)
	}
}

func (c *CollectionsDiscovererImp) CheckUpdateConflicts(collection shared.Collection, updateMessage messages.CollectionUpdateMessage, isDelete bool) bool {
	//check conflict
	res := collection.(*shared.WeakCollection).SetLastUpdate(updateMessage.Time, updateMessage.UpdateHostIp)
	if !res {
		records, err := c.dbInterface.GetRecords(updateMessage.CollectionId, updateMessage.RecordNames...)
		if err != nil {
			return false
		}

		//propagate back the conflict resoultion
		updateMessage := messages.CollectionUpdateMessage{
			CollectionId: updateMessage.CollectionId,
			RecordNames:  updateMessage.RecordNames,
			IsDelete:     isDelete,
			RecordValues: func() []string {
				strRecords := make([]string, len(records))
				for i, r := range records {
					strRecords[i] = r.(string)
				}
				return strRecords
			}(),
			Time:         collection.(*shared.WeakCollection).GetLastUpdateTimeStamp(),
			UpdateHostIp: collection.(*shared.WeakCollection).GetLastUpdateSenderIp(),
		}
		c.propagateUpdate(updateMessage, false, []string{}, isDelete)
		return false
	}
	return true
}

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func (c *CollectionsDiscovererImp) SetCollectionAndProgate(collectionId string, records map[string]string, groups []string, migrationManager migrationManager.MigrationManager, isStrong bool) error {
	//sync
	c.collectionsPartialView.Mutex.Lock()
	defer c.collectionsPartialView.Mutex.Unlock()
	//imp

	//if collection  already exists return error
	_, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if exists {
		return errors.New("collection already exists")
	}

	//try to add collection in redis
	dbErr := c.dbInterface.SetCollection(collectionId, records, groups)
	if dbErr != nil {
		return dbErr
	}

	//get size of collection in db
	size, sizeErr := c.dbInterface.GetCollectionMemoryUsage(collectionId)
	if sizeErr != nil {
		return sizeErr
	}

	//add collection in local view
	migrationManagerErr := migrationManager.NewCollectionCreated(collectionId, size, isStrong)
	if migrationManagerErr != nil {
		return migrationManagerErr
	}

	//propagate update to father
	if c.serverInfo.Father != nil {
		newCollectionMessage := messages.NewCollectionMessage{
			CollectionId: collectionId,
			IsStrong:     isStrong,
			SenderIp:     c.serverInfo.HostName,
		}
		c.sender.NewCollectionCreated(c.serverInfo.Father.HostName, newCollectionMessage)
	}

	return nil
}
