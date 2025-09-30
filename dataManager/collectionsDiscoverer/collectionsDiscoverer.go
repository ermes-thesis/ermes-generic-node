package collectionsDiscoverer

import (
	"dataManager/messages"
	"dataManager/migrationManager"
	"dataManager/sender"
	shared "dataManager/shared"
	"db"
	"errors"
	"logger"
	"server"
	"server/info"
	"sync"
	"time"
)

type CollectionsDiscoverer interface {
	IsCollectionLocallyPresent(collectionId string, consistencyClock uint64) (bool, shared.Collection, error)
	Init(*shared.CollectionPartialView, info.ServerInfo, server.ServerType, db.GenericDB, *logger.Logger)

	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, recordNames string) (string, error)
	WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, groups []string, recordNames ...string) ([]any, error)

	WeakCollectionRemoteRecordWrite(hostId string, collectionId string, groups []string, recordNames string, value string) error
	WeakCollectionRemoteRecordsWrite(hostId string, collectionId string, groups []string, recordNames []string, recordValues []string) error

	WeakCollectionRemoteRecordDelete(hostId string, collectionId string, groups []string, recordNames []string) error

	PropagateUpdates(updates []messages.CollectionUpdate, isDelete bool)

	SetCollectionAndPropagate(collectionId string, records map[string]string, groups []string, migrationManager migrationManager.MigrationManager, isStrong bool) error
}

type CollectionsDiscovererImp struct {
	//data
	collectionsPartialView *shared.CollectionPartialView
	serverInfo             info.ServerInfo
	sender                 sender.CollectionDiscovererSender
	log                    *logger.Logger
	dbInterface            db.GenericDB
}

// Singleton pattern
var collectionsDiscovererInstance CollectionsDiscoverer
var once sync.Once
var initOnce sync.Once

func GetCollectionsDiscovererInstance() CollectionsDiscoverer {
	once.Do(func() {
		collectionsDiscovererInstance = &CollectionsDiscovererImp{}
	})
	return collectionsDiscovererInstance
}

func (c *CollectionsDiscovererImp) Init(collections *shared.CollectionPartialView, serverInfo info.ServerInfo, server server.ServerType, dbInterface db.GenericDB, logger *logger.Logger) {
	initOnce.Do(func() {
		c.collectionsPartialView = collections
		c.serverInfo = serverInfo
		c.log = logger
		c.sender = sender.NewCollectionDiscovererSender(logger, server)
		c.dbInterface = dbInterface

		server.AddCustomRoute(c.sender.RemoteAccessPath(), true, c.handleIncomingRemoteAccess().ServeHTTP)
		server.AddCustomRoute(c.sender.RemoteUpdatePath(), true, c.handleIncomingRemoteUpdate().ServeHTTP)
		server.AddCustomRoute(c.sender.UpdatePropagationPath(), true, c.handleIncomingUpdatePropagation().ServeHTTP)
		server.AddCustomRoute(c.sender.CreateCollectionPropagationPath(), true, c.handleIncomingNewCollection().ServeHTTP)
	})
}

// methods
func (c *CollectionsDiscovererImp) IsCollectionLocallyPresent(collectionId string, consistencyClock uint64) (bool, shared.Collection, error) {
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

func (c *CollectionsDiscovererImp) WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, recordName string) (string, error) {
	res, err := c.sender.WeakCollectionRemoteRecordAccess(hostId, collectionId, groups, recordName)
	if len(res) != 0 {
		return "", errors.New("unexpected output")
	}
	return res[0].(string), err
}
func (c *CollectionsDiscovererImp) WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, groups []string, recordNames ...string) ([]any, error) {
	return c.sender.WeakCollectionRemoteRecordAccess(hostId, collectionId, groups, recordNames...)
}

func (c *CollectionsDiscovererImp) WeakCollectionRemoteRecordWrite(hostId string, collectionId string, groups []string, recordName string, value string) error {
	return c.sender.WeakCollectionRemoteRecordUpdate(hostId, collectionId, groups, []string{recordName}, []string{value}, false, c.serverInfo)
}

func (c *CollectionsDiscovererImp) WeakCollectionRemoteRecordsWrite(hostId string, collectionId string, groups []string, recordNames []string, recordValues []string) error {
	return c.sender.WeakCollectionRemoteRecordUpdate(hostId, collectionId, groups, recordNames, recordValues, false, c.serverInfo)
}

func (c *CollectionsDiscovererImp) WeakCollectionRemoteRecordDelete(hostId string, collectionId string, groups []string, recordNames []string) error {
	return c.sender.WeakCollectionRemoteRecordUpdate(hostId, collectionId, groups, recordNames, []string{}, true, c.serverInfo)
}

// use generics to preserve type-safety, unfortunately generics are not supported for methods.
func AccessRecords[T any](
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

func UpdateRecords(
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
	return dbFn()                           // update in local db
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
		if collection.IsLocal() && !collection.IsStrong() { //check for conflicts
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

		//propagate back the conflict resolution
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

func (c *CollectionsDiscovererImp) SetCollectionAndPropagate(collectionId string, records map[string]string, groups []string, migrationManager migrationManager.MigrationManager, isStrong bool) error {
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
