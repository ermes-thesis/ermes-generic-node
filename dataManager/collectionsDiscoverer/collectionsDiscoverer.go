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
)

type CollectionsDiscoverer interface {
	IsCollectionLocallyPresent(collectionId string, consistencyClock uint) (bool, shared.Collection, error)
	Init(*shared.CollectionPartialView, info.ServerInfo, server.ServerType, log.Logger, db.GenericDB)

	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, recordNames string) (string, error)
	WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, recordNames ...string) ([]any, error)

	UpdateCollectionsRecorsAndPropogate(updates []messages.CollectionUpdateMessage, isInitiator bool, ignoredHostName string) error
	UpdateCollectionsGroupsAndPropogate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error
	SetCollectionAndProgate(collectionId string, records map[string]string, groups []string, migrationManager migrationManager.MigrationManager, isStrong bool) error
	RemoveCollectionAndProgate(collectionId string, migrationManager migrationManager.MigrationManager, isInitiator bool, ignoredHostName string) error
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
		server.AddCustomRoute(c.sender.UpdatePropagationPath(), true, c.handleIncomingUpdatePropagation(logger).ServeHTTP)
		server.AddCustomRoute(c.sender.UpdateGroupPropagationPath(), true, c.handleIncomingUpdateGroupPropagation(logger).ServeHTTP)
		server.AddCustomRoute(c.sender.CreateCollectionPropagationPath(), true, c.handleIncomingNewCollection(logger).ServeHTTP)
		server.AddCustomRoute(c.sender.RemoveCollectionPropagationPath(), true, c.handleIncomingRemoveCollection(logger).ServeHTTP)
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
	c.collectionsPartialView.Mutex.Unlock() // lock release
	return remoteFn(c.getClosestChild(collection.HostsId()))
}

func UpdateRecord(
	c *CollectionsDiscovererImp,
	collectionId string,
	dbFn func(isStrong bool) error,
	isInitiator bool,
) (map[string]struct{}, error) {
	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	var isStrong bool = false
	if !exists {
		//can't verify if collection is strong or weak, so just notify father

		hosts := map[string]struct{}{}
		if c.serverInfo.Father == nil {
			return nil, errors.New("collection not found")
		}
		addr := c.serverInfo.Father.HostName
		hosts[addr] = struct{}{}
		return hosts, nil
	}
	if !collection.IsLocal() {
		if collection.IsStrong() && isInitiator { //safety check
			return nil, errors.New("strong collection can't be updated remotely")
		}

		hosts := collection.HostsId()
		if c.serverInfo.Father == nil {
			return hosts, nil
		}

		addr := c.serverInfo.Father.HostName
		hosts[addr] = struct{}{}
		return hosts, nil
	}

	if collection.IsStrong() {
		if isInitiator && !collection.(*shared.StrongCollection).IsLeader { //safety check
			return nil, errors.New("only leader can update strong collection")
		}

		isStrong = true
		collection.(*shared.StrongCollection).ConsistencyClock++
	}

	//always notify to father
	hosts := collection.HostsId()
	if c.serverInfo.Father == nil {
		return hosts, nil
	}

	addr := c.serverInfo.Father.HostName
	hosts[addr] = struct{}{}
	return hosts, dbFn(isStrong) // update in local db
}

func (c *CollectionsDiscovererImp) UpdateCollectionsRecorsAndPropogate(updates []messages.CollectionUpdateMessage, isInitiator bool, ignoredHostName string) error {
	var msgs map[string]messages.CollectionUpdateMessages = make(map[string]messages.CollectionUpdateMessages) // host -> message
	//sync
	c.collectionsPartialView.Mutex.Lock()
	//imp

	//write or delete in db and prepare messages for future propagation on other hosts
	for _, update := range updates {
		var dbFn func(isStrong bool) error
		if update.IsDelete {
			dbFn = func(isStrong bool) error {
				return c.dbInterface.DeleteRecords(update.CollectionId, update.RecordNames...)
			}
		} else {
			dbFn = func(isStrong bool) error {
				return c.dbInterface.SetRecords(update.CollectionId, update.RecordNames, update.RecordValues)
			}
		}

		//check conflict for strong update

		//write or delete in db
		hosts, err := UpdateRecord(c, update.CollectionId, dbFn, isInitiator)
		if err != nil {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return err
		}
		update.SenderIp = c.serverInfo.HostName

		for host := range hosts { // for each host that has the collection prepare the message
			if host == ignoredHostName {
				continue
			}
			if _, ok := msgs[host]; !ok {
				msgs[host] = make(messages.CollectionUpdateMessages, 0)
			}
			msgs[host] = append(msgs[host], update)

		}
	}
	c.collectionsPartialView.Mutex.Unlock() // lock release

	//propagate update to other hosts
	for host, msg := range msgs {
		c.sender.PropagateUpdate(host, msg)
	}

	return nil
}

func (c *CollectionsDiscovererImp) UpdateCollectionsGroupsAndPropogate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error {
	var msgs map[string]messages.CollectionGroupUpdateMessages = make(map[string]messages.CollectionGroupUpdateMessages) // host -> message
	//sync
	c.collectionsPartialView.Mutex.Lock()

	for _, update := range updates {
		var dbFn func(isStrong bool) error
		if update.IsDelete {
			dbFn = func(isStrong bool) error {
				return c.dbInterface.RemoveGroups(update.CollectionId, update.GroupNames...)
			}
		} else {
			dbFn = func(isStrong bool) error {
				return c.dbInterface.AddGroups(update.CollectionId, update.GroupNames...)
			}
		}

		//write or delete in db
		hosts, err := UpdateRecord(c, update.CollectionId, dbFn, isInitiator)
		if err != nil {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return err
		}
		update.SenderIp = c.serverInfo.HostName

		for host := range hosts { // for each host that has the collection prepare the message
			if host == ignoredHostName {
				continue
			}
			if _, ok := msgs[host]; !ok {
				msgs[host] = make(messages.CollectionGroupUpdateMessages, 0)
			}
			msgs[host] = append(msgs[host], update)
		}
	}
	c.collectionsPartialView.Mutex.Unlock() // lock release

	//propagate update to other hosts
	for host, msg := range msgs {
		c.sender.PropagateGroupUpdate(host, msg)
	}
	return nil
}

func (c *CollectionsDiscovererImp) SetCollectionAndProgate(collectionId string, records map[string]string, groups []string, migrationManager migrationManager.MigrationManager, isStrong bool) error {
	//sync
	c.collectionsPartialView.Mutex.Lock()
	//imp

	//if collection  already exists return error
	_, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if exists {
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return errors.New("collection already exists")
	}

	//try to add collection in redis
	dbErr := c.dbInterface.SetCollection(collectionId, records, groups)
	if dbErr != nil {
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return dbErr
	}

	//get size of collection in db
	size, sizeErr := c.dbInterface.GetCollectionMemoryUsage(collectionId)
	if sizeErr != nil {
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return sizeErr
	}

	//add collection in local view
	migrationManagerErr := migrationManager.NewCollectionCreated(collectionId, size, isStrong)
	if migrationManagerErr != nil {
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return migrationManagerErr
	}
	c.collectionsPartialView.Mutex.Unlock() // lock release

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

func (c *CollectionsDiscovererImp) RemoveCollectionAndProgate(collectionId string, migrationManager migrationManager.MigrationManager, isInitiator bool, ignoredHostName string) error {
	//sync
	c.collectionsPartialView.Mutex.Lock()
	//imp

	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		if c.serverInfo.Father != nil && c.serverInfo.Father.HostName != ignoredHostName {
			c.sender.RemoveCollection(c.serverInfo.Father.HostName, collectionId, c.serverInfo.HostName) //progate to father
			return nil
		}
		return errors.New("collection doesn't exists")
	}

	if !collection.IsLocal() {
		if collection.IsStrong() {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return errors.New("strong collection can't be removed remotely")
		}

		c.collectionsPartialView.Mutex.Unlock()                                // lock release
		c.propagateRemoveCollection(collection, collectionId, ignoredHostName) //progate to other hosts
		return nil
	}

	c.collectionsPartialView.KnownCollections[collectionId] = nil //remove from local view
	c.dbInterface.DeleteCollection(collectionId)                  //remove from local db
	migrationManager.RemoveCollectionFromLru(collectionId)        //remove from lru

	c.collectionsPartialView.Mutex.Unlock()                                // lock release
	c.propagateRemoveCollection(collection, collectionId, ignoredHostName) //progate to other hosts

	return nil
}

func (c *CollectionsDiscovererImp) propagateRemoveCollection(collection shared.Collection, collectionId string, ignoredHostName string) {
	hosts := collection.HostsId()
	for host := range hosts {
		if host == ignoredHostName {
			continue
		}
		c.sender.RemoveCollection(host, collectionId, c.serverInfo.HostName)
	}

	if c.serverInfo.Father != nil && c.serverInfo.Father.HostName != ignoredHostName {
		c.sender.RemoveCollection(c.serverInfo.Father.HostName, collectionId, c.serverInfo.HostName)
	}
}

func (c *CollectionsDiscovererImp) getClosestChild(hostSet map[string]struct{}) string {
	var minLatency uint64
	var minLatencyHost string

	for _, child := range c.serverInfo.Children {
		if _, ok := hostSet[child.HostName]; !ok {
			continue
		}
		if minLatencyHost == "" || child.Latency < minLatency {
			minLatency = child.Latency
			minLatencyHost = child.HostName
		}
	}

	return minLatencyHost
}
