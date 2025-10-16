package collectionsDiscoverer

import (
	"dataManager/messages"
	"dataManager/migrationManager"
	"dataManager/sender"
	shared "dataManager/shared"
	"db"
	"errors"
	"server"
	"server/info"
	"strconv"
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

type CollectionsDiscoverer interface {
	IsCollectionLocallyPresent(collectionId string, consistencyClock uint64) (bool, shared.Collection, error)
	Init(*shared.CollectionPartialView, info.ServerInfo, server.ServerType, db.GenericDB, *logger.Logger)

	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, recordNames string) (string, error)
	WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, groups []string, recordNames ...string) ([]any, error)

	UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdateMessage, groups []string, isInitiator bool, ignoredHostName string) error
	UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error
	SetCollectionAndPropagate(collectionId string, records map[string]string, groups []string, migrationManager migrationManager.MigrationManager, isStrong bool) error
	RemoveCollectionAndPropagate(collectionId string, migrationManager migrationManager.MigrationManager, groups []string, isInitiator bool, ignoredHostName string) error
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
		server.AddCustomRoute(c.sender.UpdatePropagationPath(), true, c.handleIncomingUpdatePropagation().ServeHTTP)
		server.AddCustomRoute(c.sender.UpdateGroupPropagationPath(), true, c.handleIncomingUpdateGroupPropagation().ServeHTTP)
		server.AddCustomRoute(c.sender.CreateCollectionPropagationPath(), true, c.handleIncomingNewCollection().ServeHTTP)
		server.AddCustomRoute(c.sender.RemoveCollectionPropagationPath(), true, c.handleIncomingRemoveCollection().ServeHTTP)
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

	collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
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
		if !collection.IsStrong() {
			collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
			defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RUnlock()
		}
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return dbFn()
	}
	if collection.IsStrong() {
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return zero, errors.New("strong collection can't be accessed remotely")
	}
	c.collectionsPartialView.Mutex.Unlock() // lock release
	return remoteFn(c.collectionsPartialView.GetClosestChild(collection.HostsId(), c.serverInfo))
}

func UpdateRecord(
	c *CollectionsDiscovererImp,
	collectionId string,
	dbFn func(isStrong bool) error,
	isInitiator bool,
	updateMessage messages.UpdateMessage,
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

		_, isCollectionUpdate := updateMessage.(*messages.CollectionUpdateMessage)
		if isInitiator && isCollectionUpdate {
			collection.(*shared.StrongCollection).ConsistencyClock++
			updateMessage.SetSequence(collection.(*shared.StrongCollection).ConsistencyClock)
		} else if !isInitiator && isCollectionUpdate && collection.(*shared.StrongCollection).ConsistencyClock > updateMessage.GetSequence() {
			return nil, nil //ignore outdated update
		}

		isStrong = true
	}

	//always notify to father
	hosts := collection.HostsId()

	if c.serverInfo.Father != nil {
		addr := c.serverInfo.Father.HostName
		hosts[addr] = struct{}{}
	}

	if !collection.IsStrong() {
		collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
		defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RUnlock()
	}

	return hosts, dbFn(isStrong) // update in local db
}

func (c *CollectionsDiscovererImp) UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdateMessage, groups []string, isInitiator bool, ignoredHostName string) error {
	var msgs = make(map[string]messages.CollectionUpdateMessages) // host -> message
	//sync
	c.collectionsPartialView.Mutex.Lock()
	//imp

	//write or delete in db and prepare messages for future propagation on other hosts
	for _, update := range updates {
		var dbFn func(isStrong bool) error
		if update.IsDelete {
			dbFn = func(isStrong bool) error {
				authorized, err := c.dbInterface.IsInGroups(update.CollectionId, groups...)
				if err != nil {
					return err
				}
				if !authorized {
					return &shared.UnauthorizedError{}
				}

				if isStrong {
					return c.dbInterface.DeleteRecords(update.CollectionId, "", update.RecordNames...)
				}
				return c.dbInterface.DeleteRecords(update.CollectionId, strconv.FormatInt(update.Time, 10)+update.UpdateHostIp, update.RecordNames...)
			}
		} else {
			dbFn = func(isStrong bool) error {
				authorized, err := c.dbInterface.IsInGroups(update.CollectionId, groups...)
				if err != nil {
					return err
				}
				if !authorized {
					return &shared.UnauthorizedError{}
				}
				if isStrong {
					return c.dbInterface.SetRecords(update.CollectionId, update.RecordNames, update.RecordValues, "")
				}
				return c.dbInterface.SetRecords(update.CollectionId, update.RecordNames, update.RecordValues, strconv.FormatInt(update.Time, 10)+update.UpdateHostIp)
			}
		}

		//write or delete in db
		hosts, err := UpdateRecord(c, update.CollectionId, dbFn, isInitiator, &update)
		if err != nil {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return err
		}
		update.SenderIp = c.serverInfo.HostName

		for host := range hosts { // for each host that has the collection prepare the message
			if host == ignoredHostName {
				continue
			}
			msgs[host] = messages.CollectionUpdateMessages{Groups: groups, Messages: append(msgs[host].Messages, update)}
		}
	}
	c.collectionsPartialView.Mutex.Unlock() // lock release

	//propagate update to other hosts
	for host, msg := range msgs {
		c.sender.PropagateUpdate(host, msg)
	}

	return nil
}

func (c *CollectionsDiscovererImp) UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error {
	var msgs = make(map[string]messages.CollectionGroupUpdateMessages) // host -> message
	//sync
	c.collectionsPartialView.Mutex.Lock()

	for _, update := range updates {
		dbFn := func(isStrong bool) error {
			return c.dbInterface.SetGroups(update.CollectionId, update.Sequence, update.GroupNames...)
		}

		//TODO remove
		collection, exists := c.collectionsPartialView.KnownCollections[update.CollectionId]
		if exists && !collection.IsStrong() {
			return errors.New("unimplemented ")
		}
		//TODO end remove

		//write or delete in db
		hosts, err := UpdateRecord(c, update.CollectionId, dbFn, isInitiator, &update)
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

func (c *CollectionsDiscovererImp) SetCollectionAndPropagate(collectionId string, records map[string]string, groups []string, migrationManager migrationManager.MigrationManager, isStrong bool) error {
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
	dbErr := c.dbInterface.SetCollection(collectionId, records, groups, 0)
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

func (c *CollectionsDiscovererImp) RemoveCollectionAndPropagate(collectionId string, migrationManager migrationManager.MigrationManager, groups []string, isInitiator bool, ignoredHostName string) error {
	//sync
	c.collectionsPartialView.Mutex.Lock()
	//imp

	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		if c.serverInfo.Father != nil && c.serverInfo.Father.HostName != ignoredHostName {
			c.sender.RemoveCollection(c.serverInfo.Father.HostName, collectionId, c.serverInfo.HostName, groups) //propagate to father
			return nil
		}
		return errors.New("collection doesn't exists")
	}

	if !collection.IsLocal() {
		if collection.IsStrong() {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return errors.New("strong collection can't be removed remotely")
		}

		c.collectionsPartialView.Mutex.Unlock()                                        // lock release
		c.propagateRemoveCollection(collection, collectionId, groups, ignoredHostName) //propagate to other hosts
		return nil
	}

	authorized, err := c.dbInterface.IsInGroups(collectionId, groups...)
	if err != nil {
		return err
	}
	if !authorized {
		return &shared.UnauthorizedError{}
	}

	delete(c.collectionsPartialView.KnownCollections, collectionId) //remove from local view
	c.dbInterface.DeleteCollection(collectionId)                    //remove from local db
	migrationManager.RemoveCollectionFromLru(collectionId)          //remove from lru

	c.collectionsPartialView.Mutex.Unlock()                                        // lock release
	c.propagateRemoveCollection(collection, collectionId, groups, ignoredHostName) //propagate to other hosts

	return nil
}

func (c *CollectionsDiscovererImp) propagateRemoveCollection(collection shared.Collection, collectionId string, groups []string, ignoredHostName string) {
	hosts := collection.HostsId()
	for host := range hosts {
		if host == ignoredHostName {
			continue
		}
		c.sender.RemoveCollection(host, collectionId, c.serverInfo.HostName, groups)
	}

	if c.serverInfo.Father != nil && c.serverInfo.Father.HostName != ignoredHostName {
		c.sender.RemoveCollection(c.serverInfo.Father.HostName, collectionId, c.serverInfo.HostName, groups)
	}
}
