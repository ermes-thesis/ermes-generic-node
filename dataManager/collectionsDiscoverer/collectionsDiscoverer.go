package collectionsDiscoverer

import (
	"dataManager/messages"
	"dataManager/migrationManager"
	"dataManager/sender"
	shared "dataManager/shared"
	"db"
	"errors"
	"fmt"
	"server"
	"strconv"
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

type CollectionsDiscoverer interface {
	IsCollectionLocallyPresent(collectionId string, consistencyClock uint64) (bool, shared.Collection, error)
	Init(*shared.CollectionPartialView, server.ServerInfo, server.ServerType, db.GenericDB, *logger.Logger)

	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, userId string, recordNames string) (string, error)
	WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, groups []string, userId string, recordNames ...string) ([]any, error)

	UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdateMessage, groups []string, userId string, isInitiator bool, ignoredHostName string) error
	UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error
	SetCollectionAndPropagate(collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, migrationManager migrationManager.MigrationManager, isStrong, skipUniquenessCheck bool) error
	RemoveCollectionAndPropagate(collectionId string, migrationManager migrationManager.MigrationManager, groups []string, userId string, isInitiator bool, ignoredHostName string) error
}

type CollectionsDiscovererImp struct {
	//data
	collectionsPartialView *shared.CollectionPartialView
	serverInfo             server.ServerInfo
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

func (c *CollectionsDiscovererImp) Init(collections *shared.CollectionPartialView, serverInfo server.ServerInfo, server server.ServerType, dbInterface db.GenericDB, logger *logger.Logger) {
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

		c.log.Info("Collection discoverer component initialized", "server_info", fmt.Sprintf("%+v", c.serverInfo))
	})
}

// methods
func (c *CollectionsDiscovererImp) IsCollectionLocallyPresent(collectionId string, consistencyClock uint64) (bool, shared.Collection, error) {
	//sync
	c.collectionsPartialView.Mutex.Lock()
	defer c.collectionsPartialView.Mutex.Unlock()

	c.log.Info("IsCollectionLocallyPresent called", "collection_id", collectionId, "consistency_clock", consistencyClock)

	//imp
	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		c.log.Info("Collection is not known by the current node", "collection_id", collectionId)
		return false, nil, nil
	}
	if !collection.IsLocal() {
		c.log.Info("Collection known by the current node, but it's not local", "collection_id", collectionId, "consistency_clock", consistencyClock, "collection", fmt.Sprintf("%+v", collection))
		return false, collection, nil
	}

	if collection.IsStrong() {
		strongCollection := collection.(*shared.StrongCollection)
		if strongCollection.ConsistencyClock < consistencyClock {
			c.log.Warn("Outdated strong collection replica", "collection_id", collectionId, "provided_consistency_clock", consistencyClock, "collection", fmt.Sprintf("%+v", collection), "local_consistency_clock", strongCollection.ConsistencyClock)
			return true, collection, errors.New("outdated data in a strong collection replica")
		}
		collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
		c.log.Debug("LOCK: Locked in READ the sequential collection", "collection_id", collectionId)
	}

	c.log.Debug("Collection is known by the current node and is locally present", "collection_id", collectionId, "consistency_clock", consistencyClock, "collection", fmt.Sprintf("%+v", collection))
	return true, collection, nil
}

func (c *CollectionsDiscovererImp) WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, userId string, recordName string) (string, error) {
	c.log.Info("WeakCollectionRemoteRecordAccess called: Accessing remote record of an eventual collection", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "recordName", recordName, "requesting_from_node", hostId)

	res, err := c.sender.WeakCollectionRemoteRecordAccess(hostId, collectionId, groups, userId, recordName)
	if err != nil {
		c.log.Error("Failed to retrieve record value from remote eventual collection", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "recordName", recordName, "requesting_from_node", hostId)
		return "", err
	}
	if len(res) != 1 {
		c.log.Error("Unexpected quantity of received records from eventual remote collection", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "recordName", recordName, "requesting_from_node", hostId)
		return "", errors.New("unexpected output")
	}

	c.log.Info("WeakCollectionRemoteRecordAccess: record value retrieved successfully", "collection_id", collectionId, "executing_user", userId, "record_name", recordName)
	c.log.Debug("WeakCollectionRemoteRecordAccess: record value retrieved successfully", "record_name", recordName, "record_value", res[0])
	return res[0].(string), nil
}

func (c *CollectionsDiscovererImp) WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, groups []string, userId string, recordNames ...string) ([]any, error) {
	c.log.Info("WeakCollectionRemoteRecordsAccess called: Accessing remote records of an eventual collection", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "record_names", recordNames, "requesting_from_node", hostId)
	res, err := c.sender.WeakCollectionRemoteRecordAccess(hostId, collectionId, groups, userId, recordNames...)
	if err != nil {
		c.log.Error("Failed to retrieve record value from remote eventual collection", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "record_names", recordNames, "requesting_from_node", hostId)
		return nil, err
	}

	c.log.Info("WeakCollectionRemoteRecordsAccess: record values retrieved successfully", "collection_id", collectionId, "executing_user", userId, "record_names", recordNames)
	c.log.Debug("WeakCollectionRemoteRecordsAccess: record values retrieved successfully", "record_names", recordNames, "record_values", res)
	return res, nil
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
	c.log.Info("AccessRecords called", "collection_id", collectionId)
	//imp

	var zero T
	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		if c.serverInfo.Father == nil {
			c.log.Warn("AccessRecords: Collection not known locally, no father to query. Collection DOES NOT exists", "collection_id", collectionId)
			c.collectionsPartialView.Mutex.Unlock() // lock release
			return zero, errors.New("collection not found")
		}
		addr := c.serverInfo.Father.HostName

		c.log.Debug("AccessRecords: delegating record access to father node", "collection_id", collectionId)
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return remoteFn(addr)
	}
	if collection.IsLocal() {
		if !collection.IsStrong() {
			collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RLock()
			defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.RUnlock()
		}

		c.log.Debug("AccessRecords: Accessing local collection records", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return dbFn()
	}
	if collection.IsStrong() {
		c.log.Error("AccessRecords: Attempted remote access to strong collection", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
		c.collectionsPartialView.Mutex.Unlock() // lock release
		return zero, errors.New("strong collection can't be accessed remotely")
	}
	child := c.collectionsPartialView.GetClosestChild(collection.HostsId(), c.serverInfo)
	c.collectionsPartialView.Mutex.Unlock() // lock release
	c.log.Debug("AccessRecords: Accessing remote child for eventual collection", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "child", child)
	return remoteFn(child)
}

// TODO: Complete logging
func UpdateRecord(
	c *CollectionsDiscovererImp,
	collectionId string,
	dbFn func(isStrong bool) error,
	isInitiator bool,
	updateMessage messages.UpdateMessage,
) (map[string]struct{}, error) {
	c.log.Info("UpdateRecord called", "collection_id", collectionId)

	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	var isStrong = false
	if !exists {
		//can't verify if collection is strong or weak, so just notify father
		c.log.Warn("UpdateRecord: Collection not found during update. Trying to forward to father", "collection_id", collectionId)

		hosts := map[string]struct{}{}
		if c.serverInfo.Father == nil {
			c.log.Warn("UpdateRecord: Collection not known locally, no father to query. Collection DOES NOT exists", "collection_id", collectionId)
			return nil, errors.New("collection not found")
		}
		addr := c.serverInfo.Father.HostName
		hosts[addr] = struct{}{}

		c.log.Info("UpdateRecord: Added father to collection hosts", "collection_id", collectionId, "father_hostname", addr)
		return hosts, nil
	}
	if !collection.IsLocal() {
		if collection.IsStrong() && isInitiator { //safety check
			c.log.Error("AccessRecords: Attempted remote update to strong collection", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
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

func (c *CollectionsDiscovererImp) UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdateMessage, groups []string, userId string, isInitiator bool, ignoredHostName string) error {
	c.log.Info("UpdateCollectionsRecordsAndPropagate called. Processing record updates", "record_updates", fmt.Sprintf("%+v", updates), "user_access_groups", groups, "executing_user", userId, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
	var msgs = make(map[string]messages.CollectionUpdateMessages) // host -> message
	//sync
	c.collectionsPartialView.Mutex.Lock()
	//imp

	//write or delete in db and prepare messages for future propagation on other hosts
	for _, update := range updates {
		var dbFn func(isStrong bool) error
		if update.IsDelete {
			dbFn = func(isStrong bool) error {
				authorized, err := c.dbInterface.IsAuthorized(update.CollectionId, userId, groups...)
				if err != nil {
					c.log.Error("Failed to check user authorization access", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
					return err
				}
				if !authorized {
					c.log.Error("User is not authorized to delete collection records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
					return &shared.UnauthorizedError{}
				}

				if isStrong {
					err = c.dbInterface.DeleteRecords(update.CollectionId, "", update.RecordNames...)
					if err != nil {
						c.log.Error("Failed to delete strong collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
						return err
					}
					return nil
				}

				err = c.dbInterface.DeleteRecords(update.CollectionId, strconv.FormatInt(update.Time, 10)+update.UpdateHostIp, update.RecordNames...)
				if err != nil {
					c.log.Error("Failed to delete eventual collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
					return err
				}
				return nil
			}
		} else {
			dbFn = func(isStrong bool) error {
				authorized, err := c.dbInterface.IsAuthorized(update.CollectionId, userId, groups...)
				if err != nil {
					c.log.Error("Failed to check user authorization access", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
					return err
				}
				if !authorized {
					c.log.Error("User is not authorized to update collection records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
					return &shared.UnauthorizedError{}
				}

				if isStrong {
					err = c.dbInterface.SetRecords(update.CollectionId, update.RecordNames, update.RecordValues, "")
					if err != nil {
						c.log.Error("Failed to update strong collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
						return err
					}
					return nil
				}
				err = c.dbInterface.SetRecords(update.CollectionId, update.RecordNames, update.RecordValues, strconv.FormatInt(update.Time, 10)+update.UpdateHostIp)
				if err != nil {
					c.log.Error("Failed to update eventual collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
					return err
				}
				return nil
			}
		}

		//write or delete in db
		hosts, err := UpdateRecord(c, update.CollectionId, dbFn, isInitiator, &update)
		if err != nil {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			c.log.Error("UpdateCollectionsRecordsAndPropagate: update failed", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
			return err
		}
		update.SenderIp = c.serverInfo.HostName

		for host := range hosts { // for each host that has the collection prepare the message
			if host == ignoredHostName {
				c.log.Debug("UpdateCollectionsRecordsAndPropagate: skipping ignored host", "collection_id", update.CollectionId, "ignored_host", ignoredHostName)
				continue
			}
			msgs[host] = messages.CollectionUpdateMessages{Groups: groups, Messages: append(msgs[host].Messages, update)}
		}
	}
	c.collectionsPartialView.Mutex.Unlock() // lock release

	//propagate update to other hosts
	for host, msg := range msgs {
		c.log.Debug("UpdateCollectionsRecordsAndPropagate: propagating update", "forward_to", host, "update", fmt.Sprintf("%+v", msg))
		c.sender.PropagateUpdate(host, msg)
	}

	c.log.Info("UpdateCollectionsRecordsAndPropagate: Local update completed, and propagation started successfully")
	return nil
}

func (c *CollectionsDiscovererImp) UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error {
	c.log.Info("UpdateCollectionsGroupsAndPropagate called. Processing groups updates", "groups_updates", fmt.Sprintf("%+v", updates), "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
	var msgs = make(map[string]messages.CollectionGroupUpdateMessages) // host -> message
	//sync
	c.collectionsPartialView.Mutex.Lock()

	for _, update := range updates {
		dbFn := func(isStrong bool) error {
			err := c.dbInterface.SetGroups(update.CollectionId, update.Sequence, update.GroupNames...)
			if err != nil {
				c.log.Error("Failed to set collection's groups", "collection_id", update.CollectionId, "update", fmt.Sprintf("%+v", update), "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
				return err
			}
			return nil
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
			c.log.Error("UpdateCollectionsGroupsAndPropagate: groups update failed", "collection_id", update.CollectionId, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
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
		c.log.Debug("UpdateCollectionsRecordsAndPropagate: propagating groups update", "forward_to", host, "update", fmt.Sprintf("%+v", msg))
		c.sender.PropagateGroupUpdate(host, msg)
	}

	c.log.Info("UpdateCollectionsGroupsAndPropagate: Local update completed, and propagation started successfully")
	return nil
}

func (c *CollectionsDiscovererImp) SetCollectionAndPropagate(collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, migrationManager migrationManager.MigrationManager, isStrong bool, skipUniquenessCheck bool) error {
	c.log.Info("SetCollectionAndPropagate called. Processing collection creation", "collection_id", collectionId, "collection_name", collectionName, "records", fmt.Sprintf("%+v", records), "executing_user", userId, "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck)
	//sync
	c.collectionsPartialView.Mutex.Lock()
	defer c.collectionsPartialView.Mutex.Unlock()
	//imp

	//if collection  already exists return error
	_, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if exists {
		c.log.Error("Duplicate collection ID detected. A collection with the same UUID already exists", "collection_id", collectionId, "collection_name", collectionName, "records", fmt.Sprintf("%+v", records), "executing_user", userId, "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck)
		return errors.New("collection already exists")
	}

	//propagate update to father
	if c.serverInfo.Father != nil {
		c.log.Debug("Propagating collection creation request to father", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId)
		newCollectionMessage := messages.NewCollectionMessage{
			CollectionId:        collectionId,
			CollectionName:      collectionName,
			CollectionOwner:     userId,
			IsStrong:            isStrong,
			SenderIp:            c.serverInfo.HostName,
			SkipUniquenessCheck: skipUniquenessCheck,
		}
		err := c.sender.NewCollectionCreated(c.serverInfo.Father.HostName, newCollectionMessage)
		if err != nil {
			c.log.Error("Failed to propagate new collection creation", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId, "records", fmt.Sprintf("%+v", records), "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck, "error", err)
			return err
		}
	} else {
		if !skipUniquenessCheck {
			ok, err := c.dbInterface.IsUniqueForOwner(collectionName, userId)
			if err != nil {
				c.log.Error("Failed to check whether the collection was unique or not", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId, "records", fmt.Sprintf("%+v", records), "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck, "error", err)
				return err
			}
			if !ok {
				c.log.Warn("Collection name is not unique. Aborting collection creation", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId)
				return errors.New("tried to create collection that already exists")
			}
		}
	}

	//try to add collection in redis
	dbErr := c.dbInterface.SetCollection(collectionId, records, groups, userId, 0)
	if dbErr != nil {
		c.log.Error("Failed to persist collection in the DB", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId, "records", fmt.Sprintf("%+v", records), "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck, "error", dbErr)
		return dbErr
	}

	//get size of collection in db
	size, sizeErr := c.dbInterface.GetCollectionMemoryUsage(collectionId)
	if sizeErr != nil {
		c.log.Error("Failed to retrieve the space occupied in the DB by the collection just persisted", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId, "records", fmt.Sprintf("%+v", records), "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck, "error", sizeErr)
		return sizeErr
	}

	//add collection in local view
	migrationManagerErr := migrationManager.NewCollectionCreated(collectionId, size, isStrong)
	if migrationManagerErr != nil {
		c.log.Error("Failed to register newly created collection in Migration Manager view", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId, "records", fmt.Sprintf("%+v", records), "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck, "error", migrationManagerErr)
		return migrationManagerErr
	}

	c.log.Info("Collection created successfully", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId)
	return nil
}

func (c *CollectionsDiscovererImp) RemoveCollectionAndPropagate(collectionId string, migrationManager migrationManager.MigrationManager, groups []string, userId string, isInitiator bool, ignoredHostName string) error {
	c.log.Info("RemoveCollectionAndPropagate called. Processing collection deletion", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
	//sync
	c.collectionsPartialView.Mutex.Lock()
	//imp

	collection, exists := c.collectionsPartialView.KnownCollections[collectionId]
	if !exists {
		if c.serverInfo.Father != nil && c.serverInfo.Father.HostName != ignoredHostName {
			c.log.Debug("Propagating collection removal to father", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
			c.sender.RemoveCollection(c.serverInfo.Father.HostName, collectionId, c.serverInfo.HostName, groups, userId) //propagate to father
			return nil
		}

		c.log.Warn("Cloud reached without finding the collection. Collection does not exists", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
		return errors.New("collection doesn't exists")
	}

	if !collection.IsLocal() {
		if collection.IsStrong() {
			c.collectionsPartialView.Mutex.Unlock() // lock release
			c.log.Error("Cannot delete strong collection remotely", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
			return errors.New("strong collection can't be removed remotely")
		}

		c.collectionsPartialView.Mutex.Unlock() // lock release
		c.log.Debug("Collection is known locally, but it is not stored locally. Propagating the deletion towards the direction where other replicas are stored", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups)
		c.propagateRemoveCollection(collection, collectionId, groups, userId, ignoredHostName) //propagate to other hosts
		return nil
	}

	authorized, err := c.dbInterface.IsAuthorized(collectionId, userId, groups...)
	if err != nil {
		c.log.Error("Failed to check user authorization access", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
		return err
	}
	if !authorized {
		c.log.Error("User is not authorized to delete collection", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
		return &shared.UnauthorizedError{}
	}

	delete(c.collectionsPartialView.KnownCollections, collectionId) //remove from local view
	err = c.dbInterface.DeleteCollection(collectionId)              //remove from local db
	if err != nil {
		c.log.Error("Failed to delete the local collection from DB", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
	}
	migrationManager.RemoveCollectionFromLru(collectionId) //remove from lru

	c.collectionsPartialView.Mutex.Unlock() // lock release
	c.log.Debug("Collection has been removed locally. Propagating the deletion towards the direction where other replicas are stored", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups)
	c.propagateRemoveCollection(collection, collectionId, groups, userId, ignoredHostName) //propagate to other hosts

	c.log.Info("Collection removed successfully", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups)
	return nil
}

func (c *CollectionsDiscovererImp) propagateRemoveCollection(collection shared.Collection, collectionId string, groups []string, userId string, ignoredHostName string) {
	hosts := collection.HostsId()
	for host := range hosts {
		if host == ignoredHostName {
			continue
		}
		c.log.Debug("Propagating collection removal towards the direction of a replica", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "collection", fmt.Sprintf("%+v", collection), "forward_to", host)
		c.sender.RemoveCollection(host, collectionId, c.serverInfo.HostName, groups, userId)
	}

	if c.serverInfo.Father != nil && c.serverInfo.Father.HostName != ignoredHostName {
		c.log.Debug("Propagating collection removal to father node", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "collection", fmt.Sprintf("%+v", collection), "forward_to", c.serverInfo.Father.HostName)
		c.sender.RemoveCollection(c.serverInfo.Father.HostName, collectionId, c.serverInfo.HostName, groups, userId)
	}
}
