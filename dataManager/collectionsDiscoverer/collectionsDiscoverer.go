package collectionsDiscoverer

import (
	"dataManager/lru"
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
	Init(*shared.CollectionPartialView, server.ServerInfo, server.ServerType, db.GenericDB, logger.Logger, *lru.LRUCache[shared.Collection], migrationManager.MigrationManager, *sync.Mutex)

	AccessRecords(collectionId string, dbFn func() (any, error), remoteFn func(addr string) (any, error)) (any, error)

	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, userId string, recordNames string) (string, error)
	WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, groups []string, userId string, recordNames ...string) ([]any, error)

	UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdateMessage, groups []string, userId string, isInitiator bool, ignoredHostName string) error
	UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error
	SetCollectionAndPropagate(collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, isStrong, skipUniquenessCheck bool) error
	RemoveCollectionAndPropagate(collectionId string, groups []string, userId string, isInitiator bool, ignoredHostName string) error

	GetCollectionVectorClockValue(collectionId string) (uint64, error)
}

type CollectionsDiscovererImp struct {
	//data
	collectionsPartialView *shared.CollectionPartialView
	serverInfo             server.ServerInfo
	sender                 sender.CollectionDiscovererSender
	log                    logger.Logger
	dbInterface            db.GenericDB
	lruCache               *lru.LRUCache[shared.Collection]
	migrationManager       migrationManager.MigrationManager
	mutex                  *sync.Mutex
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

func (c *CollectionsDiscovererImp) Init(collections *shared.CollectionPartialView, serverInfo server.ServerInfo, server server.ServerType, dbInterface db.GenericDB, logger logger.Logger, lruCache *lru.LRUCache[shared.Collection], migrationManager migrationManager.MigrationManager, mutex *sync.Mutex) {
	initOnce.Do(func() {
		c.collectionsPartialView = collections
		c.serverInfo = serverInfo
		c.log = logger
		c.sender = sender.NewCollectionDiscovererSender(logger, server)
		c.dbInterface = dbInterface
		c.lruCache = lruCache
		c.migrationManager = migrationManager
		c.mutex = mutex

		server.AddCustomRoute(c.sender.RemoteAccessPath(), true, c.handleIncomingRemoteAccess().ServeHTTP)
		server.AddCustomRoute(c.sender.UpdatePropagationPath(), true, c.handleIncomingUpdatePropagation().ServeHTTP)
		server.AddCustomRoute(c.sender.UpdateGroupPropagationPath(), true, c.handleIncomingUpdateGroupPropagation().ServeHTTP)
		server.AddCustomRoute(c.sender.CreateCollectionPropagationPath(), true, c.handleIncomingNewCollection().ServeHTTP)
		server.AddCustomRoute(c.sender.RemoveCollectionPropagationPath(), true, c.handleIncomingRemoveCollection().ServeHTTP)

		c.log.Info("Collection discoverer component initialized", "server_info", fmt.Sprintf("%+v", c.serverInfo))
	})
}

func (c *CollectionsDiscovererImp) getSender() sender.CollectionDiscovererSender {
	return c.sender
}

// methods
func (c *CollectionsDiscovererImp) IsCollectionLocallyPresent(collectionId string, consistencyClock uint64) (bool, shared.Collection, error) {
	//sync
	//c.collectionsPartialView.Mutex.Lock()
	//defer c.collectionsPartialView.Mutex.Unlock()

	c.log.Info("IsCollectionLocallyPresent called", "collection_id", collectionId, "consistency_clock", consistencyClock)

	//imp
	collection, exists := c.collectionsPartialView.GetCollection(collectionId)
	if !exists {
		c.log.Info("Collection is not known by the current node", "collection_id", collectionId)
		return false, nil, nil
	}
	if localState, ok := collection.GetCollectionState().(*shared.LocalCollection); !ok {
		c.log.Info("Collection known by the current node, but it's not local", "collection_id", collectionId, "consistency_clock", consistencyClock, "collection", fmt.Sprintf("%+v", collection))
		return false, collection, nil
	} else if collection.IsStrong() {
		strongCollection := collection.(*shared.StrongCollection)
		if strongCollection.ConsistencyClock < consistencyClock {
			c.log.Warn("Outdated strong collection replica", "collection_id", collectionId, "provided_consistency_clock", consistencyClock, "collection", fmt.Sprintf("%+v", collection), "local_consistency_clock", strongCollection.ConsistencyClock)
			return true, collection, errors.New("outdated data in a strong collection replica")
		} else if strongCollection.HasPostponedDecisionBehaviour() {
			return true, collection, &ToMigrateError{Code: 423, Message: "Strong collection has postponed migration behaviour to execute, can't be used until migration is completed"}
		}
		localState.CollectionLock.RLock()
		if !collection.IsLocal() {
			localState.CollectionLock.RUnlock()
			c.log.Info("Collection became non-local while acquiring the lock", "collection_id", collectionId)
			return false, collection, nil
		}
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

func (c *CollectionsDiscovererImp) AccessRecords(
	collectionId string,
	dbFn func() (any, error),
	remoteFn func(addr string) (any, error),
) (any, error) {
	//sync
	//c.collectionsPartialView.Mutex.Lock()
	c.log.Info("AccessRecords called", "collection_id", collectionId)
	//imp

	var zero any
	collection, exists := c.collectionsPartialView.GetCollection(collectionId)
	if !exists || (!collection.IsLocal() && len(collection.HostsId()) == 0) {
		if c.serverInfo.Father == nil {
			c.log.Warn("AccessRecords: Collection not known locally, no father to query. Collection DOES NOT exists", "collection_id", collectionId)
			//c.collectionsPartialView.Mutex.Unlock() // lock release
			return zero, errors.New("collection not found")
		}
		addr := c.serverInfo.Father.HostName

		c.log.Debug("AccessRecords: delegating record access to father node", "collection_id", collectionId)
		//c.collectionsPartialView.Mutex.Unlock() // lock release
		return remoteFn(addr)
	}
	if localState, ok := collection.GetCollectionState().(*shared.LocalCollection); ok {
		if !collection.IsStrong() {
			localState.CollectionLock.RLock()

			if !collection.IsLocal() {
				localState.CollectionLock.RUnlock()
				c.log.Debug("AccessRecords: Collection became non-local while acquiring the lock", "collection_id", collectionId)
				addr := c.serverInfo.GetClosestChild(collection.HostsId())
				if addr == "" {
					if c.serverInfo.Father == nil {
						c.log.Error("AccessRecords: Collection not known locally, no father to query. Collection DOES NOT exists", "collection_id", collectionId)
						return zero, errors.New("collection not found")
					}
					addr = c.serverInfo.Father.HostName
				}
				//c.collectionsPartialView.Mutex.Unlock() // lock release
				c.log.Debug("AccessRecords: Accessing remote child for eventual collection", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "addr", addr)
				return remoteFn(addr)
			} else {
				c.log.Debug("AccessRecords: Accessing local collection records", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
				//c.collectionsPartialView.Mutex.Unlock() // lock release
				defer localState.CollectionLock.RUnlock()
				return dbFn()
			}
		}

		c.log.Debug("AccessRecords: Accessing local collection records", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
		//c.collectionsPartialView.Mutex.Unlock() // lock release
		return dbFn()
	}
	if collection.IsStrong() {
		c.log.Error("AccessRecords: Attempted remote access to strong collection", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
		//c.collectionsPartialView.Mutex.Unlock() // lock release
		return zero, errors.New("strong collection can't be accessed remotely")
	}
	addr := c.serverInfo.GetClosestChild(collection.HostsId())
	if addr == "" {
		if c.serverInfo.Father == nil {
			c.log.Error("AccessRecords: Collection not known locally, no father to query. Collection DOES NOT exists", "collection_id", collectionId)
			return zero, errors.New("collection not found")
		}
		addr = c.serverInfo.Father.HostName
	}
	//c.collectionsPartialView.Mutex.Unlock() // lock release
	c.log.Debug("AccessRecords: Accessing remote child for eventual collection", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "addr", addr)
	return remoteFn(addr)
}

func (c *CollectionsDiscovererImp) updateRecord(
	collectionId string,
	collection shared.Collection,
	dbFn func(isStrong bool) (int64, error),
	isInitiator bool,
	updateMessage messages.UpdateMessage,
) (map[string]struct{}, error, bool) { //host, error, is collection locally stored
	c.log.Info("UpdateRecord called", "collection_id", collectionId)

	if collection == nil {
		//can't verify if collection is strong or weak, so just notify father
		c.log.Warn("UpdateRecord: Collection not found during update. Trying to forward to father", "collection_id", collectionId)

		hosts := map[string]struct{}{}
		if c.serverInfo.Father == nil {
			c.log.Warn("UpdateRecord: Collection not known in cloud, no father to query. Collection DOES NOT exists", "collection_id", collectionId)
			return nil, errors.New("collection not found"), false
		}
		addr := c.serverInfo.Father.HostName
		hosts[addr] = struct{}{}

		c.log.Info("UpdateRecord: Added father to collection hosts", "collection_id", collectionId, "father_hostname", addr)
		return hosts, nil, false
	}
	localState, ok := collection.GetCollectionState().(*shared.LocalCollection)
	if !ok {
		c.log.Debug("Collection to be updated is not local", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
		if collection.IsStrong() && isInitiator { //safety check
			c.log.Error("UpdateRecord: Attempted remote update to strong collection", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
			return nil, errors.New("strong collection can't be updated remotely"), false
		}

		c.log.Debug("Computing the list of hosts to which propagate the update", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
		hosts := collection.HostsId()
		if c.serverInfo.Father == nil {
			c.log.Debug("Current node is a cloud node. Avoiding to propagate the update to the father", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "hosts", hosts)
			return hosts, nil, false
		}

		addr := c.serverInfo.Father.HostName
		hosts[addr] = struct{}{}

		c.log.Debug("List of hosts to which propagate the update computed successfully", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "hosts", fmt.Sprintf("%+v", hosts))
		return hosts, nil, false
	}

	c.log.Debug("Preparing list of host to which propagate the update", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
	//always notify to father
	hosts := collection.HostsId()

	if c.serverInfo.Father != nil {
		addr := c.serverInfo.Father.HostName
		hosts[addr] = struct{}{}
	}

	if !isInitiator || !collection.IsStrong() {
		localState.CollectionLock.RLock()
		c.log.Debug("LOCK: collection locked in READ", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
		defer func() {
			localState.CollectionLock.RUnlock()
			if collection.IsStrong() {
				go c.migrationManager.HandlePostponedDecisions(collection)
			}
		}()

		if !collection.IsLocal() {
			c.log.Debug("UpdateRecord: Collection became non-local while acquiring the lock", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
			return hosts, nil, false
		}
	}

	if collection.IsStrong() {
		c.log.Debug("Collection to be updated is local and sequential consistent", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
		if isInitiator && !collection.(*shared.StrongCollection).IsLeader { //safety check
			c.log.Error("Trying to update local sequential collection even if it's not the leader", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "is_initiator", isInitiator, "message", fmt.Sprintf("%+v", updateMessage))
			return nil, errors.New("only leader can update strong collection"), true
		}

		_, isCollectionUpdate := updateMessage.(*messages.CollectionUpdateMessage)
		if isInitiator && isCollectionUpdate {
			oldClock := collection.(*shared.StrongCollection).ConsistencyClock
			collection.(*shared.StrongCollection).ConsistencyClock++
			updateMessage.SetSequence(collection.(*shared.StrongCollection).ConsistencyClock)
			c.log.Debug("Local collection sequence incremented", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "old_sequence", oldClock, "new_sequence", collection.(*shared.StrongCollection).ConsistencyClock)
		} else if !isInitiator && isCollectionUpdate && collection.(*shared.StrongCollection).ConsistencyClock > updateMessage.GetSequence() {
			c.log.Warn("Received an outdated update for the local sequential collection", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "received_sequence", updateMessage.GetSequence(), "current_sequence", collection.(*shared.StrongCollection).ConsistencyClock, "message", fmt.Sprintf("%+v", updateMessage), "is_initiator", isInitiator)
			return nil, nil, true //ignore outdated update
		}
	}

	memUsage, err := dbFn(collection.IsStrong()) // update in local db
	if err != nil {
		var staleErr *db.StaleUpdateError
		if errors.As(err, &staleErr) {
			c.log.Warn("UpdateRecord: Stale update detected during collection records update in DB", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "error", err)
		} else {
			c.log.Error("UpdateRecord: Failed to update collection records in DB", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "error", err)
		}
		return nil, err, true
	}

	if collection.IsStrong() {
		collection.(*shared.StrongCollection).ConsistencyClock = updateMessage.GetSequence()
	}

	collection.GetCollectionState().(*shared.LocalCollection).IncSize(memUsage)
	c.lruCache.UpdateSize(collection.CollectionId(), uint(collection.GetCollectionState().(*shared.LocalCollection).Size()))
	c.log.Info("UpdateRecord: Collection records updated successfully in local DB", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "memory_usage_change", memUsage, "new_memory_usage", collection.GetCollectionState().(*shared.LocalCollection).Size())

	c.log.Debug("List of hosts prepared successfully", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "hosts", fmt.Sprintf("%+v", hosts))
	c.log.Debug("Proceeding to update the collection in the local DB", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
	return hosts, nil, true
}

func (c *CollectionsDiscovererImp) popolateUpdateSafetyCheck(saftyQueue shared.FunctionQueue, dbFn func(isStrong bool) (int64, error), update messages.UpdateMessage, hosts map[string]struct{}, collectionId string) {
	saftyQueue.(*shared.FunctionQueueWithCounter).SetCounter(uint32(len(hosts)))
	saftyQueue.AddFunction(func() {
		//c.collectionsPartialView.Mutex.Lock()
		//defer c.collectionsPartialView.Mutex.Unlock()

		collection, exists := c.collectionsPartialView.GetCollection(collectionId)
		//if I receive the collection during the update
		if exists && collection.IsLocal() {
			localState, ok := collection.GetCollectionState().(*shared.LocalCollection)
			if !ok {
				return
			}
			c.log.Debug("UpdateRecord: Executing postponed update for collection", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
			//if update older than the collection strong
			var isStrong = false
			if collection.IsStrong() {
				isStrong = true
				if update.GetSequence() <= collection.(*shared.StrongCollection).ConsistencyClock {
					return
				}
			}
			if !collection.IsStrong() {
				localState.CollectionLock.RLock()
				defer localState.CollectionLock.RUnlock()

				if !collection.IsLocal() {
					c.log.Debug("UpdateRecord: Collection became non-local while acquiring the lock", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection))
					return
				}
				collection.(*shared.WeakCollection).WriteMutex.Lock()
				defer collection.(*shared.WeakCollection).WriteMutex.Unlock()
			}
			memUsage, err := dbFn(isStrong) // update in local db
			if err != nil {
				var staleErr *db.StaleUpdateError
				if errors.As(err, &staleErr) {
					c.log.Warn("UpdateRecord: Stale update detected during collection records update in DB", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "error", err)
				} else {
					c.log.Error("UpdateRecord: Failed to update collection records in DB", "collection_id", collectionId, "collection", fmt.Sprintf("%+v", collection), "error", err)
				}
				return
			}

			if isStrong {
				collection.(*shared.StrongCollection).ConsistencyClock = update.GetSequence()
			}
			collection.GetCollectionState().(*shared.LocalCollection).IncSize(memUsage)
			c.lruCache.UpdateSize(collection.CollectionId(), uint(collection.GetCollectionState().(*shared.LocalCollection).Size()))
		}
	})
}

func (c *CollectionsDiscovererImp) UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdateMessage, groups []string, userId string, isInitiator bool, ignoredHostName string) error {
	c.log.Info("UpdateCollectionsRecordsAndPropagate called. Processing record updates", "record_updates", fmt.Sprintf("%+v", updates), "user_access_groups", groups, "executing_user", userId, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
	var msgs = make(map[string]messages.CollectionUpdateMessages) // host -> message
	var safetymap = make(map[string][]shared.FunctionQueue)       // host -> safetyQueue
	//sync
	//c.collectionsPartialView.Mutex.Lock()
	//c.mutex.Lock()
	//imp

	//write or delete in db and prepare messages for future propagation on other hosts
	for _, update := range updates {
		var dbFn func(isStrong bool) (int64, error)
		if update.IsDelete {
			dbFn = func(isStrong bool) (int64, error) {
				authorized, err := c.dbInterface.IsAuthorized(update.CollectionId, userId, groups...)
				if err != nil {
					c.log.Error("Failed to check user authorization access", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
					return 0, err
				}
				if !authorized {
					c.log.Error("User is not authorized to delete collection records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
					return 0, &shared.UnauthorizedError{}
				}

				memUsage, err := c.dbInterface.GetRecordsMemoryUsage(update.CollectionId, update.RecordNames...)
				if err != nil {
					c.log.Error("Failed to get records memory usage after update", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
					return 0, err
				}

				if isStrong {
					err = c.dbInterface.DeleteRecords(update.CollectionId, "", update.RecordNames...)
					if err != nil {
						c.log.Error("Failed to delete strong collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
						return 0, err
					}
				} else {
					err = c.dbInterface.DeleteRecords(update.CollectionId, strconv.FormatInt(update.Time, 10)+update.UpdateHostIp, update.RecordNames...)
					if err != nil {
						c.log.Error("Failed to delete eventual collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
						return 0, err
					}
				}

				return -int64(memUsage), nil
			}
		} else {
			dbFn = func(isStrong bool) (int64, error) {
				authorized, err := c.dbInterface.IsAuthorized(update.CollectionId, userId, groups...)
				if err != nil {
					c.log.Error("Failed to check user authorization access", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
					return 0, err
				}
				if !authorized {
					c.log.Error("User is not authorized to update collection records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
					return 0, &shared.UnauthorizedError{}
				}

				memUsage, err := c.dbInterface.GetRecordsMemoryUsage(update.CollectionId, update.RecordNames...)
				if err != nil {
					c.log.Error("Failed to get records memory usage after update", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
					return 0, err
				}

				if isStrong {
					err = c.dbInterface.SetRecords(update.CollectionId, update.RecordNames, update.RecordValues, "")
					if err != nil {
						var staleErr *db.StaleUpdateError
						if errors.As(err, &staleErr) {
							c.log.Warn("Failed to update strong collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
						} else {
							c.log.Error("Failed to update strong collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
						}
						return 0, err
					}
				} else {
					err = c.dbInterface.SetRecords(update.CollectionId, update.RecordNames, update.RecordValues, strconv.FormatInt(update.Time, 10)+update.UpdateHostIp)
					if err != nil {
						var staleErr *db.StaleUpdateError
						if errors.As(err, &staleErr) {
							c.log.Warn("Failed to update eventual collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
						} else {
							c.log.Error("Failed to update eventual collection's records", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
						}
						return 0, err
					}
				}

				newMemUsage, err := c.dbInterface.GetRecordsMemoryUsage(update.CollectionId, update.RecordNames...)
				if err != nil {
					c.log.Error("Failed to get records memory usage after update", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
					return 0, err
				}

				return int64(newMemUsage - memUsage), nil
			}
		}

		//write or delete in db
		collection, exists := c.collectionsPartialView.GetCollection(update.CollectionId)
		if exists && !collection.IsStrong() {
			collection.(*shared.WeakCollection).WriteMutex.Lock()
		}

		hosts, err, wasCollectionLocal := c.updateRecord(update.CollectionId, collection, dbFn, isInitiator, &update)
		if err != nil {
			var staleErr *db.StaleUpdateError
			if errors.As(err, &staleErr) {
				c.log.Warn("UpdateCollectionsRecordsAndPropagate: stale update detected", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
			} else {
				c.log.Error("UpdateCollectionsRecordsAndPropagate: update failed", "collection_id", update.CollectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
			}
			if exists && !collection.IsStrong() {
				collection.(*shared.WeakCollection).WriteMutex.Unlock()
			}
			continue
		}
		update.SenderIp = c.serverInfo.HostName

		//prepare safty check
		var saftyQueue shared.FunctionQueue
		if !wasCollectionLocal {
			saftyQueue = shared.NewFunctionQueueWithCounter()
			c.popolateUpdateSafetyCheck(saftyQueue, dbFn, &update, hosts, update.CollectionId)
		}

		// for each host that has the collection prepare the message
		for host := range hosts {
			if exists && collection.GetCollectionState().IsCollectionBeingMigrated() {
				copyMsg := update.Copy()
				if host == ignoredHostName {
					copyMsg.ToForward = false
				}
				msg := messages.CollectionUpdateMessages{Groups: groups, UserId: userId, Messages: []messages.CollectionUpdateMessage{copyMsg}}
				c.log.Debug("UpdateCollectionsRecordsAndPropagate: postponing update propagation", "collection_id", update.CollectionId, "to_host", host, "update", fmt.Sprintf("%+v", msg))
				collection.GetCollectionState().AddPostponedUpdate(host,
					func() {
						c.log.Debug("UpdateCollectionsRecordsAndPropagate: executing postponed update propagation", "collection_id", update.CollectionId, "to_host", host, "update", fmt.Sprintf("%+v", msg))
						c.sender.PropagateUpdate(host, msg, make([]shared.FunctionQueue, 0))
					},
					func() {
						//TODO: refactor
						collection, exists := c.collectionsPartialView.GetCollection(update.CollectionId)
						//if I receive the collection during the update
						if exists && collection.IsLocal() {
							localState, ok := collection.GetCollectionState().(*shared.LocalCollection)
							if !ok {
								return
							}

							c.log.Debug("UpdateRecord: Executing postponed update for collection", "collection_id", update.CollectionId, "collection", fmt.Sprintf("%+v", collection))
							//if update older than the collection strong
							var isStrong = false
							if collection.IsStrong() {
								isStrong = true
								if update.GetSequence() <= collection.(*shared.StrongCollection).ConsistencyClock {
									return
								}
							}
							if !collection.IsStrong() {
								localState.CollectionLock.RLock()
								defer localState.CollectionLock.RUnlock()
								if !collection.IsLocal() {
									c.log.Debug("UpdateRecord: Collection became non-local while acquiring the lock", "collection_id", update.CollectionId, "collection", fmt.Sprintf("%+v", collection))
									return
								}
								collection.(*shared.WeakCollection).WriteMutex.Lock()
								defer collection.(*shared.WeakCollection).WriteMutex.Unlock()
							}
							memUsage, err := dbFn(isStrong) // update in local db
							if err != nil {
								var staleErr *db.StaleUpdateError
								if errors.As(err, &staleErr) {
									c.log.Warn("UpdateRecord: Stale update detected during collection records update in DB", "collection_id", update.CollectionId, "collection", fmt.Sprintf("%+v", collection), "error", err)
								} else {
									c.log.Error("UpdateRecord: Failed to update collection records in DB", "collection_id", update.CollectionId, "collection", fmt.Sprintf("%+v", collection), "error", err)
								}
								return
							}

							if isStrong {
								collection.(*shared.StrongCollection).ConsistencyClock = update.GetSequence()
							}
							collection.GetCollectionState().(*shared.LocalCollection).IncSize(memUsage)
							c.lruCache.UpdateSize(collection.CollectionId(), uint(collection.GetCollectionState().(*shared.LocalCollection).Size()))
						}
					})
				//continue
			}

			if host == ignoredHostName {
				c.log.Debug("UpdateCollectionsRecordsAndPropagate: skipping ignored host", "collection_id", update.CollectionId, "ignored_host", ignoredHostName)
				continue
			}
			if update.ToForward {
				msgs[host] = messages.CollectionUpdateMessages{Groups: groups, UserId: userId, Messages: append(msgs[host].Messages, update)}
			}
			if saftyQueue != nil && saftyQueue.HasFunctions() {
				safetymap[host] = append(safetymap[host], saftyQueue)
			}
		}
		if exists && !collection.IsStrong() {
			collection.(*shared.WeakCollection).WriteMutex.Unlock()
		}
	}
	//c.collectionsPartialView.Mutex.Unlock() // lock release
	//c.mutex.Unlock()

	//propagate update to other hosts
	for host, msg := range msgs {
		c.log.Debug("UpdateCollectionsRecordsAndPropagate: propagating update", "forward_to", host, "update", fmt.Sprintf("%+v", msg))
		c.sender.PropagateUpdate(host, msg, safetymap[host])
	}

	c.log.Info("UpdateCollectionsRecordsAndPropagate: Local update completed, and propagation started successfully")
	return nil
}

func (c *CollectionsDiscovererImp) UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error {
	c.log.Info("UpdateCollectionsGroupsAndPropagate called. Processing groups updates", "groups_updates", fmt.Sprintf("%+v", updates), "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
	var msgs = make(map[string]messages.CollectionGroupUpdateMessages) // host -> message
	var safetymap = make(map[string][]shared.FunctionQueue)            // host -> safetyQueue
	//sync
	//c.collectionsPartialView.Mutex.Lock()
	//c.mutex.Lock()

	for _, update := range updates {
		dbFn := func(isStrong bool) (int64, error) {
			err := c.dbInterface.SetGroups(update.CollectionId, update.Sequence, update.GroupNames...)
			if err != nil {
				c.log.Error("Failed to set collection's groups", "collection_id", update.CollectionId, "update", fmt.Sprintf("%+v", update), "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
				return 0, err
			}
			//TODO: compute memory usage difference
			return 0, nil
		}

		//write or delete in db
		collection, exists := c.collectionsPartialView.GetCollection(update.CollectionId)
		if exists && !collection.IsStrong() {
			collection.(*shared.WeakCollection).WriteMutex.Lock()
		}

		hosts, err, wasCollectionLocal := c.updateRecord(update.CollectionId, collection, dbFn, isInitiator, &update)
		if err != nil {
			//c.collectionsPartialView.Mutex.Unlock() // lock release
			//c.mutex.Unlock()
			c.log.Error("UpdateCollectionsGroupsAndPropagate: groups update failed", "collection_id", update.CollectionId, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName, "error", err)
			if exists && !collection.IsStrong() {
				collection.(*shared.WeakCollection).WriteMutex.Unlock()
			}
			return err
		}
		update.SenderIp = c.serverInfo.HostName

		//prepare safty check
		var saftyQueue shared.FunctionQueue
		if !wasCollectionLocal {
			saftyQueue = shared.NewFunctionQueueWithCounter()
			c.popolateUpdateSafetyCheck(saftyQueue, dbFn, &update, hosts, update.CollectionId)
		}

		// for each host that has the collection prepare the message
		for host := range hosts {
			if exists && collection.GetCollectionState().IsCollectionBeingMigrated() {
				copyMsg := update.Copy()
				if host == ignoredHostName {
					copyMsg.ToForward = false
				}
				msg := make(messages.CollectionGroupUpdateMessages, 0)
				msg = append(msg, copyMsg)
				c.log.Debug("UpdateCollectionsGroupsAndPropagate: postponing group update propagation", "collection_id", update.CollectionId, "to_host", host, "update", fmt.Sprintf("%+v", msg))
				collection.GetCollectionState().AddPostponedUpdate(host,
					func() {
						c.log.Debug("UpdateCollectionsGroupsAndPropagate: executing postponed group update propagation", "collection_id", update.CollectionId, "to_host", host, "update", fmt.Sprintf("%+v", msg))
						c.sender.PropagateGroupUpdate(host, msg, make([]shared.FunctionQueue, 0))
					},
					func() {
						//TODO: refactor
						collection, exists := c.collectionsPartialView.GetCollection(update.CollectionId)
						//if I receive the collection during the update
						if exists && collection.IsLocal() {
							localState, ok := collection.GetCollectionState().(*shared.LocalCollection)
							if !ok {
								return
							}

							c.log.Debug("UpdateRecord: Executing postponed update for collection", "collection_id", update.CollectionId, "collection", fmt.Sprintf("%+v", collection))
							//if update older than the collection strong
							var isStrong = false
							if collection.IsStrong() {
								isStrong = true
								if update.GetSequence() <= collection.(*shared.StrongCollection).ConsistencyClock {
									return
								}
							}
							if !collection.IsStrong() {
								localState.CollectionLock.RLock()
								defer localState.CollectionLock.RUnlock()
								if !collection.IsLocal() {
									c.log.Debug("UpdateRecord: Collection became non-local while acquiring the lock", "collection_id", update.CollectionId, "collection", fmt.Sprintf("%+v", collection))
									return
								}
								collection.(*shared.WeakCollection).WriteMutex.Lock()
								defer collection.(*shared.WeakCollection).WriteMutex.Unlock()
							}
							memUsage, err := dbFn(isStrong) // update in local db
							if err != nil {
								var staleErr *db.StaleUpdateError
								if errors.As(err, &staleErr) {
									c.log.Warn("UpdateRecord: Stale update detected during collection records update in DB", "collection_id", update.CollectionId, "collection", fmt.Sprintf("%+v", collection), "error", err)
								} else {
									c.log.Error("UpdateRecord: Failed to update collection records in DB", "collection_id", update.CollectionId, "collection", fmt.Sprintf("%+v", collection), "error", err)
								}
								return
							}

							if isStrong {
								collection.(*shared.StrongCollection).ConsistencyClock = update.GetSequence()
							}
							collection.GetCollectionState().(*shared.LocalCollection).IncSize(memUsage)
							c.lruCache.UpdateSize(collection.CollectionId(), uint(collection.GetCollectionState().(*shared.LocalCollection).Size()))
						}
					})
				//continue
			}
			if host == ignoredHostName {
				continue
			}
			if update.ToForward {
				if _, ok := msgs[host]; !ok {
					msgs[host] = make(messages.CollectionGroupUpdateMessages, 0)
				}
				msgs[host] = append(msgs[host], update)
			}
			if saftyQueue != nil && saftyQueue.HasFunctions() {
				safetymap[host] = append(safetymap[host], saftyQueue)
			}
		}
		if exists && !collection.IsStrong() {
			collection.(*shared.WeakCollection).WriteMutex.Unlock()
		}
	}
	//c.collectionsPartialView.Mutex.Unlock() // lock release
	//c.mutex.Unlock()

	//propagate update to other hosts
	for host, msg := range msgs {
		c.log.Debug("UpdateCollectionsRecordsAndPropagate: propagating groups update", "forward_to", host, "update", fmt.Sprintf("%+v", msg))
		c.sender.PropagateGroupUpdate(host, msg, safetymap[host])
	}

	c.log.Info("UpdateCollectionsGroupsAndPropagate: Local update completed, and propagation started successfully")
	return nil
}

func (c *CollectionsDiscovererImp) SetCollectionAndPropagate(collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, isStrong bool, skipUniquenessCheck bool) error {
	c.log.Info("SetCollectionAndPropagate called. Processing collection creation", "collection_id", collectionId, "collection_name", collectionName, "records", fmt.Sprintf("%+v", records), "executing_user", userId, "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck)
	//sync
	//c.collectionsPartialView.Mutex.Lock()
	//defer c.collectionsPartialView.Mutex.Unlock()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	//imp

	//if collection  already exists return error
	_, exists := c.collectionsPartialView.GetCollection(collectionId)
	if exists {
		c.log.Error("Duplicate collection ID detected. A collection with the same UUID already exists", "collection_id", collectionId, "collection_name", collectionName, "records", fmt.Sprintf("%+v", records), "executing_user", userId, "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck)
		return errors.New("collection already exists")
	}

	//compute size estimation
	size := uint(len(userId))
	for _, group := range groups {
		size += uint(len(group))
	}
	//free space for collection
	if err := c.migrationManager.FreeSpaceToCreateCollection(size); err != nil {
		c.log.Error("Failed to free space for the new collection", "collection_id", collectionId, "collection_name", collectionName, "records", fmt.Sprintf("%+v", records), "executing_user", userId, "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck, "error", err)
		return err
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
			LeaderIp:            c.serverInfo.HostName,
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
	actualSize, sizeErr := c.dbInterface.GetCollectionMemoryUsage(collectionId)
	if sizeErr != nil {
		c.log.Error("Failed to retrieve the space occupied in the DB by the collection just persisted", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId, "records", fmt.Sprintf("%+v", records), "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck, "error", sizeErr)
		return sizeErr
	}
	c.log.Info("Collection persisted in DB successfully", "collection_id", collectionId, "actual_size", actualSize)

	//add collection in local view
	migrationManagerErr := c.migrationManager.NewCollectionCreated(collectionId, actualSize, isStrong)
	if migrationManagerErr != nil {
		c.log.Error("Failed to register newly created collection in Migration Manager view", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId, "records", fmt.Sprintf("%+v", records), "is_strong", isStrong, "skip_uniqueness_check", skipUniquenessCheck, "error", migrationManagerErr)
		return migrationManagerErr
	}

	c.log.Info("Collection created successfully", "collection_id", collectionId, "collection_name", collectionName, "executing_user", userId)
	return nil
}

func (c *CollectionsDiscovererImp) RemoveCollectionAndPropagate(collectionId string, groups []string, userId string, isInitiator bool, ignoredHostName string) error {
	c.log.Info("RemoveCollectionAndPropagate called. Processing collection deletion", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
	//sync
	//c.collectionsPartialView.Mutex.Lock()
	c.mutex.Lock()
	//imp

	collection, exists := c.collectionsPartialView.GetCollection(collectionId)
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
			//c.collectionsPartialView.Mutex.Unlock() // lock release
			c.mutex.Unlock()
			c.log.Error("Cannot delete strong collection remotely", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
			return errors.New("strong collection can't be removed remotely")
		}

		//c.collectionsPartialView.Mutex.Unlock() // lock release
		c.mutex.Unlock()
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

	c.collectionsPartialView.RemoveCollection(collectionId)
	err = c.dbInterface.DeleteCollection(collectionId) //remove from local db
	if err != nil {
		c.log.Error("Failed to delete the local collection from DB", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "is_initiator", isInitiator, "ignored_hostname", ignoredHostName)
	}
	c.migrationManager.RemoveCollectionFromLru(collectionId) //remove from lru

	//c.collectionsPartialView.Mutex.Unlock() // lock release
	c.mutex.Unlock()
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

func (c *CollectionsDiscovererImp) GetCollectionVectorClockValue(collectionId string) (uint64, error) {
	//sync
	//c.collectionsPartialView.Mutex.Lock()
	//defer c.collectionsPartialView.Mutex.Unlock()

	//imp
	collection, exists := c.collectionsPartialView.GetCollection(collectionId)
	if !exists {
		return 0, errors.New("collection not found")
	}
	if !collection.IsStrong() {
		return 0, errors.New("collection is not strong")
	}
	if !collection.IsLocal() {
		return 0, errors.New("collection is not local")
	}

	return collection.(*shared.StrongCollection).ConsistencyClock, nil
}
