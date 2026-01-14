package migrationManager

import (
	"config"
	"context"
	"dataManager/lru"
	"dataManager/messages"
	penality "dataManager/migrationManager/penalityFunction"
	"dataManager/migrationManager/strongPolicy"
	weakpolicy "dataManager/migrationManager/weakPolicy"
	"dataManager/sender"
	shared "dataManager/shared"
	"db"
	"errors"
	"fmt"
	"server"
	"sync"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
)

const emptyCollectionSize uint = 500

type MigrationManager interface {
	// Init initialize the singleton pattern
	Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo server.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger logger.Logger, mutex *sync.Mutex)
	StatelessInit(collections *shared.CollectionPartialView, config config.MigrationManagerConfig, lruConfig config.LruConfig, serverInfo server.ServerInfo, lruCache *lru.LRUCache[shared.Collection], logger logger.Logger, mutex *sync.Mutex)
	// UpdatesRequestsInfo used to update lru in real time (collectionID)
	// if the collection is strong used to update forces in real time.
	// if the request comes from a user => nodeName = "user"
	UpdatesRequestsInfo(info []messages.CollectionRequestInfo, sender string)
	HandlePostponedDecisions(collection shared.Collection)
	UpdateCollectionsLatestUsage(collections []string)
	// ManageGossipMessage used to update non realtime info
	ManageGossipMessage(messages.GossipMessage)
	// NewCollectionReceived used when the node receive  a collection from one of the directly connected nodes (collectionID,isStrong,leader)
	//return True if the collection is already stored locally and must not be stored in Redis
	//throw an error if there is not enough space to store the collection locally and the LRU cannot free enough space
	NewCollectionReceived(collectionId string, isStrong bool, sequence uint64, isLeader bool, records map[string]db.RecordValue, groups []string, owner string, groupsSequence uint64, collectionSize uint64, hostToRemove bool, hostAddress string) (bool, error)
	// NewCollectionCreated used when user create a new collection in the local node (collectionID,size,isStrong)
	//throw an error if the collection already exists locally
	NewCollectionCreated(string, uint64, bool) error
	FreeSpaceToCreateCollection(uint) error

	RemoveCollectionFromLru(collectionId string)

	StopAsyncDump()
}

type MigrationManagerImp struct {
	//config
	config     config.MigrationManagerConfig
	serverInfo server.ServerInfo
	log        logger.Logger

	//policy
	StrongCollectionPolicy strongPolicy.StrongCollectionPolicy

	alwaysCopyPolicy     weakpolicy.WeakCollectionPolicy
	doNothingPolicy      weakpolicy.DoNothingPolicy
	WeakCollectionPolicy weakpolicy.WeakCollectionPolicy

	//policy update strategy
	policyUpdateStrategy policyUpdateStrategy

	//impl
	collectionsPartialView *shared.CollectionPartialView //set of local and remotes collections
	mutex                  *sync.Mutex

	lruCache    *lru.LRUCache[shared.Collection]
	dbInterface db.GenericDB

	sender sender.MigrationManagerSender

	dumpCtx    context.Context
	dumpCancel context.CancelFunc
}

// Singleton pattern
var migrationManagerInstance MigrationManager
var once sync.Once
var initOnce sync.Once

func GetMigrationManagerInstance() MigrationManager {
	once.Do(func() {
		migrationManagerInstance = &MigrationManagerImp{}
	})
	return migrationManagerInstance
}

func (m *MigrationManagerImp) Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo server.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger logger.Logger, mutex *sync.Mutex) {
	initOnce.Do(func() {
		//imp
		m.serverInfo = serverInfo
		m.log = logger
		m.dbInterface = dbInterface
		m.lruCache = lruCache
		m.config = config
		m.mutex = mutex

		//prepare server
		m.sender = sender.NewMigrationManagerSender(m.log, server, serverInfo, collections, dbInterface, lruCache, mutex)
		server.AddCustomRoute(m.sender.GossipPath(), true, m.handleIncomingGossipMessages().ServeHTTP)
		server.AddCustomRoute(m.sender.RepoPath(), true, m.handleIncomingCollection().ServeHTTP)
		server.AddCustomRoute(m.sender.RequestInfoPath(), true, m.handleIncomingRequestInfoMessages().ServeHTTP)

		//policy
		wp := weakpolicy.GetPolicy(config.WeakPolicy)
		sp := strongPolicy.GetPolicy(config.StrongPolicy)

		if strongPolicy.GetPolicy(config.StrongPolicy) == strongPolicy.DONOTHINGPOLICY {
			m.StrongCollectionPolicy = strongPolicy.NewDoNothingPolicy()
		} else {
			m.StrongCollectionPolicy = strongPolicy.NewDefaultPolicy(config, lruConfig, collections, penalityFunc, dbInterface, serverInfo, m.sender, m.lruCache, m.log, mutex)
		}

		if weakpolicy.GetPolicy(config.WeakPolicy) == weakpolicy.DONOTHINGPOLICY {
			m.doNothingPolicy = weakpolicy.NewDoNothingPolicy()
		} else {
			m.WeakCollectionPolicy = weakpolicy.NewAlwaysCopyPolicy(config, m.sender, m.log, collections, dbInterface, serverInfo)
		}

		//policy update strategy
		if wp == weakpolicy.DONOTHINGPOLICY && sp == strongPolicy.DONOTHINGPOLICY {
			m.policyUpdateStrategy = newNoUpdateStrategy()
		} else {
			m.policyUpdateStrategy = newErmesPolicyUpdateStrategy()
		}

		//init data
		m.collectionsPartialView = collections

		// --- START ASYNC DUMP ---
		interval := config.KnownCollectionsDumpInterval
		if interval > 0 {
			m.dumpCtx, m.dumpCancel = context.WithCancel(context.Background())
			m.startAsyncDump(m.dumpCtx, config.KnownCollectionsDumpPath, config.KnownCollectionsDumpInterval)
			m.log.Info("MIG-MANAGER, async dump enabled")
		} else {
			m.dumpCtx, m.dumpCancel = nil, nil
			m.log.Info("MIG-MANAGER, async dump disabled")
		}
		m.log.Info("MIG-MANAGER, migration manager instance has been initialized")
	})
}

func (m *MigrationManagerImp) StatelessInit(collections *shared.CollectionPartialView, config config.MigrationManagerConfig, lruConfig config.LruConfig, serverInfo server.ServerInfo, lruCache *lru.LRUCache[shared.Collection], logger logger.Logger, mutex *sync.Mutex) {
	initOnce.Do(func() {
		//imp
		m.serverInfo = serverInfo
		m.log = logger
		//no db interface
		m.lruCache = lruCache
		m.config = config

		m.mutex = mutex

		//no sender
		m.StrongCollectionPolicy = strongPolicy.NewDoNothingPolicy()
		m.WeakCollectionPolicy = weakpolicy.NewDoNothingPolicy()

		//policy update strategy
		m.policyUpdateStrategy = newNoUpdateStrategy()

		//Dump ?
	})
}

func (m *MigrationManagerImp) startAsyncDump(ctx context.Context, dumpPath string, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := m.collectionsPartialView.DumpToJsonFile(dumpPath); err != nil {
					m.log.Warn("Failed to dump known collections", "error", err)
				}
			case <-ctx.Done():
				m.log.Info("Async dump goroutine received stop signal. Dump known collection for last time")
				if err := m.collectionsPartialView.DumpToJsonFile(dumpPath); err != nil {
					m.log.Warn("Failed to dump known collections", "error", err)
				}
				return
			}
		}
	}()
}

func (m *MigrationManagerImp) StopAsyncDump() {
	if m.dumpCancel != nil {
		m.dumpCancel()
		m.log.Info("MIG-MANAGER, async dump has been stopped")
	}
}

func (m *MigrationManagerImp) RemoveCollectionFromLru(collectionId string) {
	//sync
	// Lock già acquisito in RemoveCollectionAndPropagate
	//m.collectionsLock.Lock()
	//defer m.collectionsLock.Unlock()

	m.lruCache.Remove(collectionId)
	m.log.Info("MIG-MANAGER, collection removed from LRU", "collectionId", collectionId, "lru_size", m.lruCache.GetCurrentSize())
}

// methods
func (m *MigrationManagerImp) NewCollectionReceived(collectionId string, isStrong bool, sequence uint64, isLeader bool, records map[string]db.RecordValue, groups []string, owner string, groupsSequence uint64, collectionSize uint64, hostToRemove bool, hostAddress string) (bool, error) {
	//sync
	m.mutex.Lock()
	//defer m.mutex.Unlock()

	//imp
	m.log.Info("MIG-MANAGER, new local collection received", "collection_id", collectionId, "is_strong", isStrong, "is_leader", isLeader)
	collection, exists := m.collectionsPartialView.GetCollection(collectionId)
	if exists {
		if collection.IsLocal() {
			collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Lock()
			m.mutex.Unlock()
		}

		if collection.IsStrong() && collection.IsLocal() && collection.(*shared.StrongCollection).HasPostponedDecisionBehaviour() {
			collection.(*shared.StrongCollection).RemovePostponedDeleteBehaviour()
		}

		if collection.IsLocal() {
			if hostToRemove {
				collection.RemoveHost(hostAddress)
			}

			if collection.IsStrong() && isLeader && isStrong {
				collection.(*shared.StrongCollection).IsLeader = true
				collection.(*shared.StrongCollection).LeaderId = m.serverInfo.HostName
			}

			collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()
			m.log.Info("MIG-MANAGER, new collection already present locally", "collection_id", collectionId, "is_strong", isStrong, "is_leader", isLeader)
			return true, nil
		}

		collection.SetToLocal(collectionSize)
		localState := collection.GetCollectionState().(*shared.LocalCollection)

		m.mutex.Unlock()

		actualSize, err := m.addCollectionInRedis(collectionId, records, groups, owner, groupsSequence, collectionSize)
		if err != nil {
			m.log.Warn("MIG-MANAGER, new collection could not be added to redis", "collection_id", collectionId)

			m.mutex.Lock()
			collection.SetToRemote()
			localState.CollectionLock.Unlock()
			m.mutex.Unlock()

			return true, err
		}

		if hostToRemove {
			collection.RemoveHost(hostAddress)
		}

		if collection.IsStrong() && isLeader && isStrong {
			collection.(*shared.StrongCollection).IsLeader = true
			collection.(*shared.StrongCollection).LeaderId = m.serverInfo.HostName
		}

		//collection.SetToLocal(actualSize)
		if isStrong && collection.IsStrong() {
			collection.(*shared.StrongCollection).ConsistencyClock = sequence
		}

		m.log.Info("MIG-MANAGER, new collection set to local", "collection_id", collectionId, "is_strong", isStrong, "is_leader", isLeader)
		m.updateLru(collection, uint(actualSize))
		collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()
		return false, nil
	}

	var coll shared.Collection
	if isStrong {
		coll = shared.NewStrongCollection(collectionId, collectionSize, false, isLeader, m.serverInfo.HostName)
		coll.(*shared.StrongCollection).ConsistencyClock = sequence
	} else {
		coll = shared.NewWeakCollection(collectionId, collectionSize, false)
	}
	localState := coll.GetCollectionState().(*shared.LocalCollection)
	localState.CollectionLock.Lock()
	m.collectionsPartialView.SetCollection(collectionId, coll)
	m.log.Info("MIG-MANAGER, new collection added to known collections")
	m.mutex.Unlock()

	actualSize, err := m.addCollectionInRedis(collectionId, records, groups, owner, groupsSequence, collectionSize)
	if err != nil {
		m.log.Warn("MIG-MANAGER, new collection could not be added")

		m.mutex.Lock()
		coll.SetToRemote()
		localState.CollectionLock.Unlock()
		m.mutex.Unlock()

		return true, err
	}
	m.updateLru(coll, uint(actualSize))
	localState.CollectionLock.Unlock()
	m.log.Info("MIG-MANAGER, collection placed on top in the LRU")
	return false, nil
}

func (m *MigrationManagerImp) addCollectionInRedis(collectionId string, records map[string]db.RecordValue, groups []string, owner string, groupsSequence uint64, collectionSize uint64) (uint64, error) {
	availableMem, memErr := m.dbInterface.GetAvailableMemory()
	if memErr != nil {
		m.log.Error("MIG-MANAGER, get memory usage fails")
		return 0, memErr // TODO
	}

	//check size before set collection
	m.log.Info("Available memory and lru size before adding collection", "available_mem", availableMem, "available_lru", m.lruCache.GetAvailableSize(), "collection_size", collectionSize)
	if availableMem < collectionSize || m.lruCache.GetAvailableSize() < uint(collectionSize) { //if not enough space in redis or in the lru => free space
		m.log.Warnf("MIG-MANAGER, not enough space in the LRU to place the collection %s", collectionId)
		err := m.freeLru(uint(collectionSize))
		if err != nil {
			m.log.Error("MIG-MANAGER, free lru fails", "collection_id", collectionId, "required_size", collectionSize, "available_mem", availableMem, "available_lru", m.lruCache.GetAvailableSize(), "error", err)
			return 0, err
		}
	}

	err := m.dbInterface.SetCollection(collectionId, records, groups, owner, groupsSequence) //add collection to redis
	if err != nil {
		m.log.Error("MIG-MANAGER, add collection in redis fails")
		return 0, err // TODO: decide
	}

	//Get actual size in redis. Sometimes can be different from the size sent by another node
	colMemUsage, err := m.dbInterface.GetCollectionMemoryUsage(collectionId)
	if err != nil {
		m.log.Error("MIG-MANAGER, get collection memory usage fails")
		return 0, err
	}
	m.log.Info("Collection memory usage after adding in redis", "collection_id", collectionId, "collection_memory_usage", colMemUsage)

	if colMemUsage > collectionSize {
		m.log.Warn("MIG-MANAGER, collection size in redis is greater than the size sent by the remote node", "collection_id", collectionId, "size_in_redis", colMemUsage, "size_sent", collectionSize)
		err := m.freeLru(uint(colMemUsage))
		if err != nil {
			m.log.Error("MIG-MANAGER, free lru fails", "collection_id", collectionId, "required_size", collectionSize, "available_mem", availableMem, "available_lru", m.lruCache.GetAvailableSize(), "error", err)
			return 0, err
		}
	}
	return colMemUsage, nil
}

func (m *MigrationManagerImp) FreeSpaceToCreateCollection(size uint) error {
	requiredSpace := emptyCollectionSize + size
	m.log.Info("MIG-MANAGER, free space to create collection", "size", requiredSpace)

	availableMem, memErr := m.dbInterface.GetAvailableMemory()
	if memErr != nil {
		m.log.Error("MIG-MANAGER, get memory usage fails")
		return memErr
	}

	availableLruSize := m.lruCache.GetAvailableSize()
	m.log.Debug("MIG-MANAGER, available memory and lru size", "mem", availableMem, "lru", availableLruSize)
	if availableMem < uint64(requiredSpace) || availableLruSize < requiredSpace {
		m.log.Warnf("MIG-MANAGER, not enough space in the LRU to place the new collection")
		return m.freeLru(requiredSpace)
	}

	return nil
}

func (m *MigrationManagerImp) NewCollectionCreated(collectionId string, collectionSize uint64, isStrong bool) error {
	//sync
	// Lock già acquisito in SetCollectionAndPropagate
	//m.collectionsLock.Lock()
	//defer m.collectionsLock.Unlock()

	m.log.Info("MIG-MANAGER, new local collection created")
	_, exists := m.collectionsPartialView.GetCollection(collectionId)
	if exists {
		m.log.Errorf("MIG-MANAGER, can't create a collection if the id %s is already present", collectionId)
		return errors.New("can't create a collection if the id is already present")
	}

	var coll shared.Collection
	if isStrong {
		coll = shared.NewStrongCollection(collectionId, collectionSize, false, true, m.serverInfo.HostName) //when I create a strong collection I'm the leader
	} else {
		coll = shared.NewWeakCollection(collectionId, collectionSize, false)
	}
	m.collectionsPartialView.SetCollection(collectionId, coll)
	m.log.Info("MIG-MANAGER, new collection added to known collections")

	//place on top in the LRU
	err := m.lruCache.Put(collectionId, coll, uint(collectionSize))
	if err != nil {
		//if not enough space in the LRU for the new collection return error
		return err
	}
	m.log.Info("MIG-MANAGER, collection placed on top in the LRU", "lru_size", m.lruCache.GetCurrentSize())
	return nil
}

func (m *MigrationManagerImp) ManageGossipMessage(msg messages.GossipMessage) {
	m.log.Info("MIG-MANAGER, gossip message received")
	m.StrongCollectionPolicy.ManageGossipMessage(msg)
}
func (m *MigrationManagerImp) UpdateCollectionsLatestUsage(collections []string) { // Fix: Added opening brace
	m.policyUpdateStrategy.updateCollectionsLatestUsage(collections, m)
}

// UpdatesRequestsInfo call this when wasm function has completed
func (m *MigrationManagerImp) UpdatesRequestsInfo(info []messages.CollectionRequestInfo, sender string) {
	m.policyUpdateStrategy.updatesRequestsInfo(info, sender, m)
}

func (m *MigrationManagerImp) updatesRequestsInfo(info messages.CollectionRequestInfoMessages) {
	var msgs = make(map[string]messages.CollectionRequestInfoMessages) // host -> message
	//sync
	m.log.Info("MIG-MANAGER, update request info received", "num_messages", len(info.Messages))
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.log.Info("MIG-MANAGER, update request info received - lock acquired")

	for _, i := range info.Messages {

		collection, exists := m.collectionsPartialView.GetCollection(i.CollectionId)

		//if no local info on the collection -> remote update -> save the message to send it later
		if !exists {
			if m.serverInfo.Father == nil {
				m.log.Error("MIG-MANAGER, no info on the collection and no father to send the info")
				continue
			}

			fatherId := m.serverInfo.Father.HostName
			i.Sender = m.serverInfo.HostName
			msgs[fatherId] = messages.CollectionRequestInfoMessages{Messages: append(msgs[fatherId].Messages, i)}
		} else if !collection.IsStrong() && !collection.IsLocal() {
			hostId := m.serverInfo.GetClosestChild(collection.HostsId())
			if hostId == "" {
				if m.serverInfo.Father == nil {
					m.log.Error("MIG-MANAGER, no info on the collection and no father to send the info")
					continue
				}
				hostId = m.serverInfo.Father.HostName
			}
			i.Sender = m.serverInfo.HostName
			msgs[hostId] = messages.CollectionRequestInfoMessages{Messages: append(msgs[hostId].Messages, i)}
		} else { // local update
			m.localUpdateRequestInfo(collection.IsStrong(), i)
			//execute postponed migration behaviours if any and if possible
			m.handlePostponedDecisions(collection)
		}
	}

	m.log.Info("MIG-MANAGER, remote updates to send", "num_messages", len(msgs), "messages", fmt.Sprintf("%+v", msgs))

	//send remote updates
	for host, message := range msgs {
		m.sender.SendRequestInfoMessage(message, host)
	}
}

func (m *MigrationManagerImp) HandlePostponedDecisions(collection shared.Collection) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.handlePostponedDecisions(collection)
}

func (m *MigrationManagerImp) handlePostponedDecisions(collection shared.Collection) {
	m.log.Info("MIG-MANAGER, check postponed decision behaviour for collection", "collection_id", collection.CollectionId(), "is_strong", collection.IsStrong(), "is_local", collection.IsLocal())
	if collection.IsLocal() && collection.IsStrong() && collection.(*shared.StrongCollection).HasPostponedDecisionBehaviour() && collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.TryLock() {
		m.log.Info("MIG-MANAGER, starting postponed decision behaviour execution for strong collection", "collection_id", collection.CollectionId())
		localColl := collection.GetCollectionState().(*shared.LocalCollection)
		collection.(*shared.StrongCollection).ExecutePostponedDecisionBehaviour(m.log)
		localColl.CollectionLock.Unlock()
	}
}

func (m *MigrationManagerImp) localUpdateRequestInfo(isStrong bool, i messages.CollectionRequestInfoMessage) error {
	m.log.Info("MIG-MANAGER, new request for a local collection received")

	m.log.Info("MIG-MANAGER, update size in the LRU")
	err := m.lruCache.MoveToFront(i.CollectionId)
	if err != nil {
		m.log.Warn("Error moving to front in LRU cache", "collection_id", i.CollectionId, "error", err)
	}

	//if the collection is strong update forces
	if isStrong {
		m.log.Info("MIG-MANAGER, handle request for a strong collection")
		m.StrongCollectionPolicy.ManageNewStrongRequest(i.CollectionId, i.Sender, i.IsWrite, i.RequestLatency)
	} else {
		m.log.Info("MIG-MANAGER, handle request for a week collection")
		err := m.WeakCollectionPolicy.ManageNewWeakRequest(i.CollectionId, i.Sender, i.IsWrite, i.RequestLatency)
		if err != nil {
			m.log.Errorf("MIG-MANAGER, weak policy failed to handle the request for collection %s", i.CollectionId)
			return err
		}
	}
	return nil
}

func (m *MigrationManagerImp) updateLru(collection shared.Collection, collectionSize uint) error {
	err := m.lruCache.Put(collection.CollectionId(), collection, collectionSize)
	m.log.Info("MIG-MANAGER, collection placed on top in the LRU", "lru_size", m.lruCache.GetCurrentSize())
	if err != nil {
		m.log.Error("MIG-MANAGER, not enough space in the LRU to place the collection even after free", "collection_id", collection.CollectionId(), "error", err)
		return err //TODO: decide
	}
	return nil
}

func (m *MigrationManagerImp) freeLru(sizeGoal uint) error {
	m.log.InfofWith("DEFAULT-STRONG-POLICY, set lru to safe level %d", []any{sizeGoal}, "lru_size", m.lruCache.GetCurrentSize())

	if m.serverInfo.Father == nil {
		m.log.Error("DEFAULT-STRONG-POLICY, no father to send collections to free lru space")
		return errors.New("no father to send collections to free lru space")
	}

	collectionList := m.lruCache.PeekFreeToAchieveSize(sizeGoal) //get elements to remove from lru without removing them
	m.log.Info("DEFAULT-STRONG-POLICY, collections to send to remote nodes to achieve safe level", "num_collections", collectionList.Len(), "collections", collectionList, "new_lru_size", m.lruCache.GetCurrentSize())

	var wg sync.WaitGroup
	for item := collectionList.Front(); item != nil; item = item.Next() {
		collection := item.Value.(shared.Collection)

		if collection.IsStrong() && collection.(*shared.StrongCollection).HasPostponedDecisionBehaviour() {
			continue
		}

		if m.serverInfo.Father != nil && collection.GetCollectionState().(*shared.LocalCollection).HasPostponedUpdate(m.serverInfo.Father.HostName) {
			continue
		}

		wg.Go(func() {
			m.sendCollectionAndRemove(collection)
		})
	}
	wg.Wait()

	//double check size to return error if cannot free enough space
	if m.lruCache.GetAvailableSize() < sizeGoal {
		m.log.Error("MIG-MANAGER, could not free enough space in the LRU", "required_size", sizeGoal, "available_size", m.lruCache.GetAvailableSize())
		return errors.New("could not free enough space in the LRU")
	}

	return nil
}

func (m *MigrationManagerImp) sendCollectionAndRemove(collection shared.Collection) error {
	//sync -> avoid send a collection if used
	localColl := collection.GetCollectionState().(*shared.LocalCollection)
	if !localColl.CollectionLock.TryLock() {
		//TODO: capire cosa fare in questo caso
		m.log.Warn("MIG-MANAGER, collection is currently in use, skip migration", "collection", collection)
		return errors.New("collection is currently in use, skip migration")
	}
	defer localColl.CollectionLock.Unlock()

	//remove collection from lru
	m.lruCache.Remove(collection.CollectionId())

	//get collection from redis instance
	res, groups, owner, groupsSequence, err := m.dbInterface.GetCollection(collection.CollectionId())
	if err != nil {
		m.log.Error("MIG-MANAGER, could not get collection", "collection", collection, "error", err)
		return err //TODO: decide
	}
	collectionSize, sizeError := m.dbInterface.GetCollectionMemoryUsage(collection.CollectionId())
	if sizeError != nil {
		m.log.Error("MIG-MANAGER, failed to get the collection size")
		return sizeError //TODO: decide
	}
	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	isLeader := false
	if collection.IsStrong() {
		isLeader = collection.(*shared.StrongCollection).IsLeader
		if isLeader {
			collection.(*shared.StrongCollection).LeaderId = m.serverInfo.Father.HostName
		}
		//stop keep track of access for a remote collection
		m.StrongCollectionPolicy.RemoveInfo(collection.CollectionId())
	}

	fatherName := m.serverInfo.Father.HostName
	collection.GetCollectionState().AddCollectionMigrationHost(fatherName)

	//remove collection from redis instance
	m.dbInterface.DeleteCollection(collection.CollectionId())

	//set the collection as remote only in the node partial view
	collection.SetToRemote()

	m.sender.SendRemoteCollectionTo(fatherName, collection, isLeader, res, groups, owner, groupsSequence, collectionSize, true)

	return nil
}
