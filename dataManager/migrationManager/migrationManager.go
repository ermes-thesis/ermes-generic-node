package migrationManager

import (
	"config"
	"dataManager/lru"
	gossip "dataManager/migrationManager/gossipMessages"
	sender "dataManager/migrationManager/migrationSender"
	penality "dataManager/migrationManager/penalityFunction"
	"dataManager/migrationManager/strongPolicy"
	weakpolicy "dataManager/migrationManager/weakPolicy"
	shared "dataManager/shared"
	"db"
	"errors"
	"fmt"
	log "logger"
	"server"
	"server/info"
	"sync"
)

type MigrationManager interface {
	// Init initialize the singleton pattern
	Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger log.Logger)
	// ManageNewRequest used to update lru in real time (collectionID)
	// if the collection is strong used to update forces in real time (collectionID, nodeName, write?, request cumulative latency).
	// if the request comes from a user => nodeName = "user"
	ManageNewRequest(string, string, bool, int) error
	// ManageGossipMessage used to update non realtime info
	ManageGossipMessage(gossip.GossipMessage)
	// NewCollectionReceived used when the node receive  a collection from one of the directly connected nodes (collectionID,isStrong,leader)
	//return True if the collection is already stored locally and must not be stored in Redis
	NewCollectionReceived(string, bool, bool, map[string]string, []string) bool
	// NewCollectionCreated used when user create a new collection in the local node (collectionID,size,isStrong)
	//throw an error if the collection already exists locally
	NewCollectionCreated(string, uint64, bool) error

	ChangeStrongCollectionPolicy(strongPolicy.CurrentPolicy)
	ChangeWeakCollectionPolicy(weakpolicy.CurrentPolicy)
}

type MigrationManagerImp struct {
	//config
	config     config.MigrationManagerConfig
	serverInfo info.ServerInfo
	logger     log.Logger

	//policy
	defaultPolicy          strongPolicy.StrongCollectionPolicy
	StrongCollectionPolicy strongPolicy.StrongCollectionPolicy

	alwaysCopyPolicy     weakpolicy.WeakCollectionPolicy
	WeakCollectionPolicy weakpolicy.WeakCollectionPolicy

	//impl
	collectionsPartialView *shared.CollectionPartialView //set of local and remotes collections

	lruCache    *lru.LRUCache[shared.Collection]
	dbInterface db.GenericDB

	sender sender.Sender
}

// Singleton pattern
var migrationManagerInstance MigrationManager
var once sync.Once
var initOnce sync.Once

func GetMigrationManagerInstance(logger log.Logger) MigrationManager {
	once.Do(func() {
		migrationManagerInstance = &MigrationManagerImp{logger: logger}
	})
	return migrationManagerInstance
}

func (m *MigrationManagerImp) Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger log.Logger) {
	initOnce.Do(func() {
		//imp
		if m.collectionsPartialView != nil { //if singleton already initialized
			return
		}

		m.serverInfo = serverInfo
		m.logger = logger
		m.dbInterface = dbInterface
		m.lruCache = lruCache
		m.config = config
		//prepare server
		m.sender = sender.NewSender(m.logger, server)
		server.AddCustomRoute(m.sender.GossipPath(), true, m.handleIncomingGossipMessages(logger).ServeHTTP)
		server.AddCustomRoute(m.sender.RepoPath(), true, m.handleIncomingCollection(logger).ServeHTTP)

		//policy

		m.defaultPolicy = strongPolicy.NewDefaultPolicy(config, lruConfig, collections, penalityFunc, dbInterface, serverInfo, m.sender, m.lruCache, m.logger)
		m.StrongCollectionPolicy = m.defaultPolicy

		m.alwaysCopyPolicy = weakpolicy.NewAlwaysCopyPolicy(config, m.sender, logger, collections, dbInterface)
		m.WeakCollectionPolicy = m.alwaysCopyPolicy

		//init data
		m.collectionsPartialView = collections

		logger.Println("MIG-MANAGER", "migration manager instance has been initialized", log.INFO)
	})
}

// methods
func (m *MigrationManagerImp) NewCollectionReceived(collectionId string, isStrong bool, isLeader bool, records map[string]string, groups []string) bool {
	m.logger.Println("MIG-MANAGER", "new local collection received", log.INFO)
	collection := m.collectionsPartialView.KnownCollections[collectionId]
	if collection != nil {
		if collection.IsLocal() {
			m.logger.Println("MIG-MANAGER", "new collection already present locally", log.WARNING)
			return true
			//TODO probably for weak collections conflict must be solved in this case
		}
		err := m.dbInterface.SetCollection(collectionId, records, groups) //add collection to redis
		if err != nil {
			m.logger.Println("MIG-MANAGER", "add collection in redis fails", log.ERROR)
			panic(err) //TODO: decide
		}
		collectionSize, sizeError := m.dbInterface.GetCollectionMemoryUsage(collectionId)
		if sizeError != nil {
			m.logger.Println("MIG-MANAGER", "get collection size fails", log.ERROR)
			panic(sizeError) //TODO: decide -> free lru
		}

		collection.SetToLocal(collectionSize)
		m.logger.Println("MIG-MANAGER", "new collection set to local", log.INFO)
		return false
	}
	err := m.dbInterface.SetCollection(collectionId, records, groups) //add collection to redis
	if err != nil {
		m.logger.Println("MIG-MANAGER", "add collection in redis fails", log.INFO)
		panic(err) //TODO: decide
	}
	collectionSize, sizeError := m.dbInterface.GetCollectionMemoryUsage(collectionId)
	if sizeError != nil {
		m.logger.Println("MIG-MANAGER", "get collection size fails", log.ERROR)
		panic(sizeError) //TODO: decide -> free lru
	}

	if isStrong {
		m.collectionsPartialView.KnownCollections[collectionId] = shared.NewStrongCollection(collectionId, collectionSize, false, isLeader)
	} else {
		m.collectionsPartialView.KnownCollections[collectionId] = shared.NewWeakCollection(collectionId, collectionSize, false)
	}
	m.logger.Println("MIG-MANAGER", "new collection added to known collections", log.INFO)
	//place on top in the LRU
	m.updateLru(collectionId)
	m.logger.Println("MIG-MANAGER", "collection placed on top in the LRU", log.INFO)
	return false
}

func (m *MigrationManagerImp) NewCollectionCreated(collectionId string, collectionSize uint64, isStrong bool) error {
	m.logger.Println("MIG-MANAGER", "new local collection created", log.INFO)
	collection := m.collectionsPartialView.KnownCollections[collectionId]
	if collection != nil {
		m.logger.Println("MIG-MANAGER", fmt.Sprintf("can't create a collection if the id %s is already present", collectionId), log.ERROR)
		return errors.New("can't create a collection if the id is already present")
	}

	if isStrong {
		m.collectionsPartialView.KnownCollections[collectionId] = shared.NewStrongCollection(collectionId, collectionSize, false, true) //when I create a strong collection I'm the leader
	} else {
		m.collectionsPartialView.KnownCollections[collectionId] = shared.NewWeakCollection(collectionId, collectionSize, false)
	}
	m.logger.Println("MIG-MANAGER", "new collection added to known collections", log.INFO)

	//place on top in the LRU
	m.updateLru(collectionId)
	m.logger.Println("MIG-MANAGER", "collection placed on top in the LRU", log.INFO)
	return nil
}

func (m *MigrationManagerImp) ManageGossipMessage(msg gossip.GossipMessage) {
	m.logger.Println("MIG-MANAGER", "gossip message received", log.INFO)
	m.StrongCollectionPolicy.ManageGossipMessage(msg)
}

// ManageNewRequest call this when wasm function has completed
func (m *MigrationManagerImp) ManageNewRequest(collectionId string, nodeName string, isWrite bool, requestLatency int) error {
	//if no local info on the collection
	if m.collectionsPartialView.KnownCollections[collectionId] == nil || !m.collectionsPartialView.KnownCollections[collectionId].IsLocal() {
		m.logger.Println("MIG-MANAGER", fmt.Sprintf("collection %s not found locally or not local", collectionId), log.ERROR)
		return errors.New("collection not found locally or not local")
	}
	m.logger.Println("MIG-MANAGER", "new request for a local collection received", log.INFO)

	//place on top in the LRU
	m.updateLru(collectionId)
	m.logger.Println("MIG-MANAGER", "collection placed on top in the LRU", log.INFO)

	//if the collection is strong update forces
	if m.collectionsPartialView.KnownCollections[collectionId].IsStrong() {
		m.logger.Println("MIG-MANAGER", "handle request for a strong collection", log.INFO)
		m.StrongCollectionPolicy.ManageNewStrongRequest(collectionId, nodeName, isWrite, requestLatency)
	} else {
		m.logger.Println("MIG-MANAGER", "handle request for a week collection", log.INFO)
		m.WeakCollectionPolicy.ManageNewWeakRequest(collectionId, nodeName, isWrite, requestLatency)
	}

	return nil
}

func (m *MigrationManagerImp) updateLru(collectionId string) {
	collectionSize := uint(m.collectionsPartialView.KnownCollections[collectionId].GetCollectionState().(*shared.LocalCollection).Size())
	err := m.lruCache.Put(collectionId, m.collectionsPartialView.KnownCollections[collectionId], collectionSize)
	if err != nil {
		//if not enough space in the LRU free the LRU and then retry
		m.logger.Println("MIG-MANAGER", fmt.Sprintf("not enough space in the LRU to place the collection %s", collectionId), log.WARNING)
		m.freeLru(collectionSize)
		err = m.lruCache.Put(collectionId, m.collectionsPartialView.KnownCollections[collectionId], collectionSize)
		if err != nil {
			m.logger.Println("MIG-MANAGER", fmt.Sprintf("not enough space in the LRU to place the collection %s even after free", collectionId), log.ERROR)
			panic(err) //TODO: decide
		}
	}
}

func (m *MigrationManagerImp) freeLru(sizeGoal uint) {
	m.logger.Println("MIG-MANAGER", fmt.Sprintf("free space in the lru to achieve size %d", sizeGoal), log.INFO)
	collectionList := m.lruCache.FreeToAchieveSize(sizeGoal) //TODO send the collection up to the

	for item := collectionList.Front(); item != nil; item = item.Next() {
		collection := item.Value.(shared.Collection)
		go m.SendCollectionAndRemove(collection)
	}
}

func (m *MigrationManagerImp) SendCollectionAndRemove(collection shared.Collection) {
	//sync -> avoid send a collection if used
	collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Lock()
	defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()

	//get collection from redis instance
	res, groups, err := m.dbInterface.GetCollection(collection.CollectionId())
	if err != nil {
		panic(err) //TODO: decide
	}
	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	isLeader := false
	if collection.IsStrong() {
		isLeader = collection.(*shared.StrongCollection).IsLeader
		if isLeader {
			collection.(*shared.StrongCollection).LeaderId = m.serverInfo.Father.Address
		}
		//stop keep track of access for a remote collection
		m.StrongCollectionPolicy.RemoveInfo(collection.CollectionId())
	}
	m.sender.SendRemoteCollectionTo(m.serverInfo.Father.Address, collection, isLeader, res, groups)

	//remove collection from redis instance
	m.dbInterface.DeleteCollection(collection.CollectionId())

	//set the collection as remote only in the node partial view
	collection.SetToRemote()
}

func (m *MigrationManagerImp) ChangeStrongCollectionPolicy(newPolicy strongPolicy.CurrentPolicy) {
	switch newPolicy {
	case strongPolicy.DefaultPolicy:
		m.StrongCollectionPolicy = m.defaultPolicy
	default:
		panic("unknown policy")
	}
}

func (m *MigrationManagerImp) ChangeWeakCollectionPolicy(newPolicy weakpolicy.CurrentPolicy) {
	switch newPolicy {
	case weakpolicy.ALWAYSCOPYPOLICY:
		m.WeakCollectionPolicy = m.alwaysCopyPolicy
	default:
		panic("unknown policy")
	}
}
