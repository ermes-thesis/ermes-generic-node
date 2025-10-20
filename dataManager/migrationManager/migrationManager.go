package migrationManager

import (
	"config"
	"dataManager/lru"
	"dataManager/messages"
	penality "dataManager/migrationManager/penalityFunction"
	"dataManager/migrationManager/strongPolicy"
	weakpolicy "dataManager/migrationManager/weakPolicy"
	sender "dataManager/sender"
	shared "dataManager/shared"
	"db"
	"errors"
	"server"
	"server/info"
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

type MigrationManager interface {
	// Init initialize the singleton pattern
	Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger *logger.Logger)
	// UpdatesRequestsInfo used to update lru in real time (collectionID)
	// if the collection is strong used to update forces in real time.
	// if the request comes from a user => nodeName = "user"
	UpdatesRequestsInfo(info []messages.CollectionRequestInfo)
	// ManageGossipMessage used to update non realtime info
	ManageGossipMessage(messages.GossipMessage)
	// NewCollectionReceived used when the node receive  a collection from one of the directly connected nodes (collectionID,isStrong,leader)
	//return True if the collection is already stored locally and must not be stored in Redis
	NewCollectionReceived(collectionId string, isStrong bool, isLeader bool, records map[string]string, groups []string, groupsSequence uint64, collectionSize uint64, hostToRemove bool, hostAddress string) bool
	// NewCollectionCreated used when user create a new collection in the local node (collectionID,size,isStrong)
	//throw an error if the collection already exists locally
	NewCollectionCreated(string, uint64, bool) error

	ChangeStrongCollectionPolicy(strongPolicy.CurrentPolicy)
	ChangeWeakCollectionPolicy(weakpolicy.CurrentPolicy)
	GetCollectionVectorClockValue(collectionId string) (uint64, error)

	RemoveCollectionFromLru(collectionId string)
}

type MigrationManagerImp struct {
	//config
	config     config.MigrationManagerConfig
	serverInfo info.ServerInfo
	log        *logger.Logger

	//policy
	ermesPolicy            strongPolicy.StrongCollectionPolicy
	StrongCollectionPolicy strongPolicy.StrongCollectionPolicy

	alwaysCopyPolicy     weakpolicy.WeakCollectionPolicy
	WeakCollectionPolicy weakpolicy.WeakCollectionPolicy

	//impl
	collectionsPartialView *shared.CollectionPartialView //set of local and remotes collections
	collectionsLock        *sync.Mutex

	lruCache    *lru.LRUCache[shared.Collection]
	dbInterface db.GenericDB

	sender sender.MigrationManagerSender
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

func (m *MigrationManagerImp) Init(collections *shared.CollectionPartialView, penalityFunc penality.PenalityFunc, config config.MigrationManagerConfig, lruConfig config.LruConfig, server server.ServerType, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection], dbInterface db.GenericDB, logger *logger.Logger) {
	initOnce.Do(func() {
		//imp
		m.serverInfo = serverInfo
		m.log = logger
		m.dbInterface = dbInterface
		m.lruCache = lruCache
		m.config = config
		//m.collectionsLock = &sync.Mutex{}
		m.collectionsLock = &collections.Mutex

		//prepare server
		m.sender = sender.NewMigrationManagerSender(m.log, server, serverInfo)
		server.AddCustomRoute(m.sender.GossipPath(), true, m.handleIncomingGossipMessages().ServeHTTP)
		server.AddCustomRoute(m.sender.RepoPath(), true, m.handleIncomingCollection().ServeHTTP)
		server.AddCustomRoute(m.sender.RequestInfoPath(), true, m.handleIncomingRequestInfoMessages().ServeHTTP)

		//policy

		m.ermesPolicy = strongPolicy.NewDefaultPolicy(config, lruConfig, collections, penalityFunc, dbInterface, serverInfo, m.sender, m.lruCache, m.log)
		m.StrongCollectionPolicy = m.ermesPolicy

		m.alwaysCopyPolicy = weakpolicy.NewAlwaysCopyPolicy(config, m.sender, m.log, collections, dbInterface)
		m.WeakCollectionPolicy = m.alwaysCopyPolicy

		//init data
		m.collectionsPartialView = collections

		m.log.Info("MIG-MANAGER, migration manager instance has been initialized")
	})
}

func (m *MigrationManagerImp) RemoveCollectionFromLru(collectionId string) {
	//sync
	//m.collectionsLock.Lock()
	//defer m.collectionsLock.Unlock()

	m.lruCache.Remove(collectionId)
}

// methods
func (m *MigrationManagerImp) NewCollectionReceived(collectionId string, isStrong bool, isLeader bool, records map[string]string, groups []string, groupsSequence uint64, collectionSize uint64, hostToRemove bool, hostAddress string) bool {
	//sync
	m.collectionsLock.Lock()
	defer m.collectionsLock.Unlock()

	//imp
	m.log.Info("MIG-MANAGER, new local collection received")
	collection := m.collectionsPartialView.KnownCollections[collectionId]
	if collection != nil {
		if hostToRemove {
			collection.RemoveHost(hostAddress)
		}

		if collection.IsLocal() {
			m.log.Warn("MIG-MANAGER, new collection already present locally")
			return true
		}
		m.addCollectionInRedis(collectionId, records, groups, groupsSequence, collectionSize)

		collection.SetToLocal(collectionSize)
		m.log.Info("MIG-MANAGER, new collection set to local")
		m.updateLru(collectionId, uint(collectionSize))
		return false
	}
	m.addCollectionInRedis(collectionId, records, groups, groupsSequence, collectionSize)

	if isStrong {
		m.collectionsPartialView.KnownCollections[collectionId] = shared.NewStrongCollection(collectionId, collectionSize, false, isLeader)
	} else {
		m.collectionsPartialView.KnownCollections[collectionId] = shared.NewWeakCollection(collectionId, collectionSize, false)
	}
	m.log.Info("MIG-MANAGER, new collection added to known collections")
	m.updateLru(collectionId, uint(collectionSize))
	m.log.Info("MIG-MANAGER, collection placed on top in the LRU")
	return false
}

func (m *MigrationManagerImp) addCollectionInRedis(collectionId string, records map[string]string, groups []string, groupsSequence uint64, collectionSize uint64) {
	memStats, memErr := m.dbInterface.GetMemoryStats()
	if memErr != nil {
		m.log.Error("MIG-MANAGER, get memory usage fails")
		panic(memErr) //TODO
	}
	if memStats.CurrentUsedMemory < collectionSize || m.lruCache.GetAvailableSize() < uint(collectionSize) { //if not enoght space in redis or in the lru => free space
		m.log.Warnf("MIG-MANAGER, not enough space in the LRU to place the collection %s", collectionId)
		m.freeLru(uint(collectionSize))
	}
	//check size before set collection

	err := m.dbInterface.SetCollection(collectionId, records, groups, groupsSequence) //add collection to redis
	if err != nil {
		m.log.Error("MIG-MANAGER, add collection in redis fails")
		panic(err) //TODO: decide
	}
}

func (m *MigrationManagerImp) NewCollectionCreated(collectionId string, collectionSize uint64, isStrong bool) error {
	//sync
	//m.collectionsLock.Lock()
	//defer m.collectionsLock.Unlock()

	m.log.Info("MIG-MANAGER, new local collection created")
	collection := m.collectionsPartialView.KnownCollections[collectionId]
	if collection != nil {
		m.log.Errorf("MIG-MANAGER, can't create a collection if the id %s is already present", collectionId)
		return errors.New("can't create a collection if the id is already present")
	}

	if isStrong {
		m.collectionsPartialView.KnownCollections[collectionId] = shared.NewStrongCollection(collectionId, collectionSize, false, true) //when I create a strong collection I'm the leader
	} else {
		m.collectionsPartialView.KnownCollections[collectionId] = shared.NewWeakCollection(collectionId, collectionSize, false)
	}
	m.log.Info("MIG-MANAGER, new collection added to known collections")

	//place on top in the LRU
	err := m.lruCache.Put(collectionId, m.collectionsPartialView.KnownCollections[collectionId], uint(collectionSize))
	if err != nil {
		//if not enough space in the LRU for the new collection return error
		return err
	}
	m.log.Info("MIG-MANAGER, collection placed on top in the LRU")
	return nil
}

func (m *MigrationManagerImp) ManageGossipMessage(msg messages.GossipMessage) {
	m.log.Info("MIG-MANAGER, gossip message received")
	m.StrongCollectionPolicy.ManageGossipMessage(msg)
}

// UpdatesRequestsInfo call this when wasm function has completed
func (m *MigrationManagerImp) UpdatesRequestsInfo(info []messages.CollectionRequestInfo) {
	m.updatesRequestsInfo(messages.ToCollectionRequestInfoMessages(info, m.serverInfo.HostName))
}

func (m *MigrationManagerImp) updatesRequestsInfo(info messages.CollectionRequestInfoMessages) {
	var msgs = make(map[string]messages.CollectionRequestInfoMessages) // host -> message
	//sync
	m.collectionsLock.Lock()
	defer m.collectionsLock.Unlock()

	for _, i := range info.Messages {
		//if no local info on the collection -> remote update -> save the message to send it later
		if m.collectionsPartialView.KnownCollections[i.CollectionId] == nil {
			fatherId := m.serverInfo.Father.HostName
			if fatherId == "" {
				m.log.Error("MIG-MANAGER, no info on the collection and no father to send the info")
				continue
			}
			msgs[fatherId] = messages.CollectionRequestInfoMessages{Messages: append(msgs[fatherId].Messages, i)}
		} else if !m.collectionsPartialView.KnownCollections[i.CollectionId].IsStrong() && !m.collectionsPartialView.KnownCollections[i.CollectionId].IsLocal() {
			hostId := m.collectionsPartialView.GetClosestChild(m.collectionsPartialView.KnownCollections[i.CollectionId].HostsId(), m.serverInfo)
			if hostId == "" {
				m.log.Error("MIG-MANAGER, no info on the collection and no child to send the info")
				continue
			}
			msgs[hostId] = messages.CollectionRequestInfoMessages{Messages: append(msgs[hostId].Messages, i)}
		} else { // local update
			m.localUpdateRequestInfo(i)
		}
	}

	//send remote updates
	for host, message := range msgs {
		m.sender.SendRequestInfoMessage(message, host)
	}
}

func (m *MigrationManagerImp) localUpdateRequestInfo(i messages.CollectionRequestInfoMessage) {
	m.log.Info("MIG-MANAGER, new request for a local collection received")

	//place on top in the LRU
	m.updateLru(i.CollectionId, uint(m.collectionsPartialView.KnownCollections[i.CollectionId].GetCollectionState().(*shared.LocalCollection).Size()))
	m.log.Info("MIG-MANAGER, collection placed on top in the LRU")

	//if the collection is strong update forces
	if m.collectionsPartialView.KnownCollections[i.CollectionId].IsStrong() {
		m.log.Info("MIG-MANAGER, handle request for a strong collection")
		m.StrongCollectionPolicy.ManageNewStrongRequest(i.CollectionId, i.NodeName, i.IsWrite, i.RequestLatency)
	} else {
		m.log.Info("MIG-MANAGER, handle request for a week collection")
		m.WeakCollectionPolicy.ManageNewWeakRequest(i.CollectionId, i.NodeName, i.IsWrite, i.RequestLatency)
	}
}

func (m *MigrationManagerImp) updateLru(collectionId string, collectionSize uint) {
	err := m.lruCache.Put(collectionId, m.collectionsPartialView.KnownCollections[collectionId], collectionSize)
	if err != nil {
		m.log.Errorf("MIG-MANAGER, not enough space in the LRU to place the collection %s even after free", collectionId)
		panic(err) //TODO: decide
	}
}

func (m *MigrationManagerImp) freeLru(sizeGoal uint) {
	m.log.Info("MIG-MANAGER, free space in the lru to achieve size %d", sizeGoal)
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
	res, groups, groupsSequence, err := m.dbInterface.GetCollection(collection.CollectionId())
	if err != nil {
		panic(err) //TODO: decide
	}
	collectionSize, sizeError := m.dbInterface.GetCollectionMemoryUsage(collection.CollectionId())
	if sizeError != nil {
		panic(sizeError) //TODO: decide
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
	m.sender.SendRemoteCollectionTo(m.serverInfo.Father.HostName, collection, isLeader, res, groups, groupsSequence, collectionSize)

	//remove collection from redis instance
	m.dbInterface.DeleteCollection(collection.CollectionId())

	//set the collection as remote only in the node partial view
	collection.SetToRemote()
}

func (m *MigrationManagerImp) ChangeStrongCollectionPolicy(newPolicy strongPolicy.CurrentPolicy) {
	switch newPolicy {
	case strongPolicy.ErmesPolicy:
		m.StrongCollectionPolicy = m.ermesPolicy
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

func (m *MigrationManagerImp) GetCollectionVectorClockValue(collectionId string) (uint64, error) {
	//sync
	m.collectionsLock.Lock()
	defer m.collectionsLock.Unlock()

	//imp
	collection, exists := m.collectionsPartialView.KnownCollections[collectionId]
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
