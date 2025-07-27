package migrationManager

import (
	"config"
	lru "dataManager/lru"
	gossip "dataManager/migrationManager/gossipMessages"
	shared "dataManager/shared"
	info "server/info"
	"sync"
	"time"
)

type PenalityFunc func(int, int) int //must be overwritten
type MigrationManager interface {
	//inizialize thje singleto pattern
	Init(*shared.CollectionPartialView, PenalityFunc, config.MigrationManagerConfig, info.ServerInfo, *lru.LRUCache[shared.Collection])
	// used to update lru in real time (collectionID)
	// if the collection is strong used to update forces in real time (collectionID, nodeName, write?, request cumulative latency).
	// if the request comes from a user => nodeName = "user"
	ManageNewRequest(string, string, bool, int)
	//used to update non real time info
	ManageGossipMessage(gossip.GossipMessage)
	//used when the node receive  a collection from one of the directly connected nodes (collectionID,size,isStrong)
	//return True if the collection  is already stored locally and must not be stored in Redis
	NewCollectionReceived(string, int, bool)
}

type MigrationManagerImp struct {
	//config
	config     config.MigrationManagerConfig
	serverInfo info.ServerInfo

	//state
	currentState MigrationManagerState

	idleState     MigrationManagerState
	gossipState   MigrationManagerState
	decisionState MigrationManagerState

	//timing for auto transition
	stateTimer *time.Timer
	timerMutex sync.Mutex

	//impl
	unstableEquilibriumState map[string]map[string]*CircularBuffer //mapping resource - child
	collectionsPartialView   *shared.CollectionPartialView         //set of local and remotes collections

	lruCache *lru.LRUCache[shared.Collection]

	//used to define a penalization factor for past data -> func(bufferPos int, currentValue int)
	penalityFunc PenalityFunc
}

// singleton pattern
var migrationManagareInstance *MigrationManagerImp
var lock = &sync.Mutex{}

//TODO: remove from global var

func GetMigrationManagerInstance() *MigrationManagerImp {
	if migrationManagareInstance == nil {
		lock.Lock()
		defer lock.Unlock()
		if migrationManagareInstance == nil {
			//fmt.Println("Creating single instance now.")
			migrationManagareInstance = &MigrationManagerImp{}
		}
	}
	return migrationManagareInstance
}

func (m *MigrationManagerImp) Init(collections *shared.CollectionPartialView, penalityFunc PenalityFunc, config config.MigrationManagerConfig, serverInfo info.ServerInfo, lruCache *lru.LRUCache[shared.Collection]) {
	//sync
	lock.Lock()
	defer lock.Unlock()
	//imp
	if m.collectionsPartialView != nil { //if singleton allready inizialized
		return
	}
	m.serverInfo = serverInfo
	m.lruCache = lruCache
	m.config = config
	m.penalityFunc = penalityFunc
	//init states
	m.idleState = &MigrationManagerIdleState{m}
	m.gossipState = &MigrationManagerGossipState{m, 0, make(map[string]childStateInfo, 0)}
	m.decisionState = &MigrationManagerDecisionState{m, make(map[string]childStateInfo, 0)}

	m.currentState = m.idleState

	//start timer
	m.startIdleTimer()

	//init data
	m.collectionsPartialView = collections
	m.unstableEquilibriumState = make(map[string]map[string]*CircularBuffer)
}

// methods
func (m *MigrationManagerImp) NewCollectionReceived(collectionId string, collectionSize int, isStrong bool) bool {
	collection := m.collectionsPartialView.KnowedCollections[collectionId]
	if collection != nil {
		if collection.IsLocal() {
			return true
			//TODO probably for weak collections conflict must be solved in this case
		}
		collection.SetToLocal(collectionSize)
		return false
	}
	if isStrong {
		m.collectionsPartialView.KnowedCollections[collectionId] = shared.NewStrongCollection(collectionId, collectionSize, false) //TODO leader?
	} else {
		m.collectionsPartialView.KnowedCollections[collectionId] = shared.NewWeakCollection(collectionId, collectionSize, false)
	}
	return false
}

func (m *MigrationManagerImp) ManageGossipMessage(msg gossip.GossipMessage) {
	m.currentState.ManageGossipMessage(msg)
}

func (m *MigrationManagerImp) ManageNewRequest(collectionId string, nodeName string, isWrite bool, requestLatency int) { //error{
	//if no local info on the collection
	if m.collectionsPartialView.KnowedCollections[collectionId] == nil || !m.collectionsPartialView.KnowedCollections[collectionId].IsLocal() {
		return //lancia errore
	}

	//place on top in the LRU
	m.lruCache.Put(collectionId, m.collectionsPartialView.KnowedCollections[collectionId], uint(m.collectionsPartialView.KnowedCollections[collectionId].GetCollectionState().(*shared.LocalCollection).Size()))

	//if the collection is strong update forces
	if m.collectionsPartialView.KnowedCollections[collectionId].IsStrong() {
		m.currentState.ManageNewStrongRequest(collectionId, nodeName, isWrite, requestLatency)
	}
}
