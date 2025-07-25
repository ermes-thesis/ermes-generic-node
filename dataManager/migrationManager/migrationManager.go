package migrationManager

import (
	gossip "dataManager/migrationManager/gossipMessages"
	shared "dataManager/shared"
	"server/info"
	"sync"
	"time"

	"config"
)

type PenalityFunc func(int, int) int //must be overwritten
type MigrationManager interface {
	//used to update lru in real time (collectionID, childName, request cumulative latency)
	ManageNewRequestanageNewWeekRequest(string, string, int)
	//used to update forces in real time (collectionID, childName, write?, request cumulative latency)
	ManageNewStrongRequest(string, string, bool, int)
	//used to update non real time info
	ManageChildGossipMessage()     //TODO
	ManageAnchestorGossipMessage() //TODO
	//used when the node receive  a collection from one of the directly connected nodes (collectionID,size)
	//return True if the collection  is already stored locally and must not be stored in Redis
	NewCollectionReceived(string, int, bool)
}

type MigrationManagerImp struct {
	//config
	config     config.MigrationManagerConfig
	serverInfo info.Serverinfo

	//state
	currentState MigrationManagerState

	idleState     MigrationManagerState
	gossipState   MigrationManagerState
	decisionState MigrationManagerState

	//timing for auto transition
	lastActivityTime time.Time
	stateTimer       *time.Timer
	timerMutex       sync.Mutex

	//impl
	unstableEquilibriumState map[string]map[string]*CircularBuffer //mapping resource - child
	collectionsPartialView   *shared.CollectionPartialView         //set of local and remotes collections

	//used to define a penalization factor for past data -> func(bufferPos int, currentValue int)
	penalityFunc PenalityFunc
}

// singleton pattern
var migrationManagareInstance *MigrationManagerImp
var lock = &sync.Mutex{}

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

func (m *MigrationManagerImp) Init(collections *shared.CollectionPartialView, penalityFunc PenalityFunc, config config.MigrationManagerConfig, serverInfo info.Serverinfo) {
	//sync
	lock.Lock()
	defer lock.Unlock()
	//imp
	if m.collectionsPartialView != nil { //if singleton allready inizialized
		return
	}
	m.serverInfo = serverInfo
	m.config = config
	m.penalityFunc = penalityFunc
	//init states
	m.idleState = &MigrationManagerIdleState{m}
	m.gossipState = &MigrationManagerGossipState{m, 0, make([]childStateInfo, 0)}
	m.decisionState = &MigrationManagerDecisionState{m}

	m.currentState = m.idleState

	//start timer
	m.startIdleTimer()

	//init data
	m.collectionsPartialView = collections
	m.unstableEquilibriumState = make(map[string]map[string]*CircularBuffer)
}

// methods
func (m *MigrationManagerImp) addNewRemoteCollection(collectionId string, hostId string, isStrong bool) {
	//TODO
}

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

func (m *MigrationManagerImp) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	m.currentState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency)
}

func (m *MigrationManagerImp) ManageGossipMessage(msg gossip.GossipMessage) {
	m.currentState.ManageGossipMessage(msg)
}
