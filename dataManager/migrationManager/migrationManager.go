package migrationManager

import (
	shared "dataManager/shared"
	"fmt"
	"sync"
)

// configurable parameters
const leaderThreshold = 0
const followerThreshold = 1
const viewSize = 3

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
	NewCollectionReceived(string, int)
}

type MigrationManagerImp struct {
	//state
	currentState MigrationManagerState

	idleState     MigrationManagerState
	gossipState   MigrationManagerState
	decisionState MigrationManagerState
	//impl
	adjacentNodes            map[string]nodeData                       //TODO maybe can be removed ~ sharing something with http
	unstableEquilibriumState map[string]map[string]*childResourceUsage //mapping resource - child
	collectionsPartialView   *shared.CollectionPartialView             //set of local and remotes collections
}

// singleton pattern
var migrationManagareInstance *MigrationManagerImp
var lock = &sync.Mutex{}

func GetMigrationManagerInstance() *MigrationManagerImp {
	if migrationManagareInstance == nil {
		lock.Lock()
		defer lock.Unlock()
		if migrationManagareInstance == nil {
			fmt.Println("Creating single instance now.")
			migrationManagareInstance = &MigrationManagerImp{}
		}
	}
	return migrationManagareInstance
}

func (m *MigrationManagerImp) Init(collections *shared.CollectionPartialView) {
	//sync
	lock.Lock()
	defer lock.Unlock()
	//imp
	if m.collectionsPartialView != nil { //if singleton allready inizialized
		return
	}
	m.idleState = &MigrationManagerIdleState{m}
	m.gossipState = &MigrationManagerGossipState{m}
	m.decisionState = &MigrationManagerDecisionState{m}
	//TODO -> inizializzo gli altri due stati
	m.currentState = m.idleState

	m.collectionsPartialView = collections
	m.unstableEquilibriumState = make(map[string]map[string]*childResourceUsage)
	m.adjacentNodes = make(map[string]nodeData)
}

// methods
func (m *MigrationManagerImp) addNewRemoteCollection(collectionId string, hostId string) {
	hosts := make(map[string]struct{})
	hosts[hostId] = struct{}{}
	remote := shared.NewRemoteCollection(collectionId, hosts)
	m.collectionsPartialView.KnowedCollections[collectionId] = remote
}

func (m *MigrationManagerImp) NewCollectionReceived(collectionId string, collectionSize int) bool {
	collection := m.collectionsPartialView.KnowedCollections[collectionId]
	if collection != nil {
		if collection.IsLocal() {
			return true
			//TODO probably for weak collections conflict must be solved in this case
		}
		hostsId := collection.HostsId()
		delete(hostsId, "fatherName") //TODO: change with real name
		m.collectionsPartialView.KnowedCollections[collectionId] = shared.NewLocalCollection(collectionId, collectionSize, hostsId)
		return false
	}
	m.collectionsPartialView.KnowedCollections[collectionId] = shared.NewLocalCollection(collectionId, collectionSize, make(map[string]struct{}))
	return false
}

func (m *MigrationManagerImp) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	m.currentState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency)
}

// statePattern
type MigrationManagerState interface {
	//ManageNewRequestanageNewWeekRequest(string, string, int) TODO
	ManageNewStrongRequest(string, string, bool, int)
	//ManageChildGossipMessage()     //TODO
	//ManageAnchestorGossipMessage() //TODO
}

// - Idel state -> in this state the Migration manager can receive collection requests; ignore gossip messages
type MigrationManagerIdleState struct {
	MigrationManagerImp *MigrationManagerImp
}

// methods
func (m *MigrationManagerIdleState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//sync
	m.MigrationManagerImp.collectionsPartialView.Mutex.Lock()
	defer m.MigrationManagerImp.collectionsPartialView.Mutex.Unlock()
	//imp
	if m.MigrationManagerImp.unstableEquilibriumState[collectionId] == nil { //first time accessing resource in general
		m.MigrationManagerImp.unstableEquilibriumState[collectionId] = make(map[string]*childResourceUsage)
		m.MigrationManagerImp.addNewRemoteCollection(collectionId, "fatherName") //TODO: change with real name
	}

	resUsage, exists := m.MigrationManagerImp.unstableEquilibriumState[collectionId][childName]
	if !exists { //first time accessing resource from a child
		resUsage = newChildResourceUsage()
		m.MigrationManagerImp.unstableEquilibriumState[collectionId][childName] = resUsage
	}

	if isWrite { //update saved info
		manageNewRequest(&resUsage.write, requestLatency)
	} else {
		manageNewRequest(&resUsage.read, requestLatency)
	}
	//take decisions
	//TODO -> QUI O NEL GOSSIP? -> meglio nel gossip
}

// - Gossip state -> in this state the Migration manager can receive gossip messages and place in a stack collection requests
type MigrationManagerGossipState struct {
	MigrationManagerImp *MigrationManagerImp
}

// methods
func (m *MigrationManagerGossipState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//TODO
}

// - Decision state -> in this state the Migration manager ingore all the messages, take decisions on collection migration; consume the stack
type MigrationManagerDecisionState struct {
	MigrationManagerImp *MigrationManagerImp
}

// methods
func (m *MigrationManagerDecisionState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//TODO
}
