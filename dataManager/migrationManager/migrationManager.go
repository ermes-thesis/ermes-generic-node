package migrationManager

import (
	shared "dataManager/shared"
	"fmt"
	"sync"
)

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
	adjacentNodes            map[string]nodeData                       //TODO maybe can be removed ~ sharing something with http
	unstableEquilibriumState map[string]map[string]*childResourceUsage //mapping resource - child
	collectionsPartialView   *shared.CollectionPartialView             //set of local and remotes collections
}

// configurable parameters
const leaderThreshold = 0
const followerThreshold = 1

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
	m.collectionsPartialView = collections
	m.unstableEquilibriumState = make(map[string]map[string]*childResourceUsage)
	m.adjacentNodes = make(map[string]nodeData)
}

// methods
func (m *MigrationManagerImp) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//sync
	m.collectionsPartialView.Mutex.Lock()
	defer m.collectionsPartialView.Mutex.Unlock()
	//imp
	if m.unstableEquilibriumState[collectionId] == nil { //first time accessing resource in general
		m.unstableEquilibriumState[collectionId] = make(map[string]*childResourceUsage)
		m.addNewRemoteCollection(collectionId, "fatherName") //TODO: change with real name
	}

	resUsage, exists := m.unstableEquilibriumState[collectionId][childName]
	if !exists { //first time accessing resource from a child
		resUsage = newChildResourceUsage()
		m.unstableEquilibriumState[collectionId][childName] = resUsage
	}

	if isWrite { //update saved info
		manageNewRequest(&resUsage.write, requestLatency)
	} else {
		manageNewRequest(&resUsage.read, requestLatency)
	}
	//take decisions
	//TODO -> QUI O NEL GOSSIP? -> meglio nel gossip
}

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
