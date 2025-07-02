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
}

type MigrationManagerImp struct {
	//sync
	mutex sync.Mutex
	//data
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
	m.mutex.Lock()
	defer m.mutex.Unlock()
	//imp
	if m.collectionsPartialView != nil { //if singleton allready inizialized
		return
	}
	m.collectionsPartialView = collections
	m.unstableEquilibriumState = make(map[string]map[string]*childResourceUsage)
	m.adjacentNodes = make(map[string]nodeData)
}

// methods
func (m *MigrationManagerImp) ManageNewStrongRequest(repoId string, childName string, isWrite bool, requestLatency int) {
	//sync
	m.mutex.Lock()
	defer m.mutex.Unlock()
	//imp
	if m.unstableEquilibriumState[repoId] == nil { //first time accessing resource in general
		m.unstableEquilibriumState[repoId] = make(map[string]*childResourceUsage)
		//TODO add to collection partial view
	}

	resUsage, exists := m.unstableEquilibriumState[repoId][childName]
	if !exists { //first time accessing resource from a child
		resUsage = newChildResourceUsage()
		m.unstableEquilibriumState[repoId][childName] = resUsage
	}

	if isWrite { //update saved info
		manageNewRequest(&resUsage.write, requestLatency)
	} else {
		manageNewRequest(&resUsage.read, requestLatency)
	}
	//take decisions
	//TODO -> QUI O NEL GOSSIP? -> meglio nel gossip
}
