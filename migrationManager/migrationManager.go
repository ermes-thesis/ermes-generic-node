package migrationManager

import (
	"fmt"
	"sync"
)

type MigrationManager interface {
	//used to update lru in real time (repoId, childName, request cumulative latency)
	ManageNewRequestanageNewWeekRequest(string, string, int)
	//used to update forces in real time (repoId, childName, write?, request cumulative latency)
	ManageNewStrongRequest(string, string, bool, int)
	//used to update non real time info
	ManageGossipMessage() //TODO
}

type MigrationManagerImp struct {
	adjacentNodes map[string]nodeData
	resourcesData map[string]resourceData
	//mapping resource - child
	unstableEquilibriumState map[string]map[string]childResourceUsage
}

// configurable parameters
const leaderThreshold = 0
const followerThreshold = 1

// singleton pattern
var migrationManagareInstance *MigrationManagerImp
var lock = &sync.Mutex{}

func GetMigrationManagerIstance() *MigrationManagerImp {
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

// methods
func (m *MigrationManagerImp) ManageNewStrongRequest(repoId string, childName string, isWrite bool, requestLatency int) {
	var resUsage = m.unstableEquilibriumState[repoId][childName]

	//if first access to resource -> add to map
	if m.unstableEquilibriumState[repoId] == nil {
		//TODO add new resourcesData
		resUsage = newChildResourceUsage()
		m.unstableEquilibriumState[repoId][childName] = resUsage
	}

	//update saved info
	if isWrite {
		manageNewRequest(resUsage.write, requestLatency)
	} else {
		manageNewRequest(resUsage.read, requestLatency)
	}

	//take decisions
	//TODO -> QUI O NEL GOSSIP?
}
