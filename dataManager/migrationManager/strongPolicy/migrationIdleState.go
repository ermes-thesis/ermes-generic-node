package strongPolicy

import (
	usage "dataManager/migrationManager/childUsage"
	gossipmessages "dataManager/migrationManager/gossipMessages"
)

// MigrationManagerIdleState
// - Idle state -> in this state the Migration manager can receive collection requests; ignore gossip messages
type MigrationManagerIdleState struct {
	policy *DefaultStrongCollectionPolicy
}

func (m *MigrationManagerIdleState) Setup(args ...any) {}

// methods
func (m *MigrationManagerIdleState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//sync
	m.policy.partialView.Mutex.Lock()
	defer m.policy.partialView.Mutex.Unlock()

	//imp
	if m.policy.equilibriumState[collectionId] == nil { //first time accessing resource in general
		m.policy.equilibriumState[collectionId] = make(map[string]*usage.CircularBuffer)
		//m.MigrationManagerImp.addNewRemoteCollection(collectionId, "fatherName") //TODO: change with real name
	}

	resUsage, exists := m.policy.equilibriumState[collectionId][childName]
	if !exists { //first time accessing resource from a child
		resUsage = usage.NewCircularBuffer(m.policy.currentConfig.ViewSize)
		m.policy.equilibriumState[collectionId][childName] = resUsage
	}

	if isWrite { //update saved info
		resUsage.Head().Write.ManageNewRequest(requestLatency)
	} else {
		resUsage.Head().Read.ManageNewRequest(requestLatency)
	}
}

// ManageGossipMessage if receive a gossip message during idle state directly migrate to gossip state and forward the message to that state
func (m *MigrationManagerIdleState) ManageGossipMessage(msg gossipmessages.GossipMessage) {
	m.policy.transitionToGossip()
	m.policy.currentState.ManageGossipMessage(msg)
}
