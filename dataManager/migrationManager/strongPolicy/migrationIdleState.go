package strongPolicy

import (
	gossipmessages "dataManager/messages"
	usage "dataManager/migrationManager/childUsage"
)

// MigrationManagerIdleState
// - Idle state -> in this state the Migration manager can receive collection requests; ignore gossip messages
type MigrationManagerIdleState struct {
	policy *ErmesStrongCollectionPolicy
}

func (m *MigrationManagerIdleState) Setup(args ...any) {}

// methods
func (m *MigrationManagerIdleState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//sync
	m.policy.partialView.Mutex.Lock()
	defer m.policy.partialView.Mutex.Unlock()

	//imp
	innerAny, ok := m.policy.equilibriumState.Load(collectionId)
	if !ok { //first time accessing resource in general
		m.policy.equilibriumState.Store(collectionId, make(map[string]*usage.CircularBuffer))
		innerAny, _ = m.policy.equilibriumState.Load(collectionId)
		//m.MigrationManagerImp.addNewRemoteCollection(collectionId, "fatherName") //TODO: change with real name
	}
	innerMap := innerAny.(map[string]*usage.CircularBuffer)
	resUsage, exists := innerMap[childName]
	if !exists { //first time accessing resource from a child
		resUsage = usage.NewCircularBuffer(m.policy.currentConfig.ViewSize)
		innerMap[childName] = resUsage
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
