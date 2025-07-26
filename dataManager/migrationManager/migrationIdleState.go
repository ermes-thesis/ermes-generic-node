package migrationManager

import (
	gossipmessages "dataManager/migrationManager/gossipMessages"
	"time"
)

// - Idle state -> in this state the Migration manager can receive collection requests; ignore gossip messages
type MigrationManagerIdleState struct {
	MigrationManagerImp *MigrationManagerImp
}

func (s *MigrationManagerIdleState) Setup(args ...any) {}

// methods
func (m *MigrationManagerIdleState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	if m.MigrationManagerImp.collectionsPartialView.KnowedCollections[collectionId] == nil || !m.MigrationManagerImp.collectionsPartialView.KnowedCollections[collectionId].IsStrong() {
		//if no local info on the collection of if the collection is not of strong type
		return
	}

	//sync
	m.MigrationManagerImp.collectionsPartialView.Mutex.Lock()
	defer m.MigrationManagerImp.collectionsPartialView.Mutex.Unlock()

	//imp
	if m.MigrationManagerImp.unstableEquilibriumState[collectionId] == nil { //first time accessing resource in general
		m.MigrationManagerImp.unstableEquilibriumState[collectionId] = make(map[string]*CircularBuffer)
		//m.MigrationManagerImp.addNewRemoteCollection(collectionId, "fatherName") //TODO: change with real name
	}

	resUsage, exists := m.MigrationManagerImp.unstableEquilibriumState[collectionId][childName]
	if !exists { //first time accessing resource from a child
		resUsage = NewCircularBuffer(m.MigrationManagerImp.config.ViewSize)
		m.MigrationManagerImp.unstableEquilibriumState[collectionId][childName] = resUsage
		resUsage.Add(NewChildResourceUsage())
	}
	if resUsage.Data[resUsage.Head] == nil {
		resUsage.Add(NewChildResourceUsage())
	}

	if isWrite { //update saved info
		manageNewRequest(&resUsage.Data[resUsage.Head].write, requestLatency)
	} else {
		manageNewRequest(&resUsage.Data[resUsage.Head].read, requestLatency)
	}
}

// if receive a gossip message during idle state directly migrate to gossip state and forward the message to that state
func (m *MigrationManagerIdleState) ManageGossipMessage(msg gossipmessages.GossipMessage) {
	m.MigrationManagerImp.transitionToGossip()
	m.MigrationManagerImp.currentState.ManageGossipMessage(msg)
}

// startIdleTimer starts or restarts the timer for idle->gossip transition
func (m *MigrationManagerImp) startIdleTimer() {
	// Cancel existing timer if any
	if m.stateTimer != nil {
		m.stateTimer.Stop()
	}

	// Only start timer if currently in idle state
	if _, isIdle := m.currentState.(*MigrationManagerIdleState); isIdle {
		duration := time.Duration(m.config.IdleTimeoutMs) * time.Millisecond
		m.stateTimer = time.AfterFunc(duration, func() {
			m.transitionToGossip()
		})
	}
}

func (m *MigrationManagerImp) stopIdleTimer() {
	if m.stateTimer != nil {
		m.stateTimer.Stop()
		m.stateTimer = nil
	}
}

// resetIdleTimer resets the idle timer when activity occurs
func (m *MigrationManagerImp) resetIdleTimer() {
	m.startIdleTimer()
}
