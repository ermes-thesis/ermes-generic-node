package migrationManager

import (
	gossip "dataManager/migrationManager/gossipMessages"
	"fmt"
)

// statePattern
type MigrationManagerState interface {
	//ManageNewRequestanageNewWeekRequest(string, string, int) TODO
	ManageNewStrongRequest(string, string, bool, int)
	ManageGossipMessage(msg gossip.GossipMessage)
}

func (m *MigrationManagerImp) setState(s MigrationManagerState) {
	fmt.Println("set to a new state")
	m.currentState = s
}

// transitionToGossip handles automatic transition from idle to gossip
func (m *MigrationManagerImp) transitionToGossip() {
	m.timerMutex.Lock()
	defer m.timerMutex.Unlock()

	// Only transition if still in idle state
	if _, isIdle := m.currentState.(*MigrationManagerIdleState); isIdle {
		//fmt.Println("Auto-transitioning from idle to gossip state")
		m.setState(m.gossipState)
		m.stopIdleTimer()
	}
}

func (m *MigrationManagerImp) transitionToIdle() {
	m.timerMutex.Lock()
	defer m.timerMutex.Unlock()
	// Only transition if still in decision state

	if _, isDecision := m.currentState.(*MigrationManagerDecisionState); isDecision {
		//fmt.Println("Auto-transitioning from decision to idle state")
		m.setState(m.idleState)
		m.startIdleTimer()
	}
}
