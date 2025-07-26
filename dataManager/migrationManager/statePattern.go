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
	Setup(args ...any)
}

type childStateInfo struct {
	MemoryUsage float64
	DiskUsage   float64
}

func (m *MigrationManagerImp) setState(s MigrationManagerState) {
	m.currentState = s
}

// transitionToGossip handles automatic transition from idle to gossip
func (m *MigrationManagerImp) transitionToGossip() {
	m.timerMutex.Lock()
	defer m.timerMutex.Unlock()

	// Only transition if still in idle state
	if _, isIdle := m.currentState.(*MigrationManagerIdleState); isIdle {
		fmt.Println("Auto-transitioning from idle to gossip state")
		m.gossipState.Setup(len(m.serverInfo.Children) + 1) //number of adjacent nodes - TODO: manage cloud nodes.
		m.setState(m.gossipState)
		m.stopIdleTimer()
	}
}

func (m *MigrationManagerImp) transitionToDecision(childrenResourceUsage map[string]childStateInfo) {
	// Only transition if still in gossip state
	if _, isGossip := m.currentState.(*MigrationManagerGossipState); isGossip {
		fmt.Println("Auto-transitioning from gossip to decision state")
		m.decisionState.Setup(childrenResourceUsage)
		m.setState(m.decisionState)
	}
}

func (m *MigrationManagerImp) transitionToIdle() {
	m.timerMutex.Lock()
	defer m.timerMutex.Unlock()
	// Only transition if still in decision state

	if _, isDecision := m.currentState.(*MigrationManagerDecisionState); isDecision {
		fmt.Println("Auto-transitioning from decision to idle state")
		m.setState(m.idleState)
		m.startIdleTimer()
	}
}
