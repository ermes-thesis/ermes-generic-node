package migrationManager

import (
	gossip "dataManager/migrationManager/gossipMessages"
	"fmt"
)

// - Gossip state -> in this state the Migration manager can receive gossip messages and place in a stack collection requests
type MigrationManagerGossipState struct {
	MigrationManagerImp   *MigrationManagerImp
	missingMessageCounter int
}

func (m *MigrationManagerGossipState) Setup(args ...any) {
	if len(args) != 1 {
		panic("invalid arguments for Setup in GossipState")
	}
	counter, ok := args[0].(int)
	if !ok {
		panic("expected int for counter")
	}
	m.missingMessageCounter = counter
}

// methods
func (m *MigrationManagerGossipState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//TODO
}

func (m *MigrationManagerGossipState) ManageGossipMessage(msg gossip.GossipMessage) {
	m.MigrationManagerImp.timerMutex.Lock()
	defer m.MigrationManagerImp.timerMutex.Unlock()

	switch parsed := msg.(type) {
	case *gossip.ChildStateMessage:
		m.manageChildGossipMessage(*parsed)
	case *gossip.LeaderDelegaMessage:
		m.manageAnchestorGossipMessage(*parsed)
	default:
		fmt.Println("Unknown message type:", msg.GetMessageType())
	}

	m.missingMessageCounter -= 1
	if m.missingMessageCounter <= 0 {
		m.MigrationManagerImp.transitionToIdle()
	}
}

func (m *MigrationManagerGossipState) manageChildGossipMessage(msg gossip.ChildStateMessage) {
	//TODO
}

func (m *MigrationManagerGossipState) manageAnchestorGossipMessage(msg gossip.LeaderDelegaMessage) {
	//TODO
}
