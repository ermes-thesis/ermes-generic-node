package migrationManager

import (
	gossip "dataManager/migrationManager/gossipMessages"
	"fmt"
)

// - Gossip state -> in this state the Migration manager can receive gossip messages and place in a stack collection requests
type MigrationManagerGossipState struct {
	MigrationManagerImp   *MigrationManagerImp
	missingMessageCounter int
	childrenResourceUsage map[string]childStateInfo
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
	m.childrenResourceUsage = make(map[string]childStateInfo, 0)

	for value := range m.MigrationManagerImp.serverInfo.Children {
		msgToChild := gossip.NewLeaderDelegaMessage(m.MigrationManagerImp.serverInfo.Address)
		fmt.Println(value, msgToChild)
		//TODO:= send message to child
	}

	if m.MigrationManagerImp.serverInfo.Father != nil {
		//TODO:= measure mem. state and send to father
		//msgToFather := gossip.NewChildStateMessage(m.MigrationManagerImp.serverInfo.Name, )
	}

}

// methods
func (m *MigrationManagerGossipState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	m.MigrationManagerImp.idleState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency)
}

func (m *MigrationManagerGossipState) ManageGossipMessage(msg gossip.GossipMessage) {
	//sync
	lock.Lock()
	defer lock.Unlock()

	//imp
	switch parsed := msg.(type) {
	case *gossip.ChildStateMessage:
		m.manageChildGossipMessage(*parsed)
	case *gossip.LeaderDelegaMessage:
		m.manageAnchestorGossipMessage(*parsed)
	default:
		fmt.Println("Unknown message type:", msg.GetMessageType())
		return
	}

	m.missingMessageCounter -= 1
	if m.missingMessageCounter <= 0 {
		m.MigrationManagerImp.transitionToDecision(m.childrenResourceUsage)
	}
}

// save memory and state info from the child in order to take further decisions
func (m *MigrationManagerGossipState) manageChildGossipMessage(msg gossip.ChildStateMessage) {
	childStateInfo := childStateInfo{
		//childName:   msg.GetSenderID(),
		MemoryUsage: msg.MemoryUsage,
		DiskUsage:   msg.DiskUsage,
	}
	m.childrenResourceUsage[msg.GetSenderID()] = childStateInfo
}

func (m *MigrationManagerGossipState) manageAnchestorGossipMessage(msg gossip.LeaderDelegaMessage) {
	//TODO
}
