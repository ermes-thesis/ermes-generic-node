package strongPolicy

import (
	gossip "dataManager/migrationManager/gossipMessages"
	"fmt"
)

// - Gossip state -> in this state the Migration manager can receive gossip messages and place in a stack collection requests
type MigrationManagerGossipState struct {
	policy                *DefaultStrongCollectionPolicy
	MissingMessageCounter int
	ChildrenResourceUsage map[string]ChildStateInfo
}

func (m *MigrationManagerGossipState) Setup(args ...any) {
	if len(args) != 1 {
		panic("invalid arguments for Setup in GossipState")
	}
	counter, ok := args[0].(int)
	if !ok {
		panic("expected int for counter")
	}
	m.MissingMessageCounter = counter
	m.ChildrenResourceUsage = make(map[string]ChildStateInfo, 0)

	for value := range m.policy.serverInfo.Children {
		msgToChild := gossip.NewLeaderDelegaMessage(m.policy.serverInfo.Address)
		fmt.Println(value, msgToChild)
		//TODO:= send message to child
	}

	if m.policy.serverInfo.Father != nil {
		//TODO:= measure mem. state and send to father
		//msgToFather := gossip.NewChildStateMessage(m.MigrationManagerImp.serverInfo.Name, )
	}

}

// methods
func (m *MigrationManagerGossipState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	m.policy.idleState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency)
}

func (m *MigrationManagerGossipState) ManageGossipMessage(msg gossip.GossipMessage) {
	//sync
	m.policy.migLock.Lock()
	defer m.policy.migLock.Unlock()

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

	m.MissingMessageCounter -= 1
	if m.MissingMessageCounter <= 0 {
		m.policy.transitionToDecision(m.ChildrenResourceUsage)
	}
}

// save memory and state info from the child in order to take further decisions
func (m *MigrationManagerGossipState) manageChildGossipMessage(msg gossip.ChildStateMessage) {
	childStateInfo := ChildStateInfo{
		//childName:   msg.GetSenderID(),
		MemoryUsage: msg.MemoryUsage,
		DiskUsage:   msg.DiskUsage,
	}
	m.ChildrenResourceUsage[msg.GetSenderID()] = childStateInfo
}

func (m *MigrationManagerGossipState) manageAnchestorGossipMessage(msg gossip.LeaderDelegaMessage) {
	//TODO
}
