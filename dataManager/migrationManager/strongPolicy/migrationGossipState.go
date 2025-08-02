package strongPolicy

import (
	gossip "dataManager/migrationManager/gossipMessages"
	"db"
	"fmt"
	"logger"
)

// - Gossip state -> in this state the Migration manager can receive gossip messages and place in a stack collection requests
type MigrationManagerGossipState struct {
	policy                *DefaultStrongCollectionPolicy
	dbInterface           db.GenericDB
	MissingMessageCounter int
	NodeResourceUsage     map[string]NodeStateInfo
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
	m.NodeResourceUsage = make(map[string]NodeStateInfo, 0)

	go m.SendGosssipMessages()
}

// methods
func (m *MigrationManagerGossipState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	m.policy.idleState.ManageNewStrongRequest(collectionId, childName, isWrite, requestLatency)
}

func (m *MigrationManagerGossipState) SendGosssipMessages() {
	memStats, err := m.dbInterface.GetMemoryStats()
	if err != nil {
		m.policy.logger.Println("GOSSIP-STATE", fmt.Sprintf("failed to get memory stats: %v", err), logger.ERROR)
	}
	redisFreeSize := 1.0 - memStats.UsedMemoryDatasetPercentage

	for value := range m.policy.serverInfo.Children {
		msgToChild := gossip.NewLeaderDelegaMessage(m.policy.serverInfo.Address, redisFreeSize)
		m.policy.sender.SendGosssipMessages(msgToChild, m.policy.serverInfo.Children[value].Address)
	}

	if m.policy.serverInfo.Father != nil {
		msgToFather := gossip.NewChildStateMessage(m.policy.serverInfo.Address, redisFreeSize)
		m.policy.sender.SendGosssipMessages(msgToFather, m.policy.serverInfo.Father.Address)
	}
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
		m.policy.transitionToDecision(m.NodeResourceUsage)
	}
}

// save memory and state info from the child in order to take further decisions
func (m *MigrationManagerGossipState) manageChildGossipMessage(msg gossip.ChildStateMessage) {
	childStateInfo := NodeStateInfo{
		//childName:   msg.GetSenderID(),
		FreeRedisMemory: msg.FreeRedisMemory,
	}
	m.NodeResourceUsage[msg.GetSenderID()] = childStateInfo
}

func (m *MigrationManagerGossipState) manageAnchestorGossipMessage(msg gossip.LeaderDelegaMessage) {
	childStateInfo := NodeStateInfo{
		//childName:   msg.GetSenderID(),
		FreeRedisMemory: msg.FreeRedisMemory,
	}
	m.NodeResourceUsage[msg.GetSenderID()] = childStateInfo
}
