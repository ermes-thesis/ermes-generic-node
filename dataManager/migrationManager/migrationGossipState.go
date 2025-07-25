package migrationManager

import (
	gossip "dataManager/migrationManager/gossipMessages"
	"fmt"
)

// - Gossip state -> in this state the Migration manager can receive gossip messages and place in a stack collection requests
type MigrationManagerGossipState struct {
	MigrationManagerImp   *MigrationManagerImp
	missingMessageCounter int
	childrenResourceUsage []childStateInfo
}

type childStateInfo struct {
	childName   string
	MemoryUsage float64
	DiskUsage   float64
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
	m.childrenResourceUsage = make([]childStateInfo, 0)

	fmt.Println("Auto-transitioning from decision to idle state")

	for value := range m.MigrationManagerImp.serverInfo.Children {
		msgToChild := gossip.NewLeaderDelegaMessage(m.MigrationManagerImp.serverInfo.Name)
		fmt.Println(value, msgToChild)
		//TODO:= send message to child
	}

	if m.MigrationManagerImp.serverInfo.Father != "nil" {
		//TODO:= measure mem. state and send to father
		//msgToFather := gossip.NewChildStateMessage(m.MigrationManagerImp.serverInfo.Name, )
	}

}

// methods
func (m *MigrationManagerGossipState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
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
		childName:   msg.GetSenderID(),
		MemoryUsage: msg.MemoryUsage,
		DiskUsage:   msg.DiskUsage,
	}
	m.childrenResourceUsage = append(m.childrenResourceUsage, childStateInfo)
}

func (m *MigrationManagerGossipState) manageAnchestorGossipMessage(msg gossip.LeaderDelegaMessage) {
	//TODO
}
