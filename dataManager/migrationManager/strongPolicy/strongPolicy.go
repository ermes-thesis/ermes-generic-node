package strongPolicy

import (
	gossip "dataManager/migrationManager/gossipMessages"
)

type StrongCollectionPolicy interface {
	SetDefaultState()
	ManageGossipMessage(msg gossip.GossipMessage)
	RemoveInfo(collectionId string)
	ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int)
}

type CurrentPolicy int

const (
	Pending CurrentPolicy = iota
	DefaultPolicy
)
