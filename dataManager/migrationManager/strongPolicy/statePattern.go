package strongPolicy

import (
	gossip "dataManager/migrationManager/gossipMessages"
)

// MigrationManagerState statePattern
type MigrationManagerState interface {
	//ManageNewRequestanageNewWeekRequest(string, string, int) TODO
	ManageNewStrongRequest(string, string, bool, int)
	ManageGossipMessage(msg gossip.GossipMessage)
	Setup(args ...any)
}

type NodeStateInfo struct {
	FreeRedisMemory float64
}
