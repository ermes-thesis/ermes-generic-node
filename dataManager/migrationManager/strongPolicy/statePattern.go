package strongPolicy

import (
	gossip "dataManager/migrationManager/gossipMessages"
)

// statePattern
type MigrationManagerState interface {
	//ManageNewRequestanageNewWeekRequest(string, string, int) TODO
	ManageNewStrongRequest(string, string, bool, int)
	ManageGossipMessage(msg gossip.GossipMessage)
	Setup(args ...any)
}

type ChildStateInfo struct {
	MemoryUsage float64
	DiskUsage   float64
}
