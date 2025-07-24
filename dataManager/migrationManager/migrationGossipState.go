package migrationManager

import gossipmessages "dataManager/migrationManager/gossipMessages"

// - Gossip state -> in this state the Migration manager can receive gossip messages and place in a stack collection requests
type MigrationManagerGossipState struct {
	MigrationManagerImp *MigrationManagerImp
}

// methods
func (m *MigrationManagerGossipState) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	//TODO
}

func (m *MigrationManagerGossipState) ManageGossipMessage(msg gossipmessages.GossipMessage) {
	//TODO -> save data
}
