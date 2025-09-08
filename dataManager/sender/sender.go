package sender

import (
	gossip "dataManager/messages"
	shared "dataManager/shared"
	"fmt"
	log "logger"
	"server"
)

const (
	gossipPath       = "/gossip"
	repoPath         = "/repo"
	remoteAccessPatj = "/remoteAccess"
)

type MigrationManagerSender interface {
	SendRemoteCollectionTo(string, shared.Collection, bool, map[string]string, []string, uint64)
	SendGossipMessages(gossip.GossipMessage, string)
	GossipPath() string
	RepoPath() string
}

type MigrationManagerSenderImpl struct {
	server server.ServerType
	logger log.Logger
}

func NewMigrationManagerSender(logger log.Logger, server server.ServerType) MigrationManagerSender {
	return &MigrationManagerSenderImpl{
		server: server,
		logger: logger,
	}
}

func (s *MigrationManagerSenderImpl) SendRemoteCollectionTo(destination string, collection shared.Collection, sendAsLeader bool, res map[string]string, groups []string, collectionSize uint64) {
	//imp
	s.logger.Println("SENDER", fmt.Sprintf("send collection %s to %s", collection.CollectionId(), destination), log.INFO)
	collection.AddHost(destination)

	msg := gossip.SendCollectionMessage{
		CollectionId:   collection.CollectionId(),
		Records:        res,
		Groups:         groups,
		IsStrong:       collection.IsStrong(),
		CollectionSize: collectionSize,
	}
	if collection.IsStrong() {
		msg.IsLeader = collection.(*shared.StrongCollection).IsLeader && sendAsLeader
	} else {
		msg.IsLeader = false
	}

	s.server.SendGenericHTTPRequest(destination, repoPath, msg)
}

func (s *MigrationManagerSenderImpl) SendGossipMessages(msg gossip.GossipMessage, destination string) {
	s.server.SendGenericHTTPRequest(destination, gossipPath, msg)
}

func (s *MigrationManagerSenderImpl) GossipPath() string {
	return gossipPath
}

func (s *MigrationManagerSenderImpl) RepoPath() string {
	return repoPath
}
