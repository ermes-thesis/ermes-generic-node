package migrationSender

import (
	gossip "dataManager/migrationManager/gossipMessages"
	shared "dataManager/shared"
	"fmt"
	log "logger"
	"server"
)

const (
	gossipPath = "/gossip"
	repoPath   = "/repo"
)

type Sender interface {
	SendRemoteCollectionTo(string, shared.Collection, bool, map[string]string, []string, uint64)
	SendGossipMessages(gossip.GossipMessage, string)
	GossipPath() string
	RepoPath() string
}

type SendImpl struct {
	server server.ServerType
	logger log.Logger
}

func NewSender(logger log.Logger, server server.ServerType) Sender {
	return &SendImpl{
		server: server,
		logger: logger,
	}
}

func (s *SendImpl) SendRemoteCollectionTo(destination string, collection shared.Collection, sendAsLeader bool, res map[string]string, groups []string, collectionSize uint64) {
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

func (s *SendImpl) SendGossipMessages(msg gossip.GossipMessage, destination string) {
	s.server.SendGenericHTTPRequest(destination, gossipPath, msg)
}

func (s *SendImpl) GossipPath() string {
	return gossipPath
}

func (s *SendImpl) RepoPath() string {
	return repoPath
}
