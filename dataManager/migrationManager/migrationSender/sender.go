package migrationSender

import (
	gossip "dataManager/migrationManager/gossipMessages"
	shared "dataManager/shared"
	log "logger"
	server "server"
)

const (
	gossipPath = "/gossip"
	repoPath   = "/repo"
)

type Sender interface {
	SendRemoteCollectionTo(string, shared.Collection, bool, map[string]string, []string)
	SendGosssipMessages(gossip.GossipMessage, string)
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

func (s *SendImpl) SendRemoteCollectionTo(destination string, collection shared.Collection, sendAsLeader bool, res map[string]string, groups []string) {
	//imp
	collection.AddHost(destination)

	msg := gossip.SendCollectionMessage{
		CollectionId: collection.CollectionId(),
		Res:          res,
		Groups:       groups,
		IsStrong:     collection.IsStrong(),
	}
	if collection.IsStrong() {
		msg.IsLeader = collection.(*shared.StrongCollection).IsLeader && sendAsLeader
	} else {
		msg.IsLeader = false
	}

	s.server.SendGenericHTTPRequest(destination, repoPath, msg)
}

func (s *SendImpl) SendGosssipMessages(msg gossip.GossipMessage, destination string) {
	s.server.SendGenericHTTPRequest(destination, gossipPath, msg)
}

func (s *SendImpl) GossipPath() string {
	return gossipPath
}

func (s *SendImpl) RepoPath() string {
	return repoPath
}
