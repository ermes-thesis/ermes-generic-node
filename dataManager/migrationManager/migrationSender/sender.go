package migrationSender

import (
	gossip "dataManager/migrationManager/gossipMessages"
	shared "dataManager/shared"
	log "logger"
)

type Sender interface {
	SendRemoteCollectionTo(string, shared.Collection, map[string]string, []string)
	SendGosssipMessages(gossip.GossipMessage, string)
}

type SendImpl struct {
	logger log.Logger
}

func NewSender(logger log.Logger) Sender {
	return &SendImpl{
		logger: logger,
	}
}

func (s *SendImpl) SendRemoteCollectionTo(nodeName string, collection shared.Collection, res map[string]string, groups []string) {
	//sync -> avoid send a collection if used
	collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Lock()
	defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()

	//imp
	collection.AddHost(nodeName)
	//TODO send to node
}

func (s *SendImpl) SendGosssipMessages(msg gossip.GossipMessage, destination string) {
	json, err := msg.ToJSON()
	if err != nil {
		s.logger.Println("SENDER", "can't parse gossip message to json", log.ERROR)
		return
	}

	print(json)
	//TODO send to node
}
