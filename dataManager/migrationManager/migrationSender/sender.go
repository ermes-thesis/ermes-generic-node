package migrationSender

import (
	shared "dataManager/shared"
)

type Sender interface {
	SendRemoteCollectionTo(string, shared.Collection, map[string]string, []string)
}

type SendImpl struct{}

func (s *SendImpl) SendRemoteCollectionTo(nodeName string, collection shared.Collection, res map[string]string, groups []string) {
	//sync -> avoid send a collection if used
	collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Lock()
	defer collection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()

	//imp
	collection.AddHost(nodeName)
	//TODO send to node
}
