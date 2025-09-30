package weakpolicy

import (
	"config"
	sender "dataManager/sender"
	shared "dataManager/shared"
	"db"
	"logger"
)

type WeakCollectionPolicy interface {
	ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int)
}

type CurrentPolicy int

const (
	Pending CurrentPolicy = iota
	ALWAYSCOPYPOLICY
)

type AlwaysCopyPolicy struct {
	//config
	log *logger.Logger

	//impl
	partialView *shared.CollectionPartialView

	dbInterface db.GenericDB
	sender      sender.MigrationManagerSender
}

func NewAlwaysCopyPolicy(config config.MigrationManagerConfig, sender sender.MigrationManagerSender, logger *logger.Logger, partialView *shared.CollectionPartialView, dbInterface db.GenericDB) WeakCollectionPolicy {
	policy := &AlwaysCopyPolicy{
		dbInterface: dbInterface,
		partialView: partialView,
		sender:      sender,
		log:         logger,
	}

	return policy
}

// impl
func (a *AlwaysCopyPolicy) ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	if childName == "user" {
		return
	}

	a.log.Info("WEAK-POLICY, handle request for a weak collection")
	weakCollection := a.partialView.KnownCollections[collectionId]

	weakCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Lock()
	defer weakCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()

	res, groups, err := a.dbInterface.GetCollection(collectionId)
	if err != nil {
		a.log.Error("WEAK-POLICY, got an error getting the collection from redis")
		panic(err) //TODO: decide
	}
	collectionSize, sizeError := a.dbInterface.GetCollectionMemoryUsage(collectionId)
	if sizeError != nil {
		panic(sizeError) //TODO: decide
	}

	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	a.log.Infof("WEAK-POLICY, send weak collection%s to node %s", collectionId, childName)
	a.sender.SendRemoteCollectionTo(childName, weakCollection, false, res, groups, collectionSize)
}
