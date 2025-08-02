package weakpolicy

import (
	"config"
	sender "dataManager/migrationManager/migrationSender"
	shared "dataManager/shared"
	"db"
	log "logger"
)

type WeakCollectionPolicy interface {
	ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int)
}

type CurrentPolicy int

const (
	Pending CurrentPolicy = iota
	ALWAYSCOPYPOLICY
)

type AlwayCopyPolicy struct {
	//config
	logger log.Logger

	//impl
	partialView *shared.CollectionPartialView

	dbInterface db.GenericDB
	sender      sender.Sender
}

func NewAlwayCopyPolicy(config config.MigrationManagerConfig, sender sender.Sender, logger log.Logger, partialView *shared.CollectionPartialView, dbInterface db.GenericDB) WeakCollectionPolicy {
	policy := &AlwayCopyPolicy{
		dbInterface: dbInterface,
		partialView: partialView,
		sender:      sender,
		logger:      logger,
	}

	return policy
}

// impl
func (a *AlwayCopyPolicy) ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
	if childName == "user" {
		return
	}

	a.logger.Println("WEAK-POLICY", "handle request for a weak collection", log.INFO)
	weakCollection := a.partialView.KnowedCollections[collectionId]

	res, groups, err := a.dbInterface.GetCollection(collectionId)
	if err != nil {
		a.logger.Println("WEAK-POLICY", "got an error getting the collection from redis", log.ERROR)
		panic(err) //TODO: decide
	}

	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	a.sender.SendRemoteCollectionTo(childName, weakCollection, false, res, groups)
}
