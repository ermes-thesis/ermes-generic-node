package weakpolicy

import (
	"config"
	sender "dataManager/sender"
	shared "dataManager/shared"
	"db"

	"github.com/ermes-thesis/ermes-common/logger"
)

type WeakCollectionPolicy interface {
	ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int) error
}

type CurrentPolicy int

func (p CurrentPolicy) String() string {
	switch p {
	case Pending:
		return "PENDING"
	case ALWAYSCOPYPOLICY:
		return "ALWAYSCOPYPOLICY"
	default:
		return "UNKNOWN"
	}
}

const (
	Pending CurrentPolicy = iota
	ALWAYSCOPYPOLICY
	DONOTHINGPOLICY
)

// default policy
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
func (a *AlwaysCopyPolicy) ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int) error {
	if childName == "user" {
		return nil
	}

	a.log.Info("WEAK-POLICY, handle request for a weak collection")
	weakCollection := a.partialView.KnownCollections[collectionId]

	weakCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Lock()
	defer weakCollection.GetCollectionState().(*shared.LocalCollection).CollectionLock.Unlock()

	res, groups, owner, groupsSequence, err := a.dbInterface.GetCollection(collectionId)
	if err != nil {
		a.log.Error("WEAK-POLICY, got an error getting the collection from redis")
		return err
	}
	collectionSize, sizeError := a.dbInterface.GetCollectionMemoryUsage(collectionId)
	if sizeError != nil {
		a.log.Error("WEAK-POLICY, got an error getting the collection size")
		return sizeError
	}

	//send collection to the remote node -> sync on collection lock => wait for was execution to ends
	a.log.Infof("WEAK-POLICY, send weak collection%s to node %s", collectionId, childName)
	a.sender.SendRemoteCollectionTo(childName, weakCollection, false, res, groups, owner, groupsSequence, collectionSize, false)

	return nil
}

// dumb policy for comparison in evaluation
type DoNothingPolicy struct{}

func NewDoNothingPolicy() DoNothingPolicy {
	policy := DoNothingPolicy{}

	return policy
}

func (d DoNothingPolicy) ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int) error {
	return nil
}
