package weakpolicy

import (
	"config"
	"dataManager/sender"
	shared "dataManager/shared"
	"db"
	"server"

	"github.com/ermes-thesis/ermes-common/logger"
)

type WeakCollectionPolicy interface {
	ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int) error
}

type CurrentPolicy int

const (
	Pending CurrentPolicy = iota
	ALWAYSCOPYPOLICY
	DONOTHINGPOLICY
)

func GetPolicy(policyName string) CurrentPolicy {
	switch policyName {
	case "ALWAYSCOPYPOLICY":
		return ALWAYSCOPYPOLICY
	case "DONOTHINGPOLICY":
		return DONOTHINGPOLICY
	default:
		return Pending
	}
}

// AlwaysCopyPolicy default policy for weak collections
type AlwaysCopyPolicy struct {
	//config
	log logger.Logger

	//impl
	partialView *shared.CollectionPartialView

	dbInterface db.GenericDB
	sender      sender.MigrationManagerSender
	serverInfo  server.ServerInfo

	sendingThreshold float64
}

func NewAlwaysCopyPolicy(config config.MigrationManagerConfig, sender sender.MigrationManagerSender, logger logger.Logger, partialView *shared.CollectionPartialView, dbInterface db.GenericDB, serverInfo server.ServerInfo) WeakCollectionPolicy {
	policy := &AlwaysCopyPolicy{
		dbInterface:      dbInterface,
		partialView:      partialView,
		sender:           sender,
		serverInfo:       serverInfo,
		log:              logger,
		sendingThreshold: config.WeakCollectionSendingThreshold,
	}

	return policy
}

// impl
func (a *AlwaysCopyPolicy) ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int) error {
	if childName == "user" {
		return nil
	}
	//verify threshold
	lruFreeMemory, ok := a.partialView.GetNodeResourceUsage(childName)
	if ok && lruFreeMemory.FreeRedisMemory <= a.sendingThreshold { //if not known, we assume we can send
		a.log.Warnf("WEAK-POLICY, not sending weak collection %s to node %s due to memory threshold. Free redis memory: %f", collectionId, childName, lruFreeMemory.FreeRedisMemory)
		return nil
	}

	//send collection
	a.log.Info("WEAK-POLICY, handle request for a weak collection", "collection_id", collectionId, "to_node", childName)
	weakCollection, _ := a.partialView.GetCollection(collectionId)

	weakCollection.GetCollectionState().AddCollectionMigrationHost(childName)
	a.sender.SendRemoteCollectionTo(childName, weakCollection, false, false)

	return nil
}

// DoNothingPolicy dumb policy for comparison in evaluation
type DoNothingPolicy struct{}

func NewDoNothingPolicy() DoNothingPolicy {
	policy := DoNothingPolicy{}

	return policy
}

func (d DoNothingPolicy) ManageNewWeakRequest(collectionId string, childName string, isWrite bool, requestLatency int) error {
	return nil
}
