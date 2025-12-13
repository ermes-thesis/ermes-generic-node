package collectionsDiscoverer

import (
	"dataManager/lru"
	"dataManager/messages"
	"dataManager/migrationManager"
	"dataManager/sender"
	shared "dataManager/shared"
	"db"
	"errors"
	"server"

	"github.com/ermes-thesis/ermes-common/logger"
)

type CollectionsDiscovererStatelessGenericNode struct {
	centralizedDBAddress string
	sender               sender.CollectionDiscovererSender
	log                  logger.Logger
	serverInfo           server.ServerInfo
}

func GetCollectionsDiscovererStatelessGenericNodeInstance() CollectionsDiscoverer {
	once.Do(func() {
		collectionsDiscovererInstance = &CollectionsDiscovererStatelessGenericNode{}
	})
	return collectionsDiscovererInstance
}

func (c *CollectionsDiscovererStatelessGenericNode) Init(collections *shared.CollectionPartialView, serverInfo server.ServerInfo, server server.ServerType, dbInterface db.GenericDB, logger logger.Logger, lruCache *lru.LRUCache[shared.Collection]) {
	initOnce.Do(func() {
		c.log = logger
		c.serverInfo = serverInfo
		c.sender = sender.NewCollectionDiscovererSender(logger, server)
		c.log.Info("Collection discoverer component initialized for centralized DB", "server_info", serverInfo)
	})
}

func (c *CollectionsDiscovererStatelessGenericNode) SetCentralizedDBAddress(centralizedDBAddress string) {
	c.centralizedDBAddress = centralizedDBAddress
}

func (c *CollectionsDiscovererStatelessGenericNode) IsCollectionLocallyPresent(collectionId string, consistencyClock uint64) (bool, shared.Collection, error) {
	return true, nil, nil //force functions to run where data are not stored
}

func (c *CollectionsDiscovererStatelessGenericNode) AccessRecords(collectionId string, dbFn func() (any, error), remoteFn func(addr string) (any, error)) (any, error) {
	return remoteFn(c.centralizedDBAddress) //remote to centralized storage
}

func (c *CollectionsDiscovererStatelessGenericNode) WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, userId string, recordName string) (string, error) {
	res, err := c.sender.WeakCollectionRemoteRecordAccess(hostId, collectionId, groups, userId, recordName)
	if err != nil {
		c.log.Error("Failed to retrieve record value from remote eventual collection", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "recordName", recordName, "requesting_from_node", hostId)
		return "", err
	}
	if len(res) != 1 {
		return "", errors.New("unexpected output")
	}

	return res[0].(string), nil
}

func (c *CollectionsDiscovererStatelessGenericNode) WeakCollectionRemoteRecordsAccess(hostId string, collectionId string, groups []string, userId string, recordNames ...string) ([]any, error) {
	res, err := c.sender.WeakCollectionRemoteRecordAccess(hostId, collectionId, groups, userId, recordNames...)
	if err != nil {
		c.log.Error("Failed to retrieve record value from remote eventual collection", "collection_id", collectionId, "executing_user", userId, "user_access_groups", groups, "record_names", recordNames, "requesting_from_node", hostId)
		return nil, err
	}

	return res, nil
}

func (c *CollectionsDiscovererStatelessGenericNode) UpdateCollectionsRecordsAndPropagate(updates []messages.CollectionUpdateMessage, groups []string, userId string, isInitiator bool, ignoredHostName string) error {
	msg := messages.CollectionUpdateMessages{
		Messages: updates,
		Groups:   groups,
		UserId:   userId,
	}

	c.sender.PropagateUpdate(c.centralizedDBAddress, msg, make([]shared.FunctionQueue, 0))
	return nil
}

func (c *CollectionsDiscovererStatelessGenericNode) UpdateCollectionsGroupsAndPropagate(updates []messages.CollectionGroupUpdateMessage, isInitiator bool, ignoredHostName string) error {
	c.sender.PropagateGroupUpdate(c.centralizedDBAddress, updates, make([]shared.FunctionQueue, 0))
	return nil
}

func (c *CollectionsDiscovererStatelessGenericNode) SetCollectionAndPropagate(collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, migrationManager migrationManager.MigrationManager, isStrong, skipUniquenessCheck bool) error {
	return c.sender.RemoteSetCollectionAndPropagate(c.centralizedDBAddress, collectionId, collectionName, records, groups, userId, isStrong, skipUniquenessCheck)
}

func (c *CollectionsDiscovererStatelessGenericNode) RemoveCollectionAndPropagate(collectionId string, migrationManager migrationManager.MigrationManager, groups []string, userId string, isInitiator bool, ignoredHostName string) error {
	c.sender.RemoveCollection(c.centralizedDBAddress, collectionId, c.serverInfo.HostName, groups, userId)
	return nil
}

func (c *CollectionsDiscovererStatelessGenericNode) GetCollectionVectorClockValue(collectionId string) (uint64, error) {
	return 0, nil
}
