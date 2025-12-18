package sender

import (
	"dataManager/lru"
	gossip "dataManager/messages"
	shared "dataManager/shared"
	"db"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"server"

	"github.com/ermes-thesis/ermes-common/logger"
)

const (
	gossipPath                      = "/pvt-gossip"
	repoPath                        = "/pvt-repo"
	requestInfoPath                 = "/pvt-collectionRequestInfo"
	remoteAccessPath                = "/pvt-dataRemoteAccess"
	remoteUpdatePath                = "/pvt-dataRemoteUpdate"
	updatePropagationPath           = "/pvt-dataUpdatePropagation"
	updateGroupPropagationPath      = "/pvt-dataGroupUpdatePropagation"
	createCollectionPropagationPath = "/pvt-newCollection"
	removeCollectionPropagationPath = "/pvt-removeCollection"
	remoteCollectionSet             = "/pvt-remoteCollectionSet"
)

//###############################################################
//########## Migration Manager Sender          ##################
//###############################################################

type MigrationManagerSender interface {
	SendRemoteCollectionTo(destination string, collection shared.Collection, sendAsLeader bool, res map[string]db.RecordValue, groups []string, owner string, groupsSequence uint64, collectionSize uint64, isMigration bool)
	SendGossipMessages(gossip.GossipMessage, string)
	SendRequestInfoMessage(gossip.CollectionRequestInfoMessages, string)
	GossipPath() string
	RepoPath() string
	RequestInfoPath() string
}

type MigrationManagerSenderImpl struct {
	serverInfo  server.ServerInfo
	partialView *shared.CollectionPartialView
	server      server.ServerType
	log         logger.Logger
	dbInterface db.GenericDB
	lruCache    *lru.LRUCache[shared.Collection]
}

func NewMigrationManagerSender(logger logger.Logger, server server.ServerType, serverInfo server.ServerInfo, partialView *shared.CollectionPartialView, dbInterface db.GenericDB, lruCache *lru.LRUCache[shared.Collection]) MigrationManagerSender {
	return &MigrationManagerSenderImpl{
		server:      server,
		log:         logger,
		serverInfo:  serverInfo,
		partialView: partialView,
		dbInterface: dbInterface,
		lruCache:    lruCache,
	}
}

func (s *MigrationManagerSenderImpl) SendRemoteCollectionTo(destination string, collection shared.Collection, sendAsLeader bool, res map[string]db.RecordValue, groups []string, owner string, groupsSequence uint64, collectionSize uint64, isMigration bool) {
	//imp
	s.log.Info("SENDER: Preparing to send collection", "collectionId", collection.CollectionId(), "destination", destination, "isMigration", isMigration, "sendAsLeader", sendAsLeader, "collectionSize", collectionSize)
	s.log.Infof("[SENDER] send collection %s to %s", collection.CollectionId(), destination)
	if s.serverInfo.Father == nil || destination != s.serverInfo.Father.HostName {
		s.log.Debug("Adding host to collection", "hostname", s.serverInfo.HostName, "destination", destination, "known_host_ids", collection.HostsId())
		collection.AddHost(destination)
	}

	hostToRemove := isMigration && s.serverInfo.Father != nil && destination == s.serverInfo.Father.HostName && len(collection.HostsId()) == 0

	msg := gossip.SendCollectionMessage{
		CollectionId:   collection.CollectionId(),
		Records:        res,
		Groups:         groups,
		Owner:          owner,
		GroupsSequence: groupsSequence,
		IsStrong:       collection.IsStrong(),
		CollectionSize: collectionSize,
		HostToRemove:   hostToRemove,
		SenderIp:       s.serverInfo.HostName,
	}
	if collection.IsStrong() {
		msg.IsLeader = sendAsLeader
		msg.SequenceNumber = collection.(*shared.StrongCollection).ConsistencyClock
	} else {
		msg.IsLeader = false
	}

	go func() {
		code, err := s.server.SendGenericHTTPRequest(destination, repoPath, msg)

		//sync
		s.partialView.Mutex.Lock()
		defer s.partialView.Mutex.Unlock()

		//handle errors
		if err != nil || code != http.StatusOK {
			if code == http.StatusInternalServerError {
				s.log.Warnf("SENDER , Collection refused from %s, for: %v", destination, err)

				//stop sending to this node, at least until next gossip
				nodeInfo, ok := s.partialView.NodeResourceUsage[destination]
				if ok {
					nodeInfo.FreeRedisMemory = 0.0
					s.partialView.NodeResourceUsage[destination] = nodeInfo
				}

				s.sendRemoteCollectionErrorHandler(destination, collection, sendAsLeader, res, groups, owner, groupsSequence, collectionSize, isMigration)
			} else {
				s.log.Errorf("SENDER, Error sending collection to %s: %v", destination, err)
			}
			return
		}

		//stop postpone updates for this destination + apply eventually postponed updates
		err = collection.GetCollectionState().RemoveCollectionMigrationHostAndExecutePostponedUpdates(destination)
		if err != nil {
			s.log.Warn("SENDER: try to remove non present migration host from collection", "collectionId", collection.CollectionId(), "destination", destination, "err", err)
			return
		}

		//if collection not present in subtree and no more postponed updates
		if isMigration && len(collection.HostsId()) == 0 && !collection.IsLocal() /* && !collection.GetCollectionState().IsCollectionBeingMigrated() */ {
			//remove collection from partial view
			delete(s.partialView.KnownCollections, collection.CollectionId())
		}
	}()
}

func (s *MigrationManagerSenderImpl) sendRemoteCollectionErrorHandler(destination string, collection shared.Collection, sendAsLeader bool, res map[string]db.RecordValue, groups []string, owner string, groupsSequence uint64, collectionSize uint64, isMigration bool) {
	collection.GetCollectionState().RemoveHost(destination) // remove host from known hosts

	//remove postponed delete behaviour
	if collection.IsStrong() && collection.IsLocal() && collection.(*shared.StrongCollection).HasPostponedDecisionBehaviour() {
		collection.(*shared.StrongCollection).PopPostponedDecisionBehaviour()
	}

	if isMigration {
		//set collection back in redis
		availableMem, memErr := s.dbInterface.GetAvailableMemory()
		if memErr != nil {
			s.log.Error("SENDER, get memory usage fails")
			return
		}
		if availableMem < collectionSize { //fails to rollback, only if 2 adjacent nodes has full memory and try to migrate between them
			s.log.Error("SENDER, IRREVERSIBLE ERROR, not enough memory to rollback collection after migration fails", "collectionId", collection.CollectionId())
			return
		}
		err := s.dbInterface.SetCollection(collection.CollectionId(), res, groups, owner, groupsSequence)
		if err != nil {
			s.log.Error("SENDER, add collection in redis fails")
			return
		}

		//place back in LRU
		s.lruCache.ForcePut(collection.CollectionId(), collection, uint(collectionSize))

		if collection.GetCollectionState().HasPostponedUpdate(destination) {
			err = collection.GetCollectionState().RemoveCollectionMigrationHostExecutingPostponedUpdatesLocally(destination)
			if err != nil {
				s.log.Error("SENDER, IRREVERSIBLE ERROR, some postponed updates were lost during migration error handling", "collectionId", collection.CollectionId())
				return
			}
		}

		collection.SetToLocal(collectionSize) // set collection back to local
		if collection.IsStrong() {
			collection.(*shared.StrongCollection).ConsistencyClock = groupsSequence // set back consistency clock
		}
		s.partialView.KnownCollections[collection.CollectionId()] = collection // add back to known collections
		s.log.Info("SENDER: Migration failed, set collection back to local", "collectionId", collection.CollectionId())

	}

	if sendAsLeader && collection.IsStrong() {
		collection.(*shared.StrongCollection).IsLeader = true                  // set back as leader
		collection.(*shared.StrongCollection).LeaderId = s.serverInfo.HostName // set back as leader
		s.log.Info("SENDER: Leader migration failed, set collection back as leader", "collectionId", collection.CollectionId())
	}
}

func (s *MigrationManagerSenderImpl) SendGossipMessages(msg gossip.GossipMessage, destination string) {
	go func() {
		_, err := s.server.SendGenericHTTPRequest(destination, gossipPath, msg)
		if err != nil {
			s.log.Errorf("SENDER, Error sending gossip message to %s: %v", destination, err)
		}
	}()
}

func (s *MigrationManagerSenderImpl) SendRequestInfoMessage(msg gossip.CollectionRequestInfoMessages, destination string) {
	s.log.InfofWith("SENDER: send collection request info to %s", []any{destination}, "msg", fmt.Sprintf("%+v", msg))

	go s.server.SendGenericHTTPRequest(destination, requestInfoPath, msg)
}

func (s *MigrationManagerSenderImpl) GossipPath() string {
	return gossipPath
}

func (s *MigrationManagerSenderImpl) RepoPath() string {
	return repoPath
}

func (s *MigrationManagerSenderImpl) RequestInfoPath() string {
	return requestInfoPath
}

//###############################################################
//########## Collection Discoverer Sender      ##################
//###############################################################

type CollectionDiscovererSender interface {
	RemoteSetCollectionAndPropagate(hostId string, collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, isStrong, skipUniquenessCheck bool) error
	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, userId string, recordNames ...string) ([]any, error)
	PropagateUpdate(hostId string, update gossip.CollectionUpdateMessages, saftyQueues []shared.FunctionQueue)
	PropagateGroupUpdate(hostId string, update gossip.CollectionGroupUpdateMessages, saftyQueues []shared.FunctionQueue)
	NewCollectionCreated(hostId string, newCollection gossip.NewCollectionMessage) error
	RemoveCollection(hostId string, collectionId string, senderIp string, groups []string, userId string)

	RemoteAccessPath() string
	RemoteUpdatePath() string
	UpdatePropagationPath() string
	UpdateGroupPropagationPath() string
	CreateCollectionPropagationPath() string
	RemoveCollectionPropagationPath() string
	RemoteCollectionSetPath() string
}

type CollectionDiscovererSenderImpl struct {
	server server.ServerType
	log    logger.Logger
}

func NewCollectionDiscovererSender(log logger.Logger, server server.ServerType) CollectionDiscovererSender {
	return &CollectionDiscovererSenderImpl{
		server: server,
		log:    log,
	}
}

func (s *CollectionDiscovererSenderImpl) WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, userId string, recordNames ...string) ([]any, error) {
	s.log.Infof("[SENDER] request collection %s access to %s", collectionId, hostId)

	msg := gossip.RemoteAccessRequest{
		CollectionId: collectionId,
		RecordNames:  recordNames,
		Groups:       groups,
		UserId:       userId,
	}

	res, err := s.server.SendGenericHTTPRequestReturnResponse(hostId, remoteAccessPath, msg)
	if err != nil {
		s.log.Error("HTTP-REQ, Error reading response body")
		return nil, err
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusUnauthorized {
		s.log.Info("HTTP-REQ, Received unauthorized response")
		return nil, &shared.UnauthorizedError{}
	}
	if res.StatusCode != 200 {
		s.log.Infof("HTTP-REQ, Received non-200 response: %d", res.StatusCode)
		return nil, fmt.Errorf("non-200 response: %d", res.StatusCode)
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		s.log.Error("HTTP-REQ, Error reading response body")
		return nil, err
	}
	var resp gossip.RemoteAccessResponse
	if err := json.Unmarshal(bodyBytes, &resp); err != nil {
		s.log.Error("HTTP-REQ, Error parsing remote access response")
		return nil, err
	}

	return resp.Records, err
}

func (s *CollectionDiscovererSenderImpl) NewCollectionCreated(hostId string, newCollection gossip.NewCollectionMessage) error {
	s.log.Infof("SENDER: notify new collection %s to %s", newCollection.CollectionId, hostId)

	resp, err := s.server.SendGenericHTTPRequestReturnResponse(hostId, createCollectionPropagationPath, newCollection)
	if err != nil {
		s.log.Errorf("SENDER, Error sending create collection to %s: %v", hostId, err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			s.log.Error("SENDER, Error reading response body")
			return err
		}
		return fmt.Errorf("non-200 response from create collection: %v %s", resp.StatusCode, b)
	}
	return nil
}

func (s *CollectionDiscovererSenderImpl) RemoveCollection(hostId string, collectionId string, senderIp string, groups []string, userId string) {
	s.log.Infof("SENDER: request collection %s removal to %s", collectionId, hostId)

	msg := gossip.RemoveCollectionMessage{
		CollectionId: collectionId,
		SenderIp:     senderIp,
		Groups:       groups,
		UserId:       userId,
	}

	go s.server.SendGenericHTTPRequest(hostId, removeCollectionPropagationPath, msg)
}

func (s *CollectionDiscovererSenderImpl) RemoteSetCollectionAndPropagate(hostId string, collectionId string, collectionName string, records map[string]db.RecordValue, groups []string, userId string, isStrong, skipUniquenessCheck bool) error {
	msg := gossip.SetCollectionMessage{
		CollectionId:        collectionId,
		CollectionName:      collectionName,
		Records:             records,
		Groups:              groups,
		UserId:              userId,
		IsStrong:            isStrong,
		SkipUniquenessCheck: skipUniquenessCheck,
	}

	_, err := s.server.SendGenericHTTPRequest(hostId, remoteCollectionSet, msg)
	if err != nil {
		s.log.Errorf("SENDER, Error sending set collection to %s: %v", hostId, err)
		return err
	}
	return nil
}

func (s *CollectionDiscovererSenderImpl) PropagateUpdate(hostId string, update gossip.CollectionUpdateMessages, saftyQueues []shared.FunctionQueue) {
	s.log.Infof("SENDER: request collection update propagation to %s", hostId)

	go func() {
		s.server.SendGenericHTTPRequest(hostId, updatePropagationPath, update)
		s.executeUpdateSaftyCheck(saftyQueues)
	}()
}

func (s *CollectionDiscovererSenderImpl) PropagateGroupUpdate(hostId string, update gossip.CollectionGroupUpdateMessages, saftyQueues []shared.FunctionQueue) {
	s.log.Infof("SENDER: request collection group update propagation to %s", hostId)

	go func() {
		s.server.SendGenericHTTPRequest(hostId, updateGroupPropagationPath, update)
		s.executeUpdateSaftyCheck(saftyQueues)
	}()
}

// execute order 66
func (s *CollectionDiscovererSenderImpl) executeUpdateSaftyCheck(saftyQueues []shared.FunctionQueue) {
	for _, saftyQueue := range saftyQueues {
		saftyQueue.ExecuteFunctions()
	}
}

func (s *CollectionDiscovererSenderImpl) RemoteAccessPath() string {
	return remoteAccessPath
}

func (s *CollectionDiscovererSenderImpl) RemoteUpdatePath() string {
	return remoteUpdatePath
}

func (s *CollectionDiscovererSenderImpl) UpdatePropagationPath() string {
	return updatePropagationPath
}

func (s *CollectionDiscovererSenderImpl) UpdateGroupPropagationPath() string {
	return updateGroupPropagationPath
}

func (s *CollectionDiscovererSenderImpl) CreateCollectionPropagationPath() string {
	return createCollectionPropagationPath
}

func (s *CollectionDiscovererSenderImpl) RemoveCollectionPropagationPath() string {
	return removeCollectionPropagationPath
}

func (s *CollectionDiscovererSenderImpl) RemoteCollectionSetPath() string {
	return remoteCollectionSet
}
