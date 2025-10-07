package sender

import (
	gossip "dataManager/messages"
	shared "dataManager/shared"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"server"
	info "server/info"

	"github.com/ermes-thesis/ermes-common/logger"
)

const (
	gossipPath                      = "/gossip"
	repoPath                        = "/repo"
	remoteAccessPath                = "/dataRemoteAccess"
	remoteUpdatePath                = "/dataRemoteUpdate"
	updatePropagationPath           = "/dataUpdatePropagation"
	updateGroupPropagationPath      = "/dataGroupUpdatePropagation"
	createCollectionPropagationPath = "/newCollection"
	removeCollectionPropagationPath = "/removeCollection"
)

//###############################################################
//########## Migration Manager Sender          ##################
//###############################################################

type MigrationManagerSender interface {
	SendRemoteCollectionTo(string, shared.Collection, bool, map[string]string, []string, uint64, uint64)
	SendGossipMessages(gossip.GossipMessage, string)
	GossipPath() string
	RepoPath() string
}

type MigrationManagerSenderImpl struct {
	serverInfo info.ServerInfo
	server     server.ServerType
	log        *logger.Logger
}

func NewMigrationManagerSender(logger *logger.Logger, server server.ServerType, serverInfo info.ServerInfo) MigrationManagerSender {
	return &MigrationManagerSenderImpl{
		server:     server,
		log:        logger,
		serverInfo: serverInfo,
	}
}

func (s *MigrationManagerSenderImpl) SendRemoteCollectionTo(destination string, collection shared.Collection, sendAsLeader bool, res map[string]string, groups []string, groupsSequence uint64, collectionSize uint64) {
	//imp
	s.log.Infof("[SENDER] send collection %s to %s", collection.CollectionId(), destination)
	if destination != s.serverInfo.Father.HostName {
		collection.AddHost(destination)
	}
	hostToRemove := destination == s.serverInfo.Father.HostName && len(collection.HostsId()) == 0

	msg := gossip.SendCollectionMessage{
		CollectionId:   collection.CollectionId(),
		Records:        res,
		Groups:         groups,
		GroupsSequence: groupsSequence,
		IsStrong:       collection.IsStrong(),
		CollectionSize: collectionSize,
		HostToRemove:   hostToRemove,
		SenderIp:       s.serverInfo.HostName,
	}
	if collection.IsStrong() {
		msg.IsLeader = collection.(*shared.StrongCollection).IsLeader && sendAsLeader
	} else {
		msg.IsLeader = false
	}

	s.server.SendGenericHTTPRequest(destination, repoPath, msg)
}

func (s *MigrationManagerSenderImpl) SendGossipMessages(msg gossip.GossipMessage, destination string) {
	s.server.SendGenericHTTPRequest(destination, gossipPath, msg)
}

func (s *MigrationManagerSenderImpl) GossipPath() string {
	return gossipPath
}

func (s *MigrationManagerSenderImpl) RepoPath() string {
	return repoPath
}

//###############################################################
//########## Collection Discoverer Sender      ##################
//###############################################################

type CollectionDiscovererSender interface {
	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, recordNames ...string) ([]any, error)
	PropagateUpdate(hostId string, update gossip.CollectionUpdateMessages)
	PropagateGroupUpdate(hostId string, update gossip.CollectionGroupUpdateMessages)
	NewCollectionCreated(hostId string, newCollection gossip.NewCollectionMessage)
	RemoveCollection(hostId string, collectionId string, senderIp string, groups []string)

	RemoteAccessPath() string
	RemoteUpdatePath() string
	UpdatePropagationPath() string
	UpdateGroupPropagationPath() string
	CreateCollectionPropagationPath() string
	RemoveCollectionPropagationPath() string
}

type CollectionDiscovererSenderImpl struct {
	server server.ServerType
	log    *logger.Logger
}

func NewCollectionDiscovererSender(log *logger.Logger, server server.ServerType) CollectionDiscovererSender {
	return &CollectionDiscovererSenderImpl{
		server: server,
		log:    log,
	}
}

func (s *CollectionDiscovererSenderImpl) WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, recordNames ...string) ([]any, error) {
	s.log.Infof("[SENDER] request collection %s access to %s", collectionId, hostId)

	msg := gossip.RemoteAccessRequest{
		CollectionId: collectionId,
		RecordNames:  recordNames,
		Groups:       groups,
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

func (s *CollectionDiscovererSenderImpl) NewCollectionCreated(hostId string, newCollection gossip.NewCollectionMessage) {
	s.log.Infof("SENDER: notify new collection %s to %s", newCollection.CollectionId, hostId)

	s.server.SendGenericHTTPRequest(hostId, createCollectionPropagationPath, newCollection)
}

func (s *CollectionDiscovererSenderImpl) RemoveCollection(hostId string, collectionId string, senderIp string, groups []string) {
	s.log.Infof("SENDER: request collection %s removal to %s", collectionId, hostId)

	msg := gossip.RemoveCollectionMessage{
		CollectionId: collectionId,
		SenderIp:     senderIp,
		Groups:       groups,
	}

	s.server.SendGenericHTTPRequest(hostId, removeCollectionPropagationPath, msg)
}

func (s *CollectionDiscovererSenderImpl) PropagateUpdate(hostId string, update gossip.CollectionUpdateMessages) {
	s.log.Infof("SENDER: request collection update propagation to %s", hostId)

	s.server.SendGenericHTTPRequest(hostId, updatePropagationPath, update)
}

func (s *CollectionDiscovererSenderImpl) PropagateGroupUpdate(hostId string, update gossip.CollectionGroupUpdateMessages) {
	s.log.Infof("SENDER: request collection group update propagation to %s", hostId)

	s.server.SendGenericHTTPRequest(hostId, updateGroupPropagationPath, update)
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
