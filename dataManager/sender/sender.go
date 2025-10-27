package sender

import (
	gossip "dataManager/messages"
	shared "dataManager/shared"
	"db"
	"encoding/json"
	"errors"
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
)

//###############################################################
//########## Migration Manager Sender          ##################
//###############################################################

type MigrationManagerSender interface {
	SendRemoteCollectionTo(string, shared.Collection, bool, map[string]db.RecordValue, []string, string, uint64, uint64, bool)
	SendGossipMessages(gossip.GossipMessage, string)
	SendRequestInfoMessage(gossip.CollectionRequestInfoMessages, string)
	GossipPath() string
	RepoPath() string
	RequestInfoPath() string
}

type MigrationManagerSenderImpl struct {
	serverInfo server.ServerInfo
	server     server.ServerType
	log        *logger.Logger
}

func NewMigrationManagerSender(logger *logger.Logger, server server.ServerType, serverInfo server.ServerInfo) MigrationManagerSender {
	return &MigrationManagerSenderImpl{
		server:     server,
		log:        logger,
		serverInfo: serverInfo,
	}
}

func (s *MigrationManagerSenderImpl) SendRemoteCollectionTo(destination string, collection shared.Collection, sendAsLeader bool, res map[string]db.RecordValue, groups []string, owner string, groupsSequence uint64, collectionSize uint64, isMigration bool) {
	//imp
	s.log.Infof("[SENDER] send collection %s to %s", collection.CollectionId(), destination)
	if s.serverInfo.Father == nil || destination != s.serverInfo.Father.HostName {
		s.log.Warn("ADDING HOST TO COLLECTION", "hostname", s.serverInfo.HostName, "destination", destination, "KNWN HOSTS", collection.HostsId())
		collection.AddHost(destination)
		s.log.Warn("ADDED HOST TO COLLECTION", "KNWN HOSTS", collection.HostsId())
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
		msg.IsLeader = collection.(*shared.StrongCollection).IsLeader && sendAsLeader
	} else {
		msg.IsLeader = false
	}

	go s.server.SendGenericHTTPRequest(destination, repoPath, msg)
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
	s.log.Infof("SENDER: send collection request info to %s", destination)

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
	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, userId string, recordNames ...string) ([]any, error)
	PropagateUpdate(hostId string, update gossip.CollectionUpdateMessages)
	PropagateGroupUpdate(hostId string, update gossip.CollectionGroupUpdateMessages)
	NewCollectionCreated(hostId string, newCollection gossip.NewCollectionMessage) error
	RemoveCollection(hostId string, collectionId string, senderIp string, groups []string, userId string)

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

func (s *CollectionDiscovererSenderImpl) WeakCollectionRemoteRecordAccess(hostId string, collectionId string, groups []string, userId string, recordNames ...string) ([]any, error) {
	s.log.Infof("[SENDER] request collection %s access to %s", collectionId, hostId)

	msg := gossip.RemoteAccessRequest{
		CollectionId: collectionId,
		RecordNames:  recordNames,
		Groups:       groups,
		UserId:       userId,
	}

	print("SENDER: sending remote access request to ", hostId, " for collection ", collectionId, "\n")
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
		return errors.New(fmt.Sprintf("non-200 response from create collection: %v %s", resp.StatusCode, b))
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

func (s *CollectionDiscovererSenderImpl) PropagateUpdate(hostId string, update gossip.CollectionUpdateMessages) {
	s.log.Infof("SENDER: request collection update propagation to %s", hostId)

	go s.server.SendGenericHTTPRequest(hostId, updatePropagationPath, update)
}

func (s *CollectionDiscovererSenderImpl) PropagateGroupUpdate(hostId string, update gossip.CollectionGroupUpdateMessages) {
	s.log.Infof("SENDER: request collection group update propagation to %s", hostId)

	go s.server.SendGenericHTTPRequest(hostId, updateGroupPropagationPath, update)
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
