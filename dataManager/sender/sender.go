package sender

import (
	gossip "dataManager/messages"
	shared "dataManager/shared"
	"encoding/json"
	"fmt"
	"io"
	log "logger"
	"server"
	info "server/info"
)

const (
	gossipPath                      = "/gossip"
	repoPath                        = "/repo"
	remoteAccessPath                = "/dataRemoteAccess"
	remoteUpdatePath                = "/dataRemoteUpdate"
	updatePropagationPath           = "/dataUpdatePropagation"
	createCollectionPropagationPath = "/newCollection"
)

//###############################################################
//########## Migration Manager Sender          ##################
//###############################################################

type MigrationManagerSender interface {
	SendRemoteCollectionTo(string, shared.Collection, bool, map[string]string, []string, uint64)
	SendGossipMessages(gossip.GossipMessage, string)
	GossipPath() string
	RepoPath() string
}

type MigrationManagerSenderImpl struct {
	serverInfo info.ServerInfo
	server     server.ServerType
	logger     log.Logger
}

func NewMigrationManagerSender(logger log.Logger, server server.ServerType, serverInfo info.ServerInfo) MigrationManagerSender {
	return &MigrationManagerSenderImpl{
		server:     server,
		logger:     logger,
		serverInfo: serverInfo,
	}
}

func (s *MigrationManagerSenderImpl) SendRemoteCollectionTo(destination string, collection shared.Collection, sendAsLeader bool, res map[string]string, groups []string, collectionSize uint64) {
	//imp
	s.logger.Println("SENDER", fmt.Sprintf("send collection %s to %s", collection.CollectionId(), destination), log.INFO)
	if destination != s.serverInfo.Father.HostName {
		collection.AddHost(destination)
	}
	hostToRemove := destination == s.serverInfo.Father.HostName && len(collection.HostsId()) == 0

	msg := gossip.SendCollectionMessage{
		CollectionId:   collection.CollectionId(),
		Records:        res,
		Groups:         groups,
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
	WeakCollectionRemoteRecordAccess(hostId string, collectionId string, recordNames ...string) ([]any, error)
	WeakCollectionRemoteRecordUpdate(hostId string, collectionId string, recordNames []string, value []string, isDelete bool, serverInfo info.ServerInfo) error
	PropagateUpdate(hostId string, update gossip.CollectionUpdateMessage)
	NewCollectionCreated(hostId string, newCollection gossip.NewCollectionMessage)

	RemoteAccessPath() string
	RemoteUpdatePath() string
	UpdatePropagationPath() string
	CreateCollectionPropagationPath() string
}

type CollectionDiscovererSenderImpl struct {
	server server.ServerType
	logger log.Logger
}

func NewCollectionDiscovererSender(logger log.Logger, server server.ServerType) CollectionDiscovererSender {
	return &CollectionDiscovererSenderImpl{
		server: server,
		logger: logger,
	}
}

func (s *CollectionDiscovererSenderImpl) WeakCollectionRemoteRecordAccess(hostId string, collectionId string, recordNames ...string) ([]any, error) {
	s.logger.Println("SENDER", fmt.Sprintf("request collection %s access to %s", collectionId, hostId), log.INFO)

	msg := gossip.RemoteAccessRequest{
		CollectionId: collectionId,
		RecordNames:  recordNames,
	}

	res, err := s.server.SendGenericHTTPRequestRetrunResponse(hostId, remoteAccessPath, msg)
	if err != nil {
		s.logger.Println("HTTP-REQ", "Error reading response body", log.ERROR)
		return nil, err
	}

	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		s.logger.Println("HTTP-REQ", "Error reading response body", log.ERROR)
		return nil, err
	}
	var resp gossip.RemoteAccessResponse
	if err := json.Unmarshal(bodyBytes, &resp); err != nil {
		s.logger.Println("HTTP-REQ", "Error parsing remote access response", log.ERROR)
		return nil, err
	}

	return resp.Records, err
}

func (s *CollectionDiscovererSenderImpl) WeakCollectionRemoteRecordUpdate(hostId string, collectionId string, recordNames []string, value []string, isDelete bool, serverInfo info.ServerInfo) error {
	s.logger.Println("SENDER", fmt.Sprintf("request collection %s update to %s", collectionId, hostId), log.INFO)

	msg := gossip.RemoteUpdateRequest{
		CollectionId: collectionId,
		RecordNames:  recordNames,
		Record:       value,
		SenderIp:     serverInfo.HostName,
		IsDelete:     isDelete,
	}
	if isDelete {
		msg.Record = []string{}
	}

	res, err := s.server.SendGenericHTTPRequestRetrunResponse(hostId, remoteUpdatePath, msg)
	if err != nil {
		s.logger.Println("HTTP-REQ", "Error reading response body", log.ERROR)
		return err
	}

	defer res.Body.Close()

	return nil
}

func (s *CollectionDiscovererSenderImpl) NewCollectionCreated(hostId string, newCollection gossip.NewCollectionMessage) {
	s.logger.Println("SENDER", fmt.Sprintf("notify new collection %s to %s", newCollection.CollectionId, hostId), log.INFO)

	s.server.SendGenericHTTPRequest(hostId, createCollectionPropagationPath, newCollection)
}

func (s *CollectionDiscovererSenderImpl) PropagateUpdate(hostId string, update gossip.CollectionUpdateMessage) {
	s.logger.Println("SENDER", fmt.Sprintf("request collection update propagation to %s", hostId), log.INFO)

	s.server.SendGenericHTTPRequest(hostId, updatePropagationPath, update)
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

func (s *CollectionDiscovererSenderImpl) CreateCollectionPropagationPath() string {
	return createCollectionPropagationPath
}
