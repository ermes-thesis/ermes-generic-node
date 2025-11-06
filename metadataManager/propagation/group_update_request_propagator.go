package propagation

import (
	dataManagerShared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"net/http"
	"server"
)

type GroupUpdateRequestPropagator interface {
	Propagate(collectionId string, message GroupMessage) (*UpdateGroupResponse, error)
}

type GroupsUpdateRequestPropagatorImpl struct {
	serverInfo *server.ServerInfo
	httpHelper *HTTPHelper
	log        logger.Logger
}

func NewGroupUpdateRequestPropagator(serverInfo *server.ServerInfo, log logger.Logger) *GroupsUpdateRequestPropagatorImpl {
	return &GroupsUpdateRequestPropagatorImpl{
		serverInfo: serverInfo,
		httpHelper: NewHTTPHelper(serverInfo, log),
		log:        log,
	}
}

func (p *GroupsUpdateRequestPropagatorImpl) Propagate(collectionId string, message GroupMessage) (*UpdateGroupResponse, error) {
	p.log.Info("Starting group update request propagation", "collection_id", collectionId, "executing_user", message.UserId, "node_geolocation", p.serverInfo.MetadataDomain)
	p.log.Debug("Starting group update request propagation", "user_access_groups", message.UserGroups, "groups_to_add", message.GroupsToAdd, "groups_to_remove", message.GroupsToRemove)

	// If a cloud node is configured, always propagate to it
	if p.serverInfo.CloudNode != nil {
		p.log.Info("Forwarding group update to cloud node", "collection_id", collectionId, "executing_user", message.UserId, "node_geolocation", p.serverInfo.MetadataDomain)
		p.log.Debug("Forwarding group update to cloud node", "cloud_node_hostname", p.serverInfo.CloudNode.HostName, "cloud_node_geolocation", p.serverInfo.CloudNode.MetadataDomain, "user_access_groups", message.UserGroups, "groups_to_add", message.GroupsToAdd, "groups_to_remove", message.GroupsToRemove)
		return p.sendHttpReqToDest(p.serverInfo.CloudNode.HostName, collectionId, message)
	}

	// If no cloud node is configured, propagate to father and children based on geolocation
	if p.serverInfo.Father != nil {
		p.log.Info("No cloud node configured for current node. Forwarding group update to father node", "collection_id", collectionId, "executing_user", message.UserId, "node_geolocation", p.serverInfo.MetadataDomain)
		p.log.Debug("Forwarding group update to cloud node", "father_node_hostname", p.serverInfo.Father.HostName, "father_node_geolocation", p.serverInfo.Father.MetadataDomain, "user_access_groups", message.UserGroups, "groups_to_add", message.GroupsToAdd, "groups_to_remove", message.GroupsToRemove)
		return p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, message)
	}

	p.log.Error("Propagation failed: no cloud or father node configured", "collection_id", collectionId, "executing_user", message.UserId, "node_geolocation", p.serverInfo.MetadataDomain, "user_access_groups", message.UserGroups, "groups_to_add", message.GroupsToAdd, "groups_to_remove", message.GroupsToRemove)
	return nil, errors.New("propagation failed, no cloud node or father node configured")
}

func (p *GroupsUpdateRequestPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, message GroupMessage) (*UpdateGroupResponse, error) {
	p.log.Debug("Creating HTTP request for group update request propagation", "collection_id", collectionId, "destination", dest, "message", fmt.Sprintf("%+v", message))

	req, err := p.httpHelper.CreateBasicPvtHttpRequest(http.MethodPost, dest, EndpointRequestUpdateGroups, collectionId, message)
	if err != nil {
		p.log.ErrorfWith("Failed to create basic HTTP request for '%s' path", []any{EndpointRequestUpdateGroups}, "collection_id", collectionId, "destination", dest, "message", fmt.Sprintf("%+v", message), "error", err)
		return nil, err
	}

	p.log.Debug("Sending HTTP request to destination node", "url", req.URL, "headers", fmt.Sprintf("%+v", req.Header))

	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		p.log.ErrorfWith("Failed to send HTTP request for '%s' path", []any{EndpointRequestUpdateGroups}, "collection_id", collectionId, "destination", dest, "message", fmt.Sprintf("%+v", message), "request_headers", fmt.Sprintf("%+v", req.Header), "error", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		p.log.Warn("Unauthorized response from destination", "collection_id", collectionId, "destination", dest, "message", fmt.Sprintf("%+v", req.Header))
		return nil, &dataManagerShared.UnauthorizedError{}
	}
	if resp.StatusCode != http.StatusOK {
		p.log.ErrorfWith("Unexpected status code received while contacting '%s' path", []any{EndpointRequestUpdateGroups}, "collection_id", collectionId, "destination", dest, "message", fmt.Sprintf("%+v", message), "received_status_code", resp.Status, "expected_status_code", http.StatusOK)
		return nil, fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, dest)
	}

	var respData *UpdateGroupResponse
	if err = json.NewDecoder(resp.Body).Decode(respData); err != nil {
		p.log.Error("Failed to decode response from group update propagation", "collection_id", collectionId, "destination", dest, "message", fmt.Sprintf("%+v", message), "error", err)
		return nil, err
	}

	p.log.Info("Group update request successfully propagated", "collection_id", collectionId)
	p.log.Debug("Group update request successfully propagated", "destination", dest, "message", fmt.Sprintf("%+v", message))
	return respData, err
}
