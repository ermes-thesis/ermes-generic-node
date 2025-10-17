package propagation

import (
	dataManagerShared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"net/http"
	"server/info"
)

type GroupUpdateRequestPropagator interface {
	Propagate(collectionId string, message GroupMessage) (*UpdateGroupResponse, error)
}

type GroupsUpdateRequestPropagatorImpl struct {
	serverInfo *info.ServerInfo
	httpHelper *HTTPHelper
	log        *logger.Logger
}

func NewGroupUpdateRequestPropagator(serverInfo *info.ServerInfo, log *logger.Logger) *GroupsUpdateRequestPropagatorImpl {
	return &GroupsUpdateRequestPropagatorImpl{
		serverInfo: serverInfo,
		httpHelper: NewHTTPHelper(serverInfo, log),
		log:        log,
	}
}

func (p *GroupsUpdateRequestPropagatorImpl) Propagate(collectionId string, message GroupMessage) (*UpdateGroupResponse, error) {
	p.log.Info("Starting to propagating groups update request...")

	// If a cloud node is configured, always propagate to it
	if p.serverInfo.CloudNode != nil {
		p.log.Info("Using cloud node %s for the request.\n", p.serverInfo.CloudNode.HostName)
		return p.sendHttpReqToDest(p.serverInfo.CloudNode.HostName, collectionId, message)
	}

	// If no cloud node is configured, propagate to father and children based on geolocation
	if p.serverInfo.Father != nil {
		p.log.Info("No cloud node configured, forwarding to parent node.")
		return p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, message)
	}

	return nil, errors.New("no cloud node or father node configured")
}

func (p *GroupsUpdateRequestPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, message GroupMessage) (*UpdateGroupResponse, error) {
	req, err := p.httpHelper.CreateBasicPvtHttpRequest(http.MethodPost, dest, EndpointRequestUpdateGroups, collectionId, message)

	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		p.log.Errorf("[%s] Error sending request: %v\n", p.serverInfo.MetadataDomain, err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		return nil, &dataManagerShared.UnauthorizedError{}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error when contacting the node: %s", resp.Status)
	}

	var respData *UpdateGroupResponse
	err = json.NewDecoder(resp.Body).Decode(respData)

	return respData, err
}
