package propagation

import (
	"errors"
	"fmt"
	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"net/http"
	"server"
	"strings"
)

type UpdateGroupsPropagator interface {
	Propagate(collectionId string, message UpdateGroupResponse, geolocation string) error
}

type UpdateGroupsPropagatorImpl struct {
	serverInfo *server.ServerInfo
	httpHelper *HTTPHelper
	log        *logger.Logger
}

func NewUpdateGroupsPropagator(serverInfo *server.ServerInfo, log *logger.Logger) *UpdateGroupsPropagatorImpl {
	return &UpdateGroupsPropagatorImpl{
		serverInfo: serverInfo,
		httpHelper: NewHTTPHelper(serverInfo, log),
		log:        log,
	}
}

func (p *UpdateGroupsPropagatorImpl) Propagate(collectionId string, message UpdateGroupResponse, geolocation string) error {
	p.log.Info("Starting new group set propagation", "collection_id", collectionId)
	p.log.Debug("Starting new group set propagation", "new_groups_set", message.Groups, "new_groups_sequence_num", message.Sequence)

	for _, child := range p.serverInfo.Children {
		if strings.HasPrefix(geolocation, child.MetadataDomain) {
			p.log.Info("Forwarding group set update to child node", "collection_id", collectionId, "metadata_geolocation", geolocation, "child_hostname", child.HostName, "child_geolocation", child.MetadataDomain)
			return p.sendHttpReqToDest(child.HostName, collectionId, message)
		}
	}

	p.log.Warn("No matching child node found for geolocation. Ok if we've reached an Edge Node, or a node with with a domain equal to the 'metadata_geolocation'.", "collection_id", collectionId, "node_geolocation", p.serverInfo.MetadataDomain, "metadata_geolocation", geolocation)
	return errors.New("domain not found among children nodes")
}

func (p *UpdateGroupsPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, message UpdateGroupResponse) error {
	req, err := p.httpHelper.CreateBasicPvtHttpRequest(http.MethodPost, dest, EndpointPropagateGroups, collectionId, message)
	if err != nil {
		p.log.ErrorfWith("Failed to create basic HTTP request for '%s' path", []any{EndpointPropagateGroups}, "collection_id", collectionId, "destination", dest, "message", fmt.Sprintf("%+v", message), "error", err)
		return err
	}

	p.log.Debug("Sending HTTP request to destination node", "url", req.URL, "headers", fmt.Sprintf("%+v", req.Header))

	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		p.log.ErrorfWith("Failed to send HTTP request for '%s' path", []any{EndpointPropagateGroups}, "collection_id", collectionId, "destination", dest, "message", fmt.Sprintf("%+v", message), "request_headers", fmt.Sprintf("%+v", req.Header), "error", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.log.ErrorfWith("Unexpected status code received while contacting '%s' path", []any{EndpointPropagateGroups}, "collection_id", collectionId, "destination", dest, "message", fmt.Sprintf("%+v", message), "received_status_code", resp.Status, "expected_status_code", http.StatusOK)
		return fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, dest)
	}

	p.log.Info("Group update successfully propagated", "collection_id", collectionId)
	p.log.Debug("Group update successfully propagated", "destination", dest, "message", fmt.Sprintf("%+v", message))

	return nil
}
