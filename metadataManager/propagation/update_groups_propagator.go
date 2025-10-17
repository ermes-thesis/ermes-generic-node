package propagation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"net/http"
	"server/info"
	"strings"
)

type UpdateGroupsPropagator interface {
	Propagate(collectionId string, message UpdateGroupResponse, geolocation string) error
}

type UpdateGroupsPropagatorImpl struct {
	serverInfo *info.ServerInfo
	log        *logger.Logger
}

func NewUpdateGroupsPropagator(serverInfo *info.ServerInfo, log *logger.Logger) *UpdateGroupsPropagatorImpl {
	return &UpdateGroupsPropagatorImpl{
		serverInfo: serverInfo,
		log:        log,
	}
}

func (p *UpdateGroupsPropagatorImpl) Propagate(collectionId string, message UpdateGroupResponse, geolocation string) error {
	p.log.Info("Starting to propagating groups update...")

	for _, child := range p.serverInfo.Children {
		if strings.HasPrefix(geolocation, child.MetadataDomain) {
			p.log.Infof("Forwarding to child node %s for the request.\n", child.HostName)
			return p.sendHttpReqToDest(child.HostName, collectionId, message)
		}
	}

	return errors.New("domain not found among children nodes")
}

func (p *UpdateGroupsPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, message UpdateGroupResponse) error {
	body, err := json.Marshal(message)
	if err != nil {
		p.log.Infof("Error marshalling metadata: %v", err)
		return err
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/%s/%s", dest, EndpointPropagateGroups, collectionId), bytes.NewReader(body))
	if err != nil {
		p.log.Errorf("Error creating request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", p.serverInfo.Secret)

	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		p.log.Errorf("[%s] Error sending request: %v\n", p.serverInfo.MetadataDomain, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error when contacting the node: %s", resp.Status)
	}

	return nil
}
