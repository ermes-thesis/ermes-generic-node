package metadataManager

import (
	"bytes"
	dataManagerShared "dataManager/shared"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"metadataManager/types"
	"net/http"
	"os"
	"server/info"
	"strings"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
)

type PropagationOrigin int

func (p PropagationOrigin) String() string {
	switch p {
	case FROM_FATHER:
		return "FATHER"
	case FROM_CHILD:
		return "CHILD"
	case FROM_LOCAL:
		return "LOCAL"
	default:
		return "UNKNOWN"
	}
}

const (
	FROM_FATHER PropagationOrigin = iota
	FROM_CHILD
	FROM_LOCAL
)

type MetadataAction int

const (
	Add = iota
	Remove
)

type UpdateGroupResponse struct {
	Groups   []string `json:"groups"`
	Sequence uint64   `json:"sequence"`
}

type QueryPropagator interface {
	Propagate(functionName string, functionVersion string, templateParamsMappings string, groupToken string, geolocation string) (map[uint64]types.QueryResult, error)
}

type QueryPropagatorImpl struct {
	serverInfo *info.ServerInfo
	log        *logger.Logger
}

func NewQueryPropagator(serverInfo *info.ServerInfo, log *logger.Logger) QueryPropagator {
	return &QueryPropagatorImpl{
		serverInfo: serverInfo,
		log:        log,
	}
}

func (p *QueryPropagatorImpl) Propagate(functionName string, functionVersion string, templateParamsMappings string, groupToken string, geolocation string) (map[uint64]types.QueryResult, error) {
	p.log.Info("No geolocation provided in the request header.")

	var receiver string
	if p.serverInfo.CloudNode == nil || strings.HasPrefix(p.serverInfo.MetadataDomain, geolocation) {
		p.log.Info("No cloud node configured, forwarding to parent node.")
		receiver = p.serverInfo.Father.HostName
	} else {
		p.log.Info("Using cloud node %s for the request.\n", p.serverInfo.CloudNode.HostName)
		receiver = p.serverInfo.CloudNode.HostName
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/pvt-lookup", receiver), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when creating the http request: %s \n", err)
		return nil, err
	}
	req.Header.Set("X-ERMES-REQ-FUNCTION", functionName)
	req.Header.Set("X-ERMES-REQ-FUNCTION-VERSION", functionVersion)
	req.Header.Set("X-ERMES-SECRET", p.serverInfo.Secret)
	req.Header.Set("X-ERMES-GROUPS", groupToken)
	req.Header.Set("X-ERMES-POSITION", geolocation)
	req.Header.Set("X-ERMES-QUERY", templateParamsMappings)

	res, err := httpClient.CustomClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when contacting the cloud node: %s \n", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Error when contacting the cloud node: %s \n", res.Status)
		return nil, fmt.Errorf("error when contacting the cloud node: %s", res.Status)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when reading the response body: %s \n", err)
		return nil, err
	}

	var result map[uint64]types.QueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Fprintf(os.Stderr, "Error when unmarshalling the response body: %s \n", err)
		return nil, err
	}

	return result, nil
}

type CreateMetadataPropagator interface {
	Propagate(collectionId string, metadata model.Metadata, reqOrigin PropagationOrigin) error
}

type CreateMetadataPropagatorImpl struct {
	serverInfo *info.ServerInfo
	log        *logger.Logger
}

func NewCreateMetadataPropagator(serverInfo *info.ServerInfo, log *logger.Logger) *CreateMetadataPropagatorImpl {
	return &CreateMetadataPropagatorImpl{
		serverInfo: serverInfo,
		log:        log,
	}
}

func (p *CreateMetadataPropagatorImpl) Propagate(collectionId string, metadata model.Metadata, reqOrigin PropagationOrigin) error {
	p.log.Info("Starting to propagating groups update request...")

	switch reqOrigin {
	case FROM_FATHER:
		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(metadata.Geolocation, child.MetadataDomain) {
				err := p.sendHttpReqToDest(child.HostName, collectionId, metadata)
				if err != nil {
					p.log.Errorf("Error sending request to child: %v\n", err)
					return err
				}

				break
			}
		}
	case FROM_CHILD:
		if p.serverInfo.Father != nil {
			err := p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, metadata)
			if err != nil {
				p.log.Errorf("Error sending request to father : %v\n", err)
				return err
			}
		}
	case FROM_LOCAL:
		if p.serverInfo.Father != nil {
			err := p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, metadata)
			if err != nil {
				p.log.Errorf("Error sending request to father : %v\n", err)
				return err
			}
		}

		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(metadata.Geolocation, child.MetadataDomain) {
				err := p.sendHttpReqToDest(child.HostName, collectionId, metadata)
				if err != nil {
					p.log.Errorf("Error sending request to child : %v\n", err)
					return err
				}

				break
			}
		}
	}
	return nil
}

func (p *CreateMetadataPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, metadata model.Metadata) error {
	body, err := json.Marshal(metadata)
	if err != nil {
		p.log.Infof("Error marshalling metadata: %v", err)
		return err
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/pvt-create-metadata/%s", dest, collectionId), bytes.NewReader(body))
	if err != nil {
		p.log.Errorf("Error creating request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", p.serverInfo.Secret)
	req.Header.Set("X-ERMES-METADATA-DOMAIN", p.serverInfo.MetadataDomain)

	resp, err := httpClient.CustomClient.Do(req)
	if err != nil {
		p.log.Errorf("[%s] Error sending request: %v\n", p.serverInfo.MetadataDomain, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("error when contacting the node: %s", resp.Status)
	}
	return nil
}

type GroupUpdateRequestPropagator interface {
	Propagate(collectionId string, message GroupMessage) (*UpdateGroupResponse, error)
}

type GroupsUpdateRequestPropagatorImpl struct {
	serverInfo *info.ServerInfo
	log        *logger.Logger
}

func NewGroupUpdateRequestPropagator(serverInfo *info.ServerInfo, log *logger.Logger) *GroupsUpdateRequestPropagatorImpl {
	return &GroupsUpdateRequestPropagatorImpl{
		serverInfo: serverInfo,
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
	body, err := json.Marshal(message)
	if err != nil {
		p.log.Infof("Error marshalling metadata: %v", err)
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/pvt-request-update-groups/%s", dest, collectionId), bytes.NewReader(body))
	if err != nil {
		p.log.Errorf("Error creating request: %v", err)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", p.serverInfo.Secret)

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

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/pvt-propagate-groups/%s", dest, collectionId), bytes.NewReader(body))
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
