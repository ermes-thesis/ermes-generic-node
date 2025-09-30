package metadataManager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"logger"
	"metadataManager/utils"
	"net/http"
	"os"
	"server/client"
	"server/info"
	"strings"
)

type PropagationOrigin int

const (
	FROM_FATHER PropagationOrigin = iota
	FROM_CHILD
	FROM_LOCAL
)

type MetadataAction int

const (
	CREATE MetadataAction = iota
	UPDATE
	DELETE
)

type Propagable interface {
	IsPropagable() bool
}

type QueryPropagator interface {
	Propagate(functionName string, functionVersion string, templateParamsMappings string, groupToken string, geolocation string) (map[uint64]utils.QueryResult, error)
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

func (p *QueryPropagatorImpl) Propagate(functionName string, functionVersion string, templateParamsMappings string, groupToken string, geolocation string) (map[uint64]utils.QueryResult, error) {
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

	res, err := client.CustomClient.Do(req)
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

	var result map[uint64]utils.QueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Fprintf(os.Stderr, "Error when unmarshalling the response body: %s \n", err)
		return nil, err
	}

	return result, nil
}

type MetadataPropagator interface {
	Propagate(collectionId string, obj Propagable, geolocation string, reqOrigin PropagationOrigin, action MetadataAction) error
}

type MetadataPropagatorImpl struct {
	serverInfo *info.ServerInfo
	log        *logger.Logger
}

func NewMetadataPropagator(serverInfo *info.ServerInfo, log *logger.Logger) *MetadataPropagatorImpl {
	return &MetadataPropagatorImpl{
		serverInfo: serverInfo,
		log:        log,
	}
}

func (p *MetadataPropagatorImpl) Propagate(collectionId string, obj Propagable, geolocation string, reqOrigin PropagationOrigin, action MetadataAction) error {
	p.log.Info("Starting to propagating metadata...")

	body, err := json.Marshal(obj)
	if err != nil {
		p.log.Infof("Error marshalling metadata: %v", err)
		return err
	}

	switch reqOrigin {
	case FROM_FATHER:
		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(geolocation, child.MetadataDomain) {
				err = p.sendHttpReqToDest(child.HostName, collectionId, body, action)
				if err != nil {
					p.log.Errorf("Error sending request to child: %v\n", err)
					return err
				}

				break
			}
		}
	case FROM_CHILD:
		if p.serverInfo.Father != nil {
			err = p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, body, action)
			if err != nil {
				p.log.Errorf("Error sending request to father : %v\n", err)
				return err
			}
		}
	case FROM_LOCAL:
		if p.serverInfo.Father != nil {
			err = p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, body, action)
			if err != nil {
				p.log.Errorf("Error sending request to father : %v\n", err)
				return err
			}
		}

		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(geolocation, child.MetadataDomain) {
				err = p.sendHttpReqToDest(child.HostName, collectionId, body, action)
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

func (p *MetadataPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, body []byte, action MetadataAction) error {
	var endpoint string
	switch action {
	case CREATE:
		endpoint = "pvt-create-metadata"
	case UPDATE:
		endpoint = "pvt-add-group"
	case DELETE:
		endpoint = "pvt-remove-group"
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/%s/%s", dest, endpoint, collectionId), bytes.NewReader(body))
	if err != nil {
		p.log.Errorf("Error creating request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", p.serverInfo.Secret)
	req.Header.Set("X-ERMES-METADATA-DOMAIN", p.serverInfo.MetadataDomain)

	resp, err := client.CustomClient.Do(req)
	if err != nil {
		p.log.Errorf("[%s] Error sending request: %v\n", p.serverInfo.MetadataDomain, err)
		return err
	}
	defer resp.Body.Close()

	return nil
}
