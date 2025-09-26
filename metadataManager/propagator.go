package metadataManager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

type QueryPropagator interface {
	Propagate(functionName string, functionVersion string, templateParamsMappings string, groupToken string, geolocation string) (map[uint64]utils.QueryResult, error)
}

type QueryPropagatorImpl struct {
	serverInfo *info.ServerInfo
	logger     *io.Writer
}

func NewQueryPropagator(serverInfo *info.ServerInfo, logger *io.Writer) QueryPropagator {
	return &QueryPropagatorImpl{
		serverInfo: serverInfo,
		logger:     logger,
	}
}

func (p *QueryPropagatorImpl) Propagate(functionName string, functionVersion string, templateParamsMappings string, groupToken string, geolocation string) (map[uint64]utils.QueryResult, error) {
	fmt.Fprintf(*p.logger, "No geolocation provided in the request header.\n")

	var receiver string
	if p.serverInfo.CloudNode == nil || strings.HasPrefix(p.serverInfo.MetadataDomain, geolocation) {
		fmt.Fprintf(*p.logger, "No cloud node configured, forwarding to parent node.\n")
		receiver = p.serverInfo.Father.HostName
	} else {
		fmt.Fprintf(*p.logger, "Using cloud node %s for the request.\n", p.serverInfo.CloudNode.HostName)
		receiver = p.serverInfo.CloudNode.HostName
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/lookup", receiver), nil)
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
	Propagate(collectionId string, metadata utils.Metadata, reqOrigin PropagationOrigin) error
}

type MetadataPropagatorImpl struct {
	serverInfo *info.ServerInfo
	logger     *io.Writer
}

func NewMetadataPropagator(serverInfo *info.ServerInfo, logger *io.Writer) *MetadataPropagatorImpl {
	return &MetadataPropagatorImpl{
		serverInfo: serverInfo,
		logger:     logger,
	}
}

func (p *MetadataPropagatorImpl) Propagate(collectionId string, metadata utils.Metadata, reqOrigin PropagationOrigin) error {
	fmt.Fprintf(*p.logger, "Starting to propagating metadata...\n")

	body, err := json.Marshal(metadata)
	if err != nil {
		fmt.Fprintf(*p.logger, "Error marshalling metadata: %v\n", err)
		return err
	}

	switch reqOrigin {
	case FROM_FATHER:
		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(metadata.Geolocation, child.MetadataDomain) {
				err = p.sendHttpReqToDest(child.HostName, collectionId, body)
				if err != nil {
					fmt.Fprintf(*p.logger, "Error sending request to child : %v\n", err)
					return err
				}

				break
			}
		}
	case FROM_CHILD:
		if p.serverInfo.Father != nil {
			err = p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, body)
			if err != nil {
				fmt.Fprintf(*p.logger, "Error sending request to father : %v\n", err)
				return err
			}
		}
	case FROM_LOCAL:
		if p.serverInfo.Father != nil {
			err = p.sendHttpReqToDest(p.serverInfo.Father.HostName, collectionId, body)
			if err != nil {
				fmt.Fprintf(*p.logger, "Error sending request to father : %v\n", err)
				return err
			}
		}

		for _, child := range p.serverInfo.Children {
			if strings.HasPrefix(metadata.Geolocation, child.MetadataDomain) {
				err = p.sendHttpReqToDest(child.HostName, collectionId, body)
				if err != nil {
					fmt.Fprintf(*p.logger, "Error sending request to child : %v\n", err)
					return err
				}

				break
			}
		}
	}
	return nil
}

func (p *MetadataPropagatorImpl) sendHttpReqToDest(dest string, collectionId string, body []byte) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/create_metadata/%s", dest, collectionId), bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(*p.logger, "Error creating request: %v\n", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", p.serverInfo.Secret)
	req.Header.Set("X-ERMES-METADATA-DOMAIN", p.serverInfo.MetadataDomain)

	resp, err := client.CustomClient.Do(req)
	if err != nil {
		fmt.Fprintf(*p.logger, "[%s] Error sending request: %v\n", p.serverInfo.MetadataDomain, err)
		return err
	}
	defer resp.Body.Close()

	return nil
}
