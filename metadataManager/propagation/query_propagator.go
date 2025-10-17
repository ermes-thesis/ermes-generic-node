package propagation

import (
	"encoding/json"
	"fmt"
	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
	"io"
	"metadataManager/types"
	"net/http"
	"os"
	"server/info"
	"strings"
)

type QueryPropagator interface {
	Propagate(functionName string, functionVersion string, templateParamsMappings string, groupToken string, geolocation string) (map[uint64]types.QueryResult, error)
}

type QueryPropagatorImpl struct {
	serverInfo *info.ServerInfo
	httpHelper *HTTPHelper
	log        *logger.Logger
}

func NewQueryPropagator(serverInfo *info.ServerInfo, log *logger.Logger) QueryPropagator {
	return &QueryPropagatorImpl{
		serverInfo: serverInfo,
		httpHelper: NewHTTPHelper(serverInfo, log),
		log:        log,
	}
}

func (p *QueryPropagatorImpl) Propagate(functionName, functionVersion, templateParamsMappings, groupToken, geolocation string) (map[uint64]types.QueryResult, error) {
	p.log.Info("No geolocation provided in the request header.")

	var sendTo string
	if p.serverInfo.CloudNode == nil || strings.HasPrefix(p.serverInfo.MetadataDomain, geolocation) {
		p.log.Info("No cloud node configured, forwarding to parent node.")
		sendTo = p.serverInfo.Father.HostName
	} else {
		p.log.Info("Using cloud node %s for the request.\n", p.serverInfo.CloudNode.HostName)
		sendTo = p.serverInfo.CloudNode.HostName
	}

	return p.sendHttpReqToDest(sendTo, functionName, functionVersion, templateParamsMappings, groupToken, geolocation)
}

func (p *QueryPropagatorImpl) sendHttpReqToDest(dest, functionName, functionVersion, templateParamsMappings, groupToken, geolocation string) (map[uint64]types.QueryResult, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/%s", dest, EndpointLookup), nil)
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
