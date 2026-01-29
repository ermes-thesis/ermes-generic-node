package propagation

import (
	"encoding/json"
	"fmt"
	"net/http"
	"server"
	"strings"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/model"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
)

type QueryPropagator interface {
	Propagate(functionName string, functionVersion string, templateParamsMappings string, groupToken string, geolocation string) (map[uint64]model.QueryResult, error)
}

type QueryPropagatorImpl struct {
	serverInfo *server.ServerInfo
	httpHelper *HTTPHelper
	log        logger.Logger
}

func NewQueryPropagator(serverInfo *server.ServerInfo, log logger.Logger) QueryPropagator {
	return &QueryPropagatorImpl{
		serverInfo: serverInfo,
		httpHelper: NewHTTPHelper(serverInfo, log),
		log:        log,
	}
}

func (p *QueryPropagatorImpl) Propagate(functionName, functionVersion, templateParamsMappings, groupToken, geolocation string) (map[uint64]model.QueryResult, error) {
	p.log.Info("Starting query propagation", "fn_name", functionName, "fn_version", functionVersion, "node_geolocation", p.serverInfo.MetadataDomain, "query_geolocation", geolocation)

	var sendTo string
	if p.serverInfo.CloudNode == nil || (geolocation != "" && strings.HasPrefix(p.serverInfo.MetadataDomain, geolocation)) {
		p.log.Info("No cloud node configured or node geolocation is a subdomain of the query geolocation, forwarding query to father node.", "node_geolocation", p.serverInfo.MetadataDomain, "query_geolocation", geolocation, "father_hostname", p.serverInfo.Father.HostName, "father_geolocation", p.serverInfo.Father.MetadataDomain)
		sendTo = p.serverInfo.Father.HostName
	} else {
		p.log.Info("Forwarding query to cloud node", "node_geolocation", p.serverInfo.MetadataDomain, "query_geolocation", geolocation, "cloud_hostname", p.serverInfo.CloudNode.HostName, "cloud_geolocation", p.serverInfo.CloudNode.MetadataDomain)
		sendTo = p.serverInfo.CloudNode.HostName
	}

	return p.sendHttpReqToDest(sendTo, functionName, functionVersion, templateParamsMappings, groupToken, geolocation)
}

func (p *QueryPropagatorImpl) sendHttpReqToDest(dest, functionName, functionVersion, templateParamsMappings, groupToken, geolocation string) (map[uint64]model.QueryResult, error) {
	url := "http://" + dest + EndpointLookup

	p.log.Debug("Creating HTTP request for query propagation", "fn_name", functionName, "fn_version", functionVersion, "destination", dest, "template_parameters_mappings", templateParamsMappings, "query_geolocation", geolocation)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		p.log.ErrorfWith("Failed to create basic HTTP request for '%s' path", []any{EndpointLookup}, "fn_name", functionName, "fn_version", functionVersion, "destination", dest, "template_parameters_mappings", templateParamsMappings, "query_geolocation", geolocation, "error", err)
		return nil, err
	}
	req.Header.Set("X-ERMES-REQ-FUNCTION", functionName)
	req.Header.Set("X-ERMES-REQ-FUNCTION-VERSION", functionVersion)
	req.Header.Set("X-ERMES-SECRET", p.serverInfo.Secret)
	req.Header.Set("X-ERMES-GROUPS", groupToken)
	req.Header.Set("X-ERMES-POSITION", geolocation)
	req.Header.Set("X-ERMES-QUERY", templateParamsMappings)

	if p.log.Enabled(logger.DebugLevel) {
		p.log.Debug("Sending HTTP request to destination node", "url", req.URL, "headers", fmt.Sprintf("%+v", req.Header))
	}

	res, err := httpClient.CustomClient.Do(req)
	if err != nil {
		p.log.ErrorfWith("Failed to send request to remote node for '%s' path", []any{EndpointLookup}, "fn_name", functionName, "fn_version", functionVersion, "destination", dest, "template_parameters_mappings", templateParamsMappings, "query_geolocation", geolocation, "error", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		p.log.ErrorfWith("Unexpected status code received while contacting '%s' path", []any{EndpointLookup}, "fn_name", functionName, "fn_version", functionVersion, "destination", dest, "template_parameters_mappings", templateParamsMappings, "query_geolocation", geolocation, "received_status_code", res.Status, "expected_status_code", http.StatusOK)
		return nil, fmt.Errorf("unexpected status code %d from %s", res.StatusCode, dest)
	}

	var result map[uint64]model.QueryResult
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		p.log.Error("Failed to decode response from query propagation", "fn_name", functionName, "fn_version", functionVersion, "destination", dest, "template_parameters_mapping", templateParamsMappings, "query_geolocation", geolocation, "error", err)
		return nil, err
	}

	if p.log.Enabled(logger.InfoLevel) {
		p.log.Info("Query successfully propagated", "fn_name", functionName, "fn_version", functionVersion, "query_results", fmt.Sprintf("%+v", result))
		p.log.Debug("Query successfully propagated", "destination", dest, "template_parameters_mappings", templateParamsMappings, "query_geolocation", geolocation)
	}
	return result, nil
}
