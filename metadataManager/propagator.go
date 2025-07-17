package metadataManager

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"server/info"
)

type QueryPropagator struct {
	serverInfo *info.ServerInfo
	logger     *io.Writer
}

func NewQueryPropagator(serverInfo *info.ServerInfo, logger *io.Writer) *QueryPropagator {
	return &QueryPropagator{
		serverInfo: serverInfo,
		logger:     logger,
	}
}

func (p *QueryPropagator) Propagate(queryParams string, groupToken string, geolocation string) (map[string][]CollectionInfo, error) {
	fmt.Fprintf(*p.logger, "No geolocation provided in the request header.\n")

	var receiver string
	if p.serverInfo.CloudNode == nil {
		fmt.Fprintf(*p.logger, "No cloud node configured, forwarding to parent node.\n")
		receiver = p.serverInfo.Father.Address
	} else {
		fmt.Fprintf(*p.logger, "Using cloud node %s for the request.\n", p.serverInfo.CloudNode)
		receiver = p.serverInfo.CloudNode.Address
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%s/lookup", receiver), nil)
	req.Header.Set("X-ERMES-SECRET", p.serverInfo.Secret)
	req.Header.Set("X-ERMES-GROUPS", groupToken)
	req.Header.Set("X-ERMES-POSITION", geolocation)
	req.Header.Set("X-ERMES-QUERY", queryParams)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when creating the http request: %s \n", err)
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
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

	var result map[string][]CollectionInfo
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Fprintf(os.Stderr, "Error when unmarshalling the response body: %s \n", err)
		return nil, err
	}

	return result, nil
}

type MetadataPropagator struct {
	serverInfo *info.ServerInfo
}

func NewMetadataPropagator(serverInfo *info.ServerInfo) *MetadataPropagator {
	return &MetadataPropagator{
		serverInfo: serverInfo,
	}
}

func (p *MetadataPropagator) Propagate() {

}
