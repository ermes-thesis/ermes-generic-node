package caches

import (
	"caches/metadata"
	"encoding/json"
	"fmt"
	"io"
	"metadataManager/utils"
	"net/http"
	"strings"
)

type FunctionBundle struct {
	Binary    []byte
	Templates map[string]utils.Query
	Metadata  metadata.FunctionMetadata
}

type Fetcher[T any] interface {
	Fetch(identifier string) (T, error)
}

type RemoteBundleFetcher struct {
	BaseURL string
}

func (f *RemoteBundleFetcher) Fetch(functionName string) (*FunctionBundle, error) {
	//TODO: Fix with the right decision of how taking functionName + version
	parts := strings.SplitN(functionName, "@", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("Invalid function name %s, expected name:version", functionName)
	}
	name, version := parts[0], parts[1]

	url := fmt.Sprintf("%s/retrieve-function/%s/%s", f.BaseURL, name, version)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var fnDetails map[string]any
	err = json.Unmarshal(body, &fnDetails)
	if err != nil {
		return nil, err
	}
	return &FunctionBundle{
		Binary:    fnDetails["function"].([]byte),
		Templates: fnDetails["queries"].(map[string]utils.Query),
		Metadata:  fnDetails["metadata"].(metadata.FunctionMetadata),
	}, nil
}

func NewRemoteBundleFetcher(baseURL string) *RemoteBundleFetcher {
	return &RemoteBundleFetcher{BaseURL: baseURL}
}
