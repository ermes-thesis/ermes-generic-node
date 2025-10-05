package caches

import (
	"caches/metadata"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"server/client"

	"github.com/ermes-thesis/ermes-common/model"
)

type FunctionBundle struct {
	Binary    []byte
	Metadata  metadata.FunctionMetadata
	Templates map[string]model.QueryTemplate
}

type Fetcher[T any] interface {
	Fetch(functionName string, functionVersion string) (T, error)
	FetchLatest(functionName string, cachedVersion string) (T, error)
}

type RemoteBundleFetcher struct {
	BaseURL     string
	ErmesSecret string
}

func NewRemoteBundleFetcher(baseURL string, ermesSecret string) *RemoteBundleFetcher {
	return &RemoteBundleFetcher{BaseURL: baseURL, ErmesSecret: ermesSecret}
}

func (f *RemoteBundleFetcher) Fetch(functionName string, functionVersion string) (*FunctionBundle, error) {
	url := fmt.Sprintf("%s/retrieve-function/%s/%s", f.BaseURL, functionName, functionVersion)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.CustomClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch function: %s", string(body))
	}

	var fnBundle FunctionBundle
	err = json.Unmarshal(body, &fnBundle)
	return &fnBundle, err
}

func (f *RemoteBundleFetcher) FetchLatest(functionName string, cachedVersion string) (*FunctionBundle, error) {
	url := fmt.Sprintf("%s/retrieve-function/%s", f.BaseURL, functionName)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("If-None-Match", cachedVersion)

	req.Header.Set("X-ERMES-SECRET", f.ErmesSecret)

	resp, err := client.CustomClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotModified {
		return nil, nil
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch function: %s", string(body))
	}

	var fnBundle FunctionBundle
	err = json.Unmarshal(body, &fnBundle)
	return &fnBundle, err
}
