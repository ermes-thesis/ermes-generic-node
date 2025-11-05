package propagation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ermes-thesis/ermes-common/logger"
	"io"
	"net/http"
	"server"
)

type HTTPHelper struct {
	serverInfo *server.ServerInfo
	log        logger.Logger
}

func NewHTTPHelper(serverInfo *server.ServerInfo, log logger.Logger) *HTTPHelper {
	return &HTTPHelper{serverInfo, log}
}

func (h *HTTPHelper) CreateBasicPvtHttpRequest(method, host, endpoint, collectionId string, payload any) (*http.Request, error) {
	h.log.Debug("Creating basic private HTTP request", "method", method, "host", host, "endpoint", endpoint, "collection_id", collectionId, "payload", fmt.Sprintf("%+v", payload))

	var bodyReader io.Reader
	if payload != nil {
		body, err := json.Marshal(payload)
		if err != nil {
			h.log.Error("Failed to marshal request payload", "method", method, "host", host, "endpoint", endpoint, "collection_id", collectionId, "payload", fmt.Sprintf("%+v", payload), "error", err)
			return nil, err
		}

		bodyReader = bytes.NewReader(body)
	}

	url := fmt.Sprintf("http://%s%s/%s", host, endpoint, collectionId)
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		h.log.Error("Failed to create basic HTTP request object", "method", method, "host", host, "endpoint", endpoint, "collection_id", collectionId, "payload", fmt.Sprintf("%+v", payload), "error", err)
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", h.serverInfo.Secret)

	h.log.Debug("Basic HTTP request created successfully", "method", method, "url", url)

	return req, nil
}
