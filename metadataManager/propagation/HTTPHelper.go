package propagation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ermes-thesis/ermes-common/logger"
	"io"
	"net/http"
	"server/info"
)

type HTTPHelper struct {
	serverInfo *info.ServerInfo
	log        *logger.Logger
}

func NewHTTPHelper(serverInfo *info.ServerInfo, log *logger.Logger) *HTTPHelper {
	return &HTTPHelper{serverInfo, log}
}

func (h *HTTPHelper) CreateBasicPvtHttpRequest(method, host, endpoint, collectionId string, payload any) (*http.Request, error) {
	var bodyReader io.Reader

	if payload != nil {
		body, err := json.Marshal(payload)
		if err != nil {
			h.log.Infof("Error marshalling metadata: %v", err)
			return nil, err
		}
		bodyReader = bytes.NewReader(body)
	}

	url := fmt.Sprintf("http://%s%s/%s", host, endpoint, collectionId)
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		h.log.Errorf("Error creating request: %v", err)
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", h.serverInfo.Secret)

	return req, nil
}
