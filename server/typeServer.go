package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"server/clientrepo"
	"server/info"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/ermes-thesis/ermes-common/utils/httpClient"
)

type ServerType interface {
	SendGenericHTTPRequest(nodeTo string, pathTo string, messageContent any) (int, error)
	SendGenericHTTPRequestReturnResponse(nodeTo string, pathTo string, messageContent any) (*http.Response, error)
	AddCustomRoute(path string, isPrivate bool, handler http.HandlerFunc) error
}

type Server struct {
	*http.Server
	ServerInfo *info.ServerInfo
	logger     *logger.Logger
	Handler    *http.ServeMux
	ClientRepo *clientrepo.ClientRepo
}

func NewServer(log *logger.Logger, serverInfo *info.ServerInfo) (*Server, error) {
	log.Info("Creating a new Server instance...")

	mux := http.NewServeMux()

	log.Info("Mux created correctly")

	log.Info("Creating server data structure...")

	ser := &Server{
		ServerInfo: serverInfo,
		logger:     log,
		ClientRepo: clientrepo.CreateClientRepo(),
		Handler:    mux,
		Server: &http.Server{
			Addr:    net.JoinHostPort("localhost", serverInfo.Port),
			Handler: mux,
		},
	}

	log.Info("done")

	log.Info("Adding correct routes to the server...")

	ser.compileRoutes()

	log.Info("done")

	return ser, nil

}

// SendGenericHTTPRequest Method to send a generic data transfer request. It returns both the status code of the response and the error.
func (s *Server) SendGenericHTTPRequest(nodeTo string, pathTo string, messageContent interface{}) (int, error) {
	res, err := s.SendGenericHTTPRequestReturnResponse(nodeTo, pathTo, messageContent)
	if err != nil {
		return -1, err
	}
	return res.StatusCode, nil
}

func (s *Server) SendGenericHTTPRequestReturnResponse(nodeTo string, pathTo string, messageContent interface{}) (*http.Response, error) {
	// Check if the node to is correctly formatted

	s.logger.Infof("got a generic request %s", pathTo)

	// Check if the node is valid TODO: refactor with slices if possible

	ok := false

	for _, elem := range s.ServerInfo.Children {
		if elem.HostName == nodeTo {
			ok = true
		}
	}

	if s.ServerInfo.Father.HostName != nodeTo && !ok {
		s.logger.Infof("request should be sent to %s , which is not a known node.", nodeTo)

		return nil, errors.New("node not in the hierarchy, cannot send request")
	}

	reqUrl := fmt.Sprintf("http://%s%s", nodeTo, pathTo)

	s.logger.Infof("Sending request to node: %s \n", reqUrl)

	jsonBody, err := json.Marshal(messageContent)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when marshalling the body of the request: %s \n", err)

		return nil, err
	}

	// Create the request

	req, err := http.NewRequest(http.MethodPost, reqUrl, bytes.NewBuffer(jsonBody))

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when creating the http request: %s \n", err)

		return nil, err
	}

	// Prepare the headers

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", s.ServerInfo.Secret)

	s.logger.Info("Request is ready, now sending it ...")

	res, err := httpClient.CustomClient.Do(req)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when contacting the node: %s \n", err)

		return nil, err
	}

	// If everything went well return the response.
	return res, nil
}
