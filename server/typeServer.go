package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"

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
	ServerInfo *ServerInfo
	logger     logger.Logger
	Handler    *http.ServeMux
}

func NewServer(log logger.Logger, serverInfo *ServerInfo) (*Server, error) {
	log.Info("Creating a new Server instance...")

	mux := http.NewServeMux()

	log.Debug("Creating server data structure...")

	ser := &Server{
		ServerInfo: serverInfo,
		logger:     log,
		Handler:    mux,
		Server: &http.Server{
			Addr:    net.JoinHostPort("", serverInfo.Port),
			Handler: mux,
		},
	}

	log.Debug("Adding correct routes to the server...")

	if err := ser.compileRoutes(); err != nil {
		log.Error("Failed to compile routes", "server", fmt.Sprintf("%+v", ser.ServerInfo), "error", err)
		return nil, err
	}

	log.Info("New Server instance created successfully", "server", fmt.Sprintf("%+v", ser.ServerInfo))
	return ser, nil
}

// SendGenericHTTPRequest Method to send a generic data transfer request. It returns both the status code of the response and the error.
func (s *Server) SendGenericHTTPRequest(nodeTo string, pathTo string, messageContent interface{}) (int, error) {
	res, err := s.SendGenericHTTPRequestReturnResponse(nodeTo, pathTo, messageContent)
	if err != nil {
		s.logger.Error("Sending HTTP request failed", "to_node", nodeTo, "path", pathTo, "content", fmt.Sprintf("%+v", messageContent), "error", err)
		return -1, err
	}
	return res.StatusCode, nil
}

func (s *Server) SendGenericHTTPRequestReturnResponse(nodeTo string, pathTo string, messageContent interface{}) (*http.Response, error) {
	s.logger.Info("Sending HTTP request", "to_node", nodeTo, "path", pathTo, "content", fmt.Sprintf("%+v", messageContent))

	ok := !s.ServerInfo.Stateful

	for _, elem := range s.ServerInfo.Children {
		if elem.HostName == nodeTo {
			ok = true
		}
	}

	if s.ServerInfo.Father != nil && s.ServerInfo.Father.HostName != nodeTo && !ok {
		s.logger.WarnfWith("Request should be sent to %s, which is not a known node.", []any{nodeTo}, "server_info", fmt.Sprintf("%+v", s.ServerInfo))
		return nil, errors.New("node not in the hierarchy, cannot send request")
	}

	reqUrl := fmt.Sprintf("http://%s%s", nodeTo, pathTo)

	jsonBody, err := json.Marshal(messageContent)
	if err != nil {
		s.logger.Error("Error when marshalling the body of the request", "to_node", nodeTo, "path", pathTo, "content", fmt.Sprintf("%+v", messageContent), "error", err)
		return nil, err
	}

	// Create the request
	req, err := http.NewRequest(http.MethodPost, reqUrl, bytes.NewBuffer(jsonBody))
	if err != nil {
		s.logger.Error("Error when creating the http request", "to_node", nodeTo, "path", pathTo, "content", fmt.Sprintf("%+v", messageContent), "error", err)
		return nil, err
	}

	// Prepare the headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", s.ServerInfo.Secret)

	s.logger.Debug("Request is ready, now sending it ...")
	res, err := httpClient.CustomClient.Do(req)
	if err != nil {
		s.logger.Error("Error when contacting the node", "to_node", nodeTo, "path", pathTo, "content", fmt.Sprintf("%+v", messageContent), "error", err)
		return nil, err
	}

	s.logger.Info("HTTP request sent successfully", "to_node", nodeTo, "path", pathTo, "content", fmt.Sprintf("%+v", messageContent))
	// If everything went well return the response.
	return res, nil
}
