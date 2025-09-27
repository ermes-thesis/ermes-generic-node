package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"server/client"
	"server/clientrepo"
	"server/info"
)

type ServerType interface {
	SendGenericHTTPRequest(nodeTo string, pathTo string, messageContent any) (int, error)
	SendGenericHTTPRequestReturnResponse(nodeTo string, pathTo string, messageContent any) (*http.Response, error)
	AddCustomRoute(path string, isPrivate bool, handler http.HandlerFunc) error
}

type Server struct {
	*http.Server
	ServerInfo *info.ServerInfo
	Logger     io.Writer
	Handler    *http.ServeMux
	ClientRepo *clientrepo.ClientRepo
}

func NewServer(logger io.Writer, serverInfo *info.ServerInfo) (*Server, error) {

	fmt.Fprintf(logger, "Creating a new Server instance... \n")

	mux := http.NewServeMux()

	fmt.Fprintf(logger, "Mux created correctly \n")

	fmt.Fprintf(logger, "Creating server data structure... ")

	ser := &Server{
		ServerInfo: serverInfo,
		Logger:     logger,
		ClientRepo: clientrepo.CreateClientRepo(),
		Handler:    mux,
		Server: &http.Server{
			Addr:    net.JoinHostPort("localhost", serverInfo.Port),
			Handler: mux,
		},
	}

	fmt.Fprintf(logger, "done \n")

	fmt.Fprintf(logger, "Adding correct routes to the server... ")

	ser.compileRoutes()

	fmt.Fprintf(logger, "done \n")

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

	fmt.Fprintf(s.Logger, "got a generic request %s \n", pathTo)

	// Check if the node is valid TODO: refactor with slices if possible

	ok := false

	for _, elem := range s.ServerInfo.Children {
		if elem.HostName == nodeTo {
			ok = true
		}
	}

	if s.ServerInfo.Father.HostName != nodeTo && !ok {
		fmt.Fprintf(s.Logger, "request should be sent to %s , which is not a known node. \n", nodeTo)

		return nil, errors.New("node not in the hierarchy, cannot send request")
	}

	reqUrl := fmt.Sprintf("http://%s%s", nodeTo, pathTo)

	fmt.Fprintf(s.Logger, "Sending request to node: %s \n", reqUrl)

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

	fmt.Fprintf(s.Logger, "Request is ready, now sending it ...\n")

	res, err := client.CustomClient.Do(req)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when contacting the node: %s \n", err)

		return nil, err
	}

	// If everything went well return the response.
	return res, nil
}
