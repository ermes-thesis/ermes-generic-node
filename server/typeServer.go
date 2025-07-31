package server

import (
	"bytes"
	"db"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"metadataManager"
	"net/http"
	"os"
	"server/clientrepo"
	"server/info"
)

type ServerType interface {
	SendGenericHTTPRequest(nodeTo string, pathTo string, messageContent interface{}) (int, error)
	compileRoutes()
	AddCustomRoute(path string, isPrivate bool, handler http.HandlerFunc) error
}

type Server struct {
	Serverinfo      *info.ServerInfo
	Port            string
	Logger          io.Writer
	Handler         *http.ServeMux
	ClientRepo      *clientrepo.ClientRepo
	RedisClient     db.GenericDB
	MetadataManager metadataManager.MetadataManager
}

func NewServer(logger io.Writer, serverInfo *info.ServerInfo, redisClient db.GenericDB, metadataManager metadataManager.MetadataManager) (*Server, error) {

	fmt.Fprintf(logger, "Creating a new Server instance... \n")

	mux := http.NewServeMux()

	fmt.Fprintf(logger, "Mux created correctly \n")

	fmt.Fprintf(logger, "Creating server data structure... ")

	ser := &Server{
		Serverinfo:      serverInfo,
		Port:            serverInfo.Address,
		Logger:          logger,
		Handler:         mux,
		ClientRepo:      clientrepo.CreateClientRepo(),
		RedisClient:     redisClient,
		MetadataManager: metadataManager,
	}

	fmt.Fprintf(logger, "done \n")

	fmt.Fprintf(logger, "Adding correct routes to the server... ")

	ser.compileRoutes()

	fmt.Fprintf(logger, "done \n")

	return ser, nil

}

// Method to send a generic data transfer request. It returns both the status code of the response and the error.
func (this *Server) SendGenericHTTPRequest(nodeTo string, pathTo string, messageContent interface{}) (int, error) {
	// Check if the node to is correctly formatted

	fmt.Fprintf(this.Logger, "got a generic request %s \n", pathTo)

	// Check if the node is valid TODO: refactor with slices if possible

	ok := false

	for _, elem := range this.Serverinfo.Children {
		if elem.Address == nodeTo {
			ok = true
		}
	}

	if this.Serverinfo.Father.Address != nodeTo && !ok {
		fmt.Fprintf(this.Logger, "request should be sent to %s , which is not a known node. \n", nodeTo)

		return -1, errors.New("node not in the hierarchy, cannot send request")
	}

	reqUrl := fmt.Sprintf("http://localhost:%s%s", nodeTo, pathTo)

	fmt.Fprintf(this.Logger, "Sending request to node: %s \n", reqUrl)

	jsonBody, err := json.Marshal(messageContent)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when marshalling the body of the request: %s \n", err)

		return -1, err
	}

	// Create the request

	req, err := http.NewRequest(http.MethodPost, reqUrl, bytes.NewBuffer(jsonBody))

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when creating the http request: %s \n", err)

		return -1, err
	}

	// Prepare the headers

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", this.Serverinfo.Father.Address)

	fmt.Fprintf(this.Logger, "Request is ready, now sending it ...\n")

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when contacting the node: %s \n", err)

		return -1, err
	}

	// If everything went well return the response status code.
	return res.StatusCode, nil
}
