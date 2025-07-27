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

// Add a new method to the server which is responsible for creating a new request to move data

func (this *Server) CreateDataMovementHTTPRequest(nodeTo string, key string, value string) error {

	// Check if the node to is correctly formatted

	fmt.Fprintf(this.Logger, "got a request to send the data to another node \n")

	// Check if the node is valid TODO: refactor with slices if possible

	ok := false

	for _, elem := range this.Serverinfo.Children {
		if elem.Address == nodeTo {
			ok = true
		}
	}

	if this.Serverinfo.Father.Address != nodeTo && !ok {

		fmt.Fprintf(this.Logger, "request should be sent to %s , which is not a known node. \n", nodeTo)

		return errors.New("node not in the hierarchy, cannot send request")
	}

	reqUrl := fmt.Sprintf("http://localhost:%s/pvt-data-transfer-request", nodeTo)

	fmt.Fprintf(this.Logger, "Sending request to node: %s \n", reqUrl)

	// Wrap the information in the serializable type

	reqBody := info.DataMovementRequestConstructor(nodeTo, this.Serverinfo.Address, key, value)

	// Convert to json

	jsonBody, err := json.Marshal(reqBody)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when marshalling the body of the request: %s \n", err)

		return err
	}

	// Create the request

	req, err := http.NewRequest(http.MethodPost, reqUrl, bytes.NewBuffer(jsonBody))

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when creating the http request: %s \n", err)

		return err
	}

	// Prepare the headers

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ERMES-SECRET", this.Serverinfo.Father.Address)

	fmt.Fprintf(this.Logger, "Request is ready, now sending it ...\n")

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when contacting the node: %s \n", err)

		return err
	}

	// TODO; what to do with the response? I should check if it was possible to perform it

	if res.StatusCode == http.StatusInternalServerError {
		// Not enough space
		return errors.New("it was not possible to insert the data in the node as there was not enough space (i guess?)")
	}

	return nil
}
