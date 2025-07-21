package server

import (
	"db"
	"fmt"
	"io"
	"metadataManager"
	"net/http"
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
