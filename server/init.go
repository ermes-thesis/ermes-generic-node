package server

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"server/clientrepo"
	"server/info"
	"strconv"
)

type Server struct {
	Serverinfo info.Serverinfo
	Port       string
	Logger     io.Writer
	Handler    *http.ServeMux
	ClientRepo *clientrepo.ClientRepo
}

func NewServer(args []string, logger io.Writer) (*Server, error) {

	fmt.Fprintf(logger, "Creating a new Server instance... \n")

	mux := http.NewServeMux()

	fmt.Fprintf(logger, "Mux created correctly \n")

	fmt.Fprintf(logger, "Parsing input arguments... ")

	// Extract the information of port, name, parent, children in this order

	if len(args) < 5 {
		return nil, errors.New("too few arguments in the program, cannot launch correctly")
	}

	// Parse first argument
	port, err := strconv.Atoi(args[1])

	if err != nil || port < 1 {
		return nil, errors.New("first argument of the program must be a non-negative integer")
	}

	// Parse info about the name, parent and children
	// TODO: check if convention on names is respected (which we do not have yet)

	// NB: these operations just copy the references !
	name := args[2]
	parent := args[3]
	children := args[4:]

	serverinfo := info.Serverinfo{
		Name:     name,
		Father:   parent,
		Children: children,
		Secret:   "ermes-secret", // For the moment leave it hardcoded
	}

	fmt.Fprintf(logger, "done \n")

	fmt.Fprintf(logger, "Creating server data structure... ")

	ser := &Server{
		Serverinfo: serverinfo,
		Port:       args[1],
		Logger:     logger,
		Handler:    mux,
		ClientRepo: clientrepo.CreateClientRepo(),
	}

	fmt.Fprintf(logger, "done \n")

	fmt.Fprintf(logger, "Adding correct routes to the server... ")

	ser.compileRoutes()

	fmt.Fprintf(logger, "done \n")

	return ser, nil

}
