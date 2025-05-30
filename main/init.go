package main

import (
	"errors"
	"fmt"
	"info"
	"io"
	"net/http"
	"routes"
	"strconv"
)

type Server struct {
	serverinfo info.Serverinfo
	port       string
	logger     io.Writer
	handler    http.Handler
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

	serverinfo := info.NewServerInfo(
		name,
		parent,
		children,
	)

	fmt.Fprintf(logger, "done \n")

	fmt.Fprintf(logger, "Creating server data structure... ")

	ser := &Server{
		serverinfo: serverinfo,
		port:       args[1],
		logger:     logger,
		handler:    mux,
	}

	fmt.Fprintf(logger, "done \n")

	fmt.Fprintf(logger, "Adding correct routes to the server... ")

	routes.AddRoutes(mux, &logger, &serverinfo)

	fmt.Fprintf(logger, "done \n")

	return ser, nil

}
