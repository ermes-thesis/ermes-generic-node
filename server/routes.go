package server

import (
	"errors"
	"fmt"
	"net/http"
)

// Method to add pre-defined routes to the server.
func (s *Server) compileRoutes() error {
	err := s.AddCustomRoute("/info", false, s.handleRouteInfo())
	if err != nil {
		s.logger.Error("Failed to register custom routes", "path", "/info", "error", err)
		return fmt.Errorf("Error while registering /info route: %v\n", err)
	}

	return nil
}

// AddCustomRoute Method to add a route defined by the user to the server. DO NOT ADD IT AFTER THE SERVER HAS BEEN STARTED PLS (idk what could happen)
func (s *Server) AddCustomRoute(path string, isPrivate bool, handler http.HandlerFunc) error {
	if len(path) < 1 || path[0] != '/' {
		return errors.New("Request badly formatted. The first character should be a '/' ")
	}

	if isPrivate && (len(path) < 5 || path[:5] != "/pvt-") {
		return errors.New("Private request badly formatted. The request should start with a '/pvt' ")
	}

	if isPrivate {
		s.Handler.Handle(path, s.checkErmesSecret(handler))
	} else {
		s.Handler.Handle(path, s.checkClientSecret(handler))
	}

	return nil
}

func (s *Server) handleRouteInfo() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("Received info request for the current node", "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.RequestURI())
		fmt.Fprintf(w, "Hello! Since you have requested it I will dump all of my info now\n")
		fmt.Fprintf(w, "%s", s.ServerInfo.PrettyString())
	}
}
