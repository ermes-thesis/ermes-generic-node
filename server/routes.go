package server

import (
	"errors"
	"fmt"

	routing "github.com/qiangxue/fasthttp-routing"
)

// Method to add pre-defined routes to the server.
func (s *Server) compileRoutes() error {
	err := s.AddCustomRoute("/info", false, "GET", s.handleRouteInfo)
	if err != nil {
		s.logger.Error("Failed to register custom routes", "path", "/info", "error", err)
		return fmt.Errorf("Error while registering /info route: %v\n", err)
	}

	return nil
}

// AddCustomRoute Method to add a route defined by the user to the server. DO NOT ADD IT AFTER THE SERVER HAS BEEN STARTED PLS (idk what could happen)
func (s *Server) AddCustomRoute(path string, isPrivate bool, methods string, handler routing.Handler) error {
	if len(path) < 1 || path[0] != '/' {
		return errors.New("Request badly formatted. The first character should be a '/' ")
	}

	if isPrivate && (len(path) < 5 || path[:5] != "/pvt-") {
		return errors.New("Private request badly formatted. The request should start with a '/pvt' ")
	}

	if isPrivate {
		s.Handler.To(methods, path, s.checkErmesSecret(handler))
	} else {
		s.Handler.To(methods, path, s.checkClientSecret(handler))
	}

	return nil
}

func (s *Server) handleRouteInfo(c *routing.Context) error {

	// In theory I should not close the body

	s.logger.Info("Received info request for the current node", "remote_addr", c.RemoteAddr(), "method", c.Method(), "path", c.RequestURI())

	fmt.Fprintf(c, "Hello! Since you have requested it I will dump all of my info now\n")
	fmt.Fprintf(c, "%s", s.ServerInfo.PrettyString())

	return nil
}
