package server

import (
	"errors"
	"net/http"
	"server/routes/private"
	"server/routes/public"
)

// Method to add pre-defined routes to the server.
func (s *Server) compileRoutes() {
	s.Handler.Handle("/add-client", public.HandleRouteAddClient(&s.Logger, s.Serverinfo, s.ClientRepo))
	s.Handler.Handle("/ask-client-exists", public.HandleRouteAskClientExists(&s.Logger, s.Serverinfo, s.ClientRepo))
	s.Handler.Handle("/pvt-add-client", s.checkErmesSecret(private.HandleRoutePvtAddClient(&s.Logger, s.Serverinfo, s.ClientRepo)))
	s.Handler.Handle("/pvt-ask-client-exists", s.checkErmesSecret(private.HandleRoutePvtAskClientExists(&s.Logger, s.Serverinfo, s.ClientRepo)))
	s.Handler.Handle("/pvt-data-transfer-request", s.checkErmesSecret(private.HandleRoutePvtDataTransferRequest(&s.Logger, s.Serverinfo)))
	s.Handler.Handle("/lookup", s.checkErmesSecret(private.HandleRouteLookup(&s.Logger, s.TokenDecoder, s.MetadataManager)))
	s.Handler.Handle("/create_metadata/{collection_id}", s.checkErmesSecret(private.HandleRouteCreateMetadata(&s.Logger, s.MetadataManager, s.Serverinfo)))
	s.Handler.Handle("/info", public.HandleRouteInfo(&s.Logger, s.Serverinfo))

	s.Handler.Handle("/invoke-function/{name}/{version}/", s.checkMiddlewareSecret(public.HandleFunctionInvocation(&s.Logger, s.TokenDecoder, s.MetadataManager, s.Scheduler)))
	s.Handler.Handle("/invoke-function/{name}", s.checkMiddlewareSecret(public.HandleFunctionInvocation(&s.Logger, s.TokenDecoder, s.MetadataManager, s.Scheduler)))

	s.Handler.Handle("/offload-function/{name}/{version}/", s.checkErmesSecret(private.HandleOffloadFunctionInvocation(&s.Logger, s.TokenDecoder, s.Scheduler)))
	s.Handler.Handle("/offload-function/{name}", s.checkErmesSecret(private.HandleOffloadFunctionInvocation(&s.Logger, s.TokenDecoder, s.Scheduler)))

	// Let's test the method AddCustomRoute with the /info handler but calling it /info2

	s.AddCustomRoute("/info2", false, public.HandleRouteInfo(&s.Logger, s.Serverinfo))
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
		s.Handler.Handle(path, handler)
	}

	return nil
}
