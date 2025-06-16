package routes

import (
	"clientrepo"
	"info"
	"io"
	"net/http"
)

func AddRoutes(
	mux *http.ServeMux,
	logger *io.Writer,
	sinfo *info.Serverinfo,
	clientRepo *clientrepo.ClientRepo,

) {
	mux.Handle("/", handleRouteDefault(logger))
	mux.Handle("/accept", handleRouteAccept(logger))
	mux.Handle("/info", handleRouteInfo(logger, sinfo))
	mux.Handle("/pingFather", handleRoutePingFather(logger, sinfo))
	mux.Handle("/add-client", handleRouteAddClient(logger, sinfo, clientRepo))
	mux.Handle("/ask-client-exists", handleRouteAskClientExists(logger, sinfo, clientRepo))
}
