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
	mux.Handle("/addclient", handleRouteAddClient(logger, clientRepo))
}
