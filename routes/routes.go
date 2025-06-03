package routes

import (
	"info"
	"io"
	"net/http"
)

func AddRoutes(
	mux *http.ServeMux,
	logger *io.Writer,
	sinfo *info.Serverinfo,

) {
	mux.Handle("/", handleRouteDefault(logger))
	mux.Handle("/accept", handleRouteAccept(logger))
	mux.Handle("/info", handleRouteInfo(logger, sinfo))
	mux.Handle("/pingFather", handleRoutePingFather(logger, sinfo))
}
