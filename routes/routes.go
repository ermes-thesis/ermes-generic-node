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
	mux.Handle("/accept", handleRouteAccept(logger))
	mux.Handle("/info", handleRouteInfo(logger, sinfo))
}
