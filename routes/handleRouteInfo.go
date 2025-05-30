package routes

import (
	"fmt"
	"info"
	"io"
	"net/http"
)

func handleRouteInfo(logger *io.Writer, serverinfo *info.Serverinfo) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(*logger, "got /info \n")
			fmt.Fprintf(w, "Hello! Since you have requested it I will dump all of my info now\n")
			fmt.Fprintf(w, "%s", info.StringifyInfo(serverinfo))
		},
	)
}
