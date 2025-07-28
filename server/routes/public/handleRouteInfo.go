package public

import (
	"fmt"
	"io"
	"net/http"
	"server/info"
)

func HandleRouteInfo(logger *io.Writer, serverinfo *info.ServerInfo) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(*logger, "got /info \n")
			fmt.Fprintf(w, "Hello! Since you have requested it I will dump all of my info now\n")
			fmt.Fprintf(w, "%s", info.StringifyInfo(serverinfo))
		},
	)
}
