package public

import (
	"fmt"
	"logger"
	"net/http"
	"server/info"
)

func HandleRouteInfo(logger *logger.Logger, serverinfo *info.ServerInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Info("got /info")
		fmt.Fprintf(w, "Hello! Since you have requested it I will dump all of my info now\n")
		fmt.Fprintf(w, "%s", info.StringifyInfo(serverinfo))
	}
}
