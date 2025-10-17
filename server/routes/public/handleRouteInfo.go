package public

import (
	"fmt"
	"net/http"
	"server/info"

	"github.com/ermes-thesis/ermes-common/logger"
)

func HandleRouteInfo(logger *logger.Logger, serverinfo *info.ServerInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Info("got /info")
		fmt.Fprintf(w, "Hello! Since you have requested it I will dump all of my info now\n")
		fmt.Fprintf(w, "%s", info.StringifyInfo(serverinfo))
	}
}
