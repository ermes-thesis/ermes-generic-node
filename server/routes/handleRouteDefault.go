package routes

import (
	"fmt"
	"io"
	"net/http"
)

func handleRouteDefault(logger *io.Writer) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(*logger, "got / \n")
		},
	)
}
