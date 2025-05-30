package routes

import (
	"fmt"
	"io"
	"net/http"
)

func handleRouteAccept(logger *io.Writer) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(*logger, "got /accept \n")
			io.WriteString(w, "You have connected succesfully! \n")
		},
	)
}
