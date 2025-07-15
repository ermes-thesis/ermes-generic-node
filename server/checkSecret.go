package server

import (
	"fmt"
	"net/http"
	"os"
)

func (s *Server) checkSecret(h http.HandlerFunc) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		// If the header does not contain the secret then return 404 not found
		if r.Header.Get("X-ERMES-SECRET") != s.Serverinfo.Secret {
			http.NotFound(w, r)
			fmt.Fprintf(os.Stderr, "Secret not found in the header. Returning 404 Not Found.\n")
			return
		}

		h(w, r)
	}

}
