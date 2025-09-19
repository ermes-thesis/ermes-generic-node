package server

import (
	"fmt"
	"net/http"
	"os"
)

func (s *Server) checkErmesSecret(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// If the header does not contain the secret then return 403 Forbidden
		if r.Header.Get("X-ERMES-SECRET") != s.Serverinfo.Secret {
			http.Error(w, "Secret does not match", http.StatusForbidden)
			fmt.Fprintf(os.Stderr, "Secret not found in the header. Returning 403 Forbidden.\n")
			return
		}
		h(w, r)
	}
}

func (s *Server) checkMiddlewareSecret(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// If the header does not contain the secret then return 403 Forbidden
		if r.Header.Get("X-MIDDLEWARE-SECRET") != s.Serverinfo.MiddlewareSecret {
			http.Error(w, "Secret does not match", http.StatusForbidden)
			fmt.Fprintf(os.Stderr, "Secret not found in the header. Returning 403 Forbidden.\n")
			return
		}
		h(w, r)
	}
}
