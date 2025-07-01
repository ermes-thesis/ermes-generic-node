package server

import (
	"net/http"
)

func (s *Server) checkSecret(h http.HandlerFunc) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		// If the header does not contain the secret then return 404 not found
		if r.Header.Get("X-ERMES-SECRET") != s.Serverinfo.Secret {
			http.NotFound(w, r)
			return
		}

		h(w, r)
	}

}
