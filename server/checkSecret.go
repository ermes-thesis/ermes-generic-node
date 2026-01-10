package server

import (
	"net/http"
	"time"
)

func (s *Server) checkErmesSecret(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// If the header does not contain the secret then return 403 Forbidden
		if r.Header.Get("X-ERMES-SECRET") != s.ServerInfo.Secret {
			s.logger.Warn("Unauthorized request with invalid Ermes secret",
				"remote_addr", r.RemoteAddr,
				"method", r.Method,
				"path", r.URL.RequestURI(),
			)
			s.logger.DebugfWith("Forbidden request: mismatched X-ERMES-SECRET '%s'",
				[]any{r.Header.Get("X-ERMES-SECRET")},
				"true_ermes_secret", s.ServerInfo.Secret)
			http.Error(w, "Forbidden: invalid Ermes secret", http.StatusUnauthorized)
			return
		}
		startTime := time.Now()
		h(w, r)
		s.logger.Eval("Ermes request processed", "request_url", r.URL.RequestURI(), "duration", time.Since(startTime))
	}
}

func (s *Server) checkClientSecret(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// If the header does not contain the secret then return 403 Forbidden
		if r.Header.Get("X-CLIENT-SECRET") != s.ServerInfo.ClientSecret {
			s.logger.Warn("Unauthorized request with invalid Ermes Client library secret",
				"remote_addr", r.RemoteAddr,
				"method", r.Method,
				"path", r.URL.RequestURI(),
			)
			s.logger.DebugfWith("Forbidden request: mismatched X-CLIENT-SECRET '%s'",
				[]any{r.Header.Get("X-CLIENT-SECRET")},
				"true_ermes_secret", s.ServerInfo.ClientSecret)
			http.Error(w, "Forbidden: invalid Ermes client library secret", http.StatusUnauthorized)
			return
		}
		startTime := time.Now()
		h(w, r)
		s.logger.Eval("Client request processed", "request_url", r.URL.RequestURI(), "duration", time.Since(startTime))
	}
}
