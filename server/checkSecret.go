package server

import (
	"errors"
	"net/http"
	"server/helper"
	"time"

	routing "github.com/qiangxue/fasthttp-routing"
)

func (s *Server) checkErmesSecret(h routing.Handler) routing.Handler {
	return func(c *routing.Context) error {
		// If the header does not contain the secret then return 403 Forbidden

		receivedServerHeader := helper.UnsafeString(c.Request.Header.Peek("X-ERMES-SECRET"))

		if receivedServerHeader != s.ServerInfo.Secret {
			s.logger.Warn("Unauthorized request with invalid Ermes secret",
				"remote_addr", c.RemoteAddr(),
				"method", c.Method(),
				"path", c.RequestURI(),
			)
			s.logger.DebugfWith("Forbidden request: mismatched X-ERMES-SECRET '%s'",
				[]any{receivedServerHeader},
				"true_ermes_secret", s.ServerInfo.Secret)
			c.Error("Forbidden: invalid Ermes secret", http.StatusUnauthorized)
			return errors.New("forbidden: invalid Ermes secret")
		}
		startTime := time.Now()
		h(c)
		s.logger.Eval("Ermes request processed", "request_url", c.RequestURI(), "duration", time.Since(startTime))
		return nil
	}
}

func (s *Server) checkClientSecret(h routing.Handler) routing.Handler {
	return func(c *routing.Context) error {
		// If the header does not contain the secret then return 403 Forbidden

		receivedClientHeader := helper.UnsafeString(c.Request.Header.Peek("X-CLIENT-SECRET"))

		if receivedClientHeader != s.ServerInfo.ClientSecret {
			s.logger.Warn("Unauthorized request with invalid Ermes Client library secret",
				"remote_addr", c.RemoteAddr(),
				"method", c.Method(),
				"path", c.RequestURI(),
			)
			s.logger.DebugfWith("Forbidden request: mismatched X-CLIENT-SECRET '%s'",
				[]any{receivedClientHeader},
				"true_ermes_secret", s.ServerInfo.ClientSecret)
			c.Error("Forbidden: invalid Ermes client library secret", http.StatusUnauthorized)
			return errors.New("forbidden: invalid Ermes client library secret")
		}
		startTime := time.Now()
		h(c)
		s.logger.Eval("Client request processed", "request_url", c.RequestURI(), "duration", time.Since(startTime))
		return nil
	}
}
