package server

import (
	"server/routes/private"
	"server/routes/public"
)

func (s *Server) compileRoutes() {
	s.Handler.Handle("/add-client", public.HandleRouteAddClient(&s.Logger, &s.Serverinfo, s.ClientRepo))
	s.Handler.Handle("/ask-client-exists", public.HandleRouteAskClientExists(&s.Logger, &s.Serverinfo, s.ClientRepo))
	s.Handler.Handle("/pvt-add-client", s.checkSecret(private.HandleRoutePvtAddClient(&s.Logger, &s.Serverinfo, s.ClientRepo)))
	s.Handler.Handle("/pvt-ask-client-exists", s.checkSecret(private.HandleRoutePvtAskClientExists(&s.Logger, &s.Serverinfo, s.ClientRepo)))

}
