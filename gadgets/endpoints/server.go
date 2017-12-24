package endpoints

import (
	"fmt"
	"net/http"

	"github.com/samwooo/bolsa/gadgets/config"
	"github.com/samwooo/bolsa/gadgets/endpoints/restful"
	"github.com/samwooo/bolsa/gadgets/logging"
)

type Server struct {
	port string
	*restful.API
}

func (s *Server) RegisterHandler(h http.Handler, paths ...string) {
	for _, path := range paths {
		http.Handle(path, h)
	}
}

func (s *Server) RegisterHandleFn(fn http.HandlerFunc, paths ...string) {
	for _, path := range paths {
		http.HandleFunc(path, fn)
	}
}

func (s *Server) Start() error {
	s.Logger.Infof("listening on port: %s", s.port)
	if err := http.ListenAndServe(s.port, nil); err != nil {
		s.Logger.Err(err)
		return err
	}
	return nil
}

func NewServer(webInfo *config.Web) *Server {
	logger := logging.GetLogger(" < endpoints > ")
	return &Server{
		port: fmt.Sprintf(":%d", webInfo.Port),
		API:  &restful.API{Logger: logger},
	}
}
