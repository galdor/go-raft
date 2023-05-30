package main

import (
	"github.com/galdor/go-service/pkg/shttp"
)

type APIServer struct {
	Service *Service
}

func NewAPIServer(s *Service) (*APIServer, error) {
	api := APIServer{
		Service: s,
	}

	return &api, nil
}

func (api *APIServer) Init() error {
	api.initRoutes()
	return nil
}

func (api *APIServer) initRoutes() {
	api.Route("/store", "GET", api.hStoreGET)
	api.Route("/store/:key", "GET", api.hStoreKeyGET)
	api.Route("/store/:key", "PUT", api.hStoreKeyPUT)
	api.Route("/store/:key", "DELETE", api.hStoreKeyDELETE)
}

func (api *APIServer) Route(pathPattern, method string, routeFunc shttp.RouteFunc) {
	s := api.Service.Service.HTTPServer("api")
	s.Route(pathPattern, method, routeFunc)
}

func (api *APIServer) hStoreGET(h *shttp.Handler) {
	// TODO
	h.ReplyNotImplemented("key listing")
}

func (api *APIServer) hStoreKeyGET(h *shttp.Handler) {
	// TODO
	h.ReplyNotImplemented("key read")
}

func (api *APIServer) hStoreKeyPUT(h *shttp.Handler) {
	// TODO
	h.ReplyNotImplemented("key write")
}

func (api *APIServer) hStoreKeyDELETE(h *shttp.Handler) {
	// TODO
	h.ReplyNotImplemented("key deletion")
}
