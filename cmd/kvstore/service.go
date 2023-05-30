package main

import (
	"fmt"
	"net"

	jsonvalidator "github.com/galdor/go-json-validator"
	"github.com/galdor/go-log"
	"github.com/galdor/go-program"
	"github.com/galdor/go-raft/pkg/raft"
	"github.com/galdor/go-service/pkg/service"
	"github.com/galdor/go-service/pkg/shttp"
)

type ServiceCfg struct {
	Service service.ServiceCfg `json:"service"`
	Raft    RaftCfg            `json:"raft"`
}

type RaftCfg struct {
	Servers       raft.ServerSet `json:"servers"`
	DataDirectory string         `json:"dataDirectory"`
}

type Service struct {
	Cfg     ServiceCfg
	Program *program.Program
	Service *service.Service
	Log     *log.Logger

	store      *Store
	raftServer *raft.Server
	apiServer  *APIServer
}

func (cfg *ServiceCfg) ValidateJSON(v *jsonvalidator.Validator) {
	v.CheckObject("service", &cfg.Service)

	v.CheckObject("raft", &cfg.Raft)
}

func (cfg *RaftCfg) ValidateJSON(v *jsonvalidator.Validator) {
	v.WithChild("servers", func() {
		for _, server := range cfg.Servers {
			v.CheckStringNotEmpty("localAddress", server.LocalAddress)
			v.CheckStringNotEmpty("publicAddress", server.PublicAddress)
		}
	})

	v.CheckStringNotEmpty("dataDirectory", cfg.DataDirectory)
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) InitProgram(p *program.Program) {
	s.Program = p

	p.AddArgument("id", "the server identifier")
}

func (s *Service) DefaultCfg() interface{} {
	return &s.Cfg
}

func (s *Service) ValidateCfg() error {
	return nil
}

func (s *Service) ServiceCfg() *service.ServiceCfg {
	cfg := &s.Cfg.Service

	instanceId := s.Program.ArgumentValue("id")

	if cfg.HTTPServers == nil {
		cfg.HTTPServers = make(map[string]*shttp.ServerCfg)
	}

	raftServerCfg := s.Cfg.Raft.Servers[instanceId]
	host, _, _ := net.SplitHostPort(raftServerCfg.LocalAddress)

	cfg.HTTPServers["api"] = &shttp.ServerCfg{
		Address:               net.JoinHostPort(host, "8081"),
		LogSuccessfulRequests: true,
		ErrorHandler:          shttp.JSONErrorHandler,
	}

	return cfg
}

func (s *Service) Init(ss *service.Service) error {
	s.Service = ss
	s.Log = ss.Log

	s.store = NewStore()

	if err := s.initRaftServer(); err != nil {
		return err
	}

	if err := s.initAPIServer(); err != nil {
		return err
	}

	return nil
}

func (s *Service) initRaftServer() error {
	instanceId := s.Service.Program.ArgumentValue("id")

	logger := s.Log.Child("raft", log.Data{
		"instance": instanceId,
	})

	serverCfg := raft.ServerCfg{
		Id:      instanceId,
		Servers: s.Cfg.Raft.Servers,

		DataDirectory: s.Cfg.Raft.DataDirectory,

		Logger: logger,

		LogReplayFunc: s.replayLogEntry,
	}

	server, err := raft.NewServer(serverCfg)
	if err != nil {
		return fmt.Errorf("cannot create raft server: %w", err)
	}

	s.raftServer = server

	return nil
}

func (s *Service) initAPIServer() error {
	api, err := NewAPIServer(s)
	if err != nil {
		return fmt.Errorf("cannot create api server: %w", err)
	}

	s.apiServer = api

	return nil
}

func (s *Service) Start(ss *service.Service) error {
	if err := s.raftServer.Start(ss.ErrorChan()); err != nil {
		return fmt.Errorf("cannot start raft server: %w", err)
	}

	if err := s.apiServer.Init(); err != nil {
		return fmt.Errorf("cannot initialize api server: %w", err)
	}

	return nil
}

func (s *Service) Stop(ss *service.Service) {
	s.raftServer.Stop()
}

func (s *Service) Terminate(ss *service.Service) {
}

func (s *Service) replayLogEntry(entry *raft.LogEntry) error {
	op, err := DecodeOp(entry.Data)
	if err != nil {
		return fmt.Errorf("cannot decode op: %w", err)
	}

	s.store.ApplyOp(op)

	return nil
}
