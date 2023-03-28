package main

import (
	"fmt"

	"github.com/exograd/go-program"
	"github.com/galdor/go-raft/pkg/raft"
	"github.com/galdor/go-service/pkg/log"
	"github.com/galdor/go-service/pkg/service"
	"github.com/galdor/go-service/pkg/sjson"
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
	Service *service.Service
	Log     *log.Logger

	raftServer *raft.Server
}

func (cfg *ServiceCfg) ValidateJSON(v *sjson.Validator) {
	v.CheckObject("service", &cfg.Service)

	v.CheckObject("raft", &cfg.Raft)
}

func (cfg *RaftCfg) ValidateJSON(v *sjson.Validator) {
	v.Push("servers")
	for _, server := range cfg.Servers {
		v.CheckStringNotEmpty("localAddress", server.LocalAddress)
		v.CheckStringNotEmpty("publicAddress", server.PublicAddress)
	}
	v.Pop()

	v.CheckStringNotEmpty("dataDirectory", cfg.DataDirectory)
}

func NewService() *Service {
	return &Service{}
}

func (e *Service) InitProgram(p *program.Program) {
	p.AddArgument("id", "the server identifier")
}

func (e *Service) DefaultCfg() interface{} {
	return &e.Cfg
}

func (e *Service) ValidateCfg() error {
	return nil
}

func (e *Service) ServiceCfg() *service.ServiceCfg {
	return &e.Cfg.Service
}

func (e *Service) Init(s *service.Service) error {
	e.Service = s
	e.Log = s.Log

	if err := e.initRaftServer(); err != nil {
		return err
	}

	return nil
}

func (e *Service) initRaftServer() error {
	instanceId := e.Service.Program.ArgumentValue("id")

	logger := e.Log.Child("raft", nil)

	serverCfg := raft.ServerCfg{
		Id:      instanceId,
		Servers: e.Cfg.Raft.Servers,

		DataDirectory: e.Cfg.Raft.DataDirectory,

		Logger: logger,
	}

	server, err := raft.NewServer(serverCfg)
	if err != nil {
		return fmt.Errorf("cannot create raft server: %w", err)
	}

	e.raftServer = server

	return nil
}

func (e *Service) Start(s *service.Service) error {
	if err := e.raftServer.Start(s.ErrorChan()); err != nil {
		return fmt.Errorf("cannot start raft server: %w", err)
	}

	return nil
}

func (e *Service) Stop(s *service.Service) {
	e.raftServer.Stop()
}

func (e *Service) Terminate(s *service.Service) {
}
