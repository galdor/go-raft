package raft

import (
	"sync"
)

type ServerCfg struct {
	Id      ServerId
	Servers ServerSet

	Logger Logger
}

type Server struct {
	Cfg ServerCfg
	Log Logger

	Id            ServerId
	LocalAddress  ServerAddress
	PublicAddress ServerAddress

	errChan  chan<- error
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewServer(cfg ServerCfg) *Server {
	sdata := cfg.Servers[cfg.Id]

	s := &Server{
		Cfg: cfg,
		Log: cfg.Logger,

		Id:            cfg.Id,
		LocalAddress:  sdata.LocalAddress,
		PublicAddress: sdata.PublicAddress,

		stopChan: make(chan struct{}),
	}

	return s
}

func (s *Server) Start(errChan chan<- error) error {
	s.Log.Info("starting")

	s.errChan = errChan

	s.wg.Add(1)
	go s.main()

	s.Log.Info("started")

	return nil
}

func (s *Server) Stop() {
	s.Log.Info("stopping")

	close(s.stopChan)
	s.wg.Wait()

	s.Log.Info("stopped")
}

func (s *Server) main() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stopChan:
			s.shutdown()
			return
		}
	}
}

func (s *Server) shutdown() {
}
