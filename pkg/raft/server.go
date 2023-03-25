package raft

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path"
	"sync"
	"time"
)

type ServerCfg struct {
	Id      ServerId
	Servers ServerSet

	DataDirectory string

	Logger Logger
}

type Server struct {
	Cfg ServerCfg
	Log Logger

	Id            ServerId
	LocalAddress  ServerAddress
	PublicAddress ServerAddress

	state         ServerState
	currentLeader ServerId

	commitIndex LogIndex
	lastApplied LogIndex

	persistentState PersistentState

	// Leader only
	nextIndex  map[ServerId]LogIndex
	matchIndex map[ServerId]LogIndex

	// Candidate only
	votes map[ServerId]bool

	// Internal
	persistentStore *PersistentStore

	httpServer *http.Server
	httpClient *http.Client

	errChan  chan<- error
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewServer(cfg ServerCfg) (*Server, error) {
	if cfg.Id == "" {
		return nil, fmt.Errorf("missing or empty server id")
	}

	sdata, found := cfg.Servers[cfg.Id]
	if !found {
		return nil, fmt.Errorf("unknown server id %q", cfg.Id)
	}

	if cfg.DataDirectory == "" {
		return nil, fmt.Errorf("missing or empty data directory")
	}

	if cfg.Logger == nil {
		return nil, fmt.Errorf("missing logger")
	}

	persistentStorePath := path.Join(cfg.DataDirectory, "persistent-state.json")
	persistentStore := NewPersistentStore(persistentStorePath)

	s := &Server{
		Cfg: cfg,
		Log: cfg.Logger,

		Id:            cfg.Id,
		LocalAddress:  sdata.LocalAddress,
		PublicAddress: sdata.PublicAddress,

		persistentStore: persistentStore,

		stopChan: make(chan struct{}),
	}

	return s, nil
}

func (s *Server) Start(errChan chan<- error) error {
	s.Log.Info("starting")

	s.errChan = errChan

	// Persistent store
	if err := s.persistentStore.Open(); err != nil {
		return fmt.Errorf("cannot open persistent persistentStore: %w", err)
	}

	if err := s.persistentStore.Read(&s.persistentState); err != nil {
		return fmt.Errorf("cannot read persistent state: %w", err)
	}

	s.Log.Debug(1, "initial persistent state: currentTerm %d, votedFor %q",
		s.persistentState.CurrentTerm, s.persistentState.VotedFor)

	// Transport
	if err := s.startHTTPServer(); err != nil {
		return fmt.Errorf("cannot start http server: %w", err)
	}
	s.Log.Info("listening on %s", s.LocalAddress)

	s.httpClient = newHTTPClient()

	// Internal state
	s.state = ServerStateFollower

	// Main
	s.wg.Add(1)
	go s.main()

	s.Log.Info("started")

	return nil
}

func (s *Server) Stop() {
	s.Log.Info("stopping")

	close(s.stopChan)
	s.wg.Wait()

	s.stopHTTPServer()

	s.persistentStore.Close()

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

func (s *Server) startHTTPServer() error {
	listener, err := net.Listen("tcp", string(s.LocalAddress))
	if err != nil {
		return fmt.Errorf("cannot listen on %s: %w", s.LocalAddress, err)
	}

	s.Log.Info("listening on %s", s.LocalAddress)

	s.httpServer = &http.Server{
		Addr:              string(s.LocalAddress),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		defer func() {
			if value := recover(); value != nil {
				msg := RecoverValueString(value)
				trace := StackTrace(10)
				s.Log.Error("panic: %s\n%s", msg, trace)
			}
		}()

		if err := s.httpServer.Serve(listener); err != http.ErrServerClosed {
			s.errChan <- fmt.Errorf("server error: %w", err)
			return
		}
	}()

	return nil
}

func (s *Server) stopHTTPServer() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	s.httpServer.Shutdown(ctx)
}

func newHTTPClient() *http.Client {
	transport := http.Transport{
		Proxy: http.ProxyFromEnvironment,

		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 10 * time.Second,
		}).DialContext,

		MaxIdleConns: 30,

		IdleConnTimeout:       60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	client := http.Client{
		Timeout:   10 * time.Second,
		Transport: &transport,
	}

	return &client
}
