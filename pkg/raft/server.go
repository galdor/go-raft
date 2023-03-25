package raft

import (
	"fmt"
	"net/http"
	"path"
	"sync"
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
	s.stopHTTPServer()

	s.persistentStore.Close()
}
