package raft

import (
	"fmt"
	"math/rand"
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

	MinElectionTimeout time.Duration
	MaxElectionTimeout time.Duration

	HeartbeatInterval time.Duration
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
	logStore        *LogStore

	randGenerator *rand.Rand

	heartbeatTicker *time.Ticker
	electionTimer   *time.Timer // follower or candidate only

	httpServer *http.Server
	httpClient *http.Client

	rpcChan chan IncomingRPCMsg

	errorChan chan<- error
	stopChan  chan struct{}
	wg        sync.WaitGroup
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

	if cfg.MinElectionTimeout == 0 {
		cfg.MinElectionTimeout = 500 * time.Millisecond
	}

	if cfg.MaxElectionTimeout == 0 {
		cfg.MaxElectionTimeout = 1000 * time.Millisecond
	}

	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}

	randSource := rand.NewSource(time.Now().UnixNano())

	dataDirectory := path.Join(cfg.DataDirectory, cfg.Id)

	persistentStorePath := path.Join(dataDirectory, "persistent-state.json")
	persistentStore := NewPersistentStore(persistentStorePath)

	logStorePath := path.Join(dataDirectory, "log.data")
	logStore := NewLogStore(logStorePath)

	s := &Server{
		Cfg: cfg,
		Log: cfg.Logger,

		Id:            cfg.Id,
		LocalAddress:  sdata.LocalAddress,
		PublicAddress: sdata.PublicAddress,

		persistentStore: persistentStore,
		logStore:        logStore,

		randGenerator: rand.New(randSource),

		rpcChan: make(chan IncomingRPCMsg),

		stopChan: make(chan struct{}),
	}

	return s, nil
}

func (s *Server) Start(errorChan chan<- error) error {
	s.Log.Debug(1, "starting")

	s.errorChan = errorChan

	// Persistent store
	s.Log.Debug(1, "loading persistent store from %q",
		s.persistentStore.filePath)

	if err := s.persistentStore.Open(); err != nil {
		return fmt.Errorf("cannot open persistent persistentStore: %w", err)
	}

	if err := s.persistentStore.Read(&s.persistentState); err != nil {
		return fmt.Errorf("cannot read persistent state: %w", err)
	}

	s.Log.Debug(1, "initial persistent state: currentTerm %d, votedFor %q",
		s.persistentState.CurrentTerm, s.persistentState.VotedFor)

	// Log store
	s.Log.Debug(1, "loading log store from %q", s.logStore.filePath)

	if err := s.logStore.Open(nil); err != nil {
		return fmt.Errorf("cannot open log store: %w", err)
	}

	// Transport
	if err := s.startHTTPServer(); err != nil {
		return fmt.Errorf("cannot start http server: %w", err)
	}
	s.Log.Info("listening on %s", s.LocalAddress)

	s.httpClient = newHTTPClient()

	// Internal state
	s.state = ServerStateFollower

	s.setupHeartbeatTicker()
	s.setupElectionTimer()

	// Main
	s.wg.Add(1)
	go s.main()

	s.Log.Debug(1, "started")

	return nil
}

func (s *Server) Stop() {
	s.Log.Debug(1, "stopping")

	close(s.stopChan)
	s.wg.Wait()

	s.Log.Debug(1, "stopped")
}

func (s *Server) main() {
	defer s.wg.Done()

	defer func() {
		if value := recover(); value != nil {
			msg := RecoverValueString(value)
			trace := StackTrace(10)
			s.Log.Error("panic: %s\n%s", msg, trace)

			s.errorChan <- fmt.Errorf("panic: %s", msg)
			s.shutdown()
		}
	}()

	for {
		select {
		case <-s.stopChan:
			s.shutdown()
			return

		case <-s.heartbeatTicker.C:
			s.onHeartbeatTicker()

		case <-s.electionTimer.C:
			s.onElectionTimer()

		case incomingMsg := <-s.rpcChan:
			s.onRPCMsg(incomingMsg.SourceId, incomingMsg.Msg)
		}
	}
}

func (s *Server) shutdown() {
	s.Log.Debug(1, "shutting down")

	s.stopHTTPServer()

	s.logStore.Close()
	s.persistentStore.Close()

	close(s.rpcChan)
}

func (s *Server) onHeartbeatTicker() {
	if s.state != ServerStateLeader {
		return
	}

	s.broadcastMsg(&RPCAppendEntriesRequest{
		Term:         s.persistentState.CurrentTerm,
		LeaderId:     s.Id,
		PrevLogIndex: 0, // TODO
		PrevLogTerm:  0, // TODO
		LeaderCommit: 0, // TODO
	})
}

func (s *Server) onElectionTimer() {
	switch s.state {
	case ServerStateFollower:
		s.startElection()

	case ServerStateCandidate:
		s.onElectionTimeout()

	default:
		Panicf("unexpected election timer activation in state %v", s.state)
	}
}

func (s *Server) onRPCMsg(sourceId ServerId, msg RPCMsg) {
	s.Log.Debug(2, "received %v from %s", msg, sourceId)

	term := msg.GetTerm()

	if term < s.persistentState.CurrentTerm {
		// If a message contains a term lower that the current one, it is
		// stale and we must ignore it.

		s.Log.Debug(1, "ignoring stale message %v (current term: %d)",
			msg, s.persistentState.CurrentTerm)
		return
	}

	if term > s.persistentState.CurrentTerm {
		// If a message contains a term higher than the current one, we are
		// out-of-date and must revert to follower.

		s.Log.Debug(1, "received message with term %d (current term: %d), "+
			"reverting to follower", term, s.persistentState.CurrentTerm)

		pstate := PersistentState{CurrentTerm: term, VotedFor: ""}
		if err := s.updatePersistentState(pstate); err != nil {
			return
		}

		s.revertToFollower()
	}

	switch msgv := msg.(type) {
	case *RPCRequestVoteRequest:
		s.onRPCRequestVoteRequest(sourceId, msgv)
	case *RPCRequestVoteResponse:
		s.onRPCRequestVoteResponse(sourceId, msgv)
	case *RPCAppendEntriesRequest:
		s.onRPCAppendEntriesRequest(sourceId, msgv)
	case *RPCAppendEntriesResponse:
		s.onRPCAppendEntriesResponse(sourceId, msgv)
	default:
		s.Log.Error("unexpected message %v from %s", msg, sourceId)
	}
}

func (s *Server) onRPCRequestVoteRequest(sourceId ServerId, req *RPCRequestVoteRequest) {
	pstate := s.persistentState

	noVoteGranted := pstate.VotedFor == ""
	sameVoteGranted := pstate.VotedFor == req.CandidateId
	logUpToDate := req.LastLogIndex >= s.logStore.LastIndex()

	res := RPCRequestVoteResponse{
		Term:        pstate.CurrentTerm,
		VoteGranted: (noVoteGranted || sameVoteGranted) && logUpToDate,
	}

	if res.VoteGranted {
		s.persistentState.VotedFor = sourceId
	}

	if err := s.updatePersistentState(pstate); err != nil {
		return
	}

	s.sendMsg(sourceId, &res)
}

func (s *Server) onRPCRequestVoteResponse(sourceId ServerId, res *RPCRequestVoteResponse) {
	// Update the vote table
	s.votes[sourceId] = res.VoteGranted

	// Count votes
	nbVotes := 0

	for _, vote := range s.votes {
		if vote {
			nbVotes++
		}
	}

	// If we do not have not the majority of votes, there is nothing more to
	// do
	nbServers := len(s.Cfg.Servers)

	if nbVotes <= nbServers/2 {
		return
	}

	// If the majority of the servers voted for us, become leader
	s.Log.Info("obtained %d/%d votes, becoming leader", nbVotes, nbServers)

	s.state = ServerStateLeader

	// Clear the election timer if it is active
	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	// Clear candidate data
	s.votes = nil

	// Send an empty AppendEntries message to all other servers
	s.broadcastMsg(&RPCAppendEntriesRequest{
		Term:         s.persistentState.CurrentTerm,
		LeaderId:     s.Id,
		PrevLogIndex: 0, // TODO
		PrevLogTerm:  0, // TODO
		LeaderCommit: 0, // TODO
	})

	// Reset the heartbeat timer
	s.resetHeartbeatTicker()
}

func (s *Server) onRPCAppendEntriesRequest(sourceId ServerId, req *RPCAppendEntriesRequest) {
	if s.state == ServerStateFollower {
		s.resetElectionTimer()
	}

	if req.LeaderId != s.currentLeader {
		// The leader has changed

		s.Log.Info("leader is %s", req.LeaderId)
		s.currentLeader = req.LeaderId

		if s.state == ServerStateCandidate {
			s.revertToFollower()
		}
	}

	// TODO
}

func (s *Server) onRPCAppendEntriesResponse(sourceId ServerId, res *RPCAppendEntriesResponse) {
	// TODO
}

func (s *Server) revertToFollower() {
	s.state = ServerStateFollower

	// Clear leader data
	s.nextIndex = nil
	s.matchIndex = nil

	// Clear candidate data
	s.votes = nil

	// Rearm the election timer; if we do not receive any AppendEntries
	// request before the timer goes off, we will become candidate and start
	// an election.
	s.setupElectionTimer()
}

func (s *Server) setupHeartbeatTicker() {
	intervalMs := s.Cfg.HeartbeatInterval.Milliseconds()
	interval := time.Duration(intervalMs) * time.Millisecond

	s.heartbeatTicker = time.NewTicker(interval)
}

func (s *Server) resetHeartbeatTicker() {
	if s.state != ServerStateLeader {
		Panicf("cannot reset heartbeat ticker in state %v", s.state)
	}

	intervalMs := s.Cfg.HeartbeatInterval.Milliseconds()
	interval := time.Duration(intervalMs) * time.Millisecond

	s.heartbeatTicker.Reset(interval)
}

func (s *Server) setupElectionTimer() {
	if s.state == ServerStateLeader {
		Panicf("cannot setup election timer in state %v", s.state)
	}

	timeout := s.electionTimeout()
	s.Log.Debug(2, "election timer will expire in %v", timeout)

	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	s.electionTimer = time.NewTimer(timeout)
}

func (s *Server) resetElectionTimer() {
	if s.state != ServerStateFollower {
		Panicf("cannot reset election timer in state %v", s.state)
	}

	timeout := s.electionTimeout()
	s.Log.Debug(2, "election timer will expire in %v", timeout)

	if !s.electionTimer.Stop() {
		<-s.electionTimer.C
	}

	s.electionTimer.Reset(timeout)
}

func (s *Server) electionTimeout() time.Duration {
	minTimeoutMs := s.Cfg.MinElectionTimeout.Milliseconds()
	maxTimeoutMs := s.Cfg.MaxElectionTimeout.Milliseconds()

	jitter := s.randGenerator.Int63n(maxTimeoutMs - minTimeoutMs + 1)
	timeoutMs := minTimeoutMs + jitter

	return time.Duration(timeoutMs) * time.Millisecond
}

func (s *Server) startElection() {
	if s.state != ServerStateFollower {
		Panicf("cannot start election in state %v", s.state)
	}

	s.Log.Debug(1, "starting election for term %d",
		s.persistentState.CurrentTerm+1)

	// Start a new term and vote for ourselves
	pstate := PersistentState{
		CurrentTerm: s.persistentState.CurrentTerm + 1,
		VotedFor:    s.Id,
	}

	if err := s.updatePersistentState(pstate); err != nil {
		// If we cannot save the persistent state, we rearm the election timer
		// to try again later.
		s.setupElectionTimer()
		return
	}

	// Send RequestVote requests to all other servers
	s.broadcastMsg(&RPCRequestVoteRequest{
		Term:         s.persistentState.CurrentTerm,
		CandidateId:  s.Id,
		LastLogIndex: s.logStore.LastIndex(),
		LastLogTerm:  s.logStore.LastTerm(),
	})

	// We are now a candidate
	s.state = ServerStateCandidate

	s.votes = make(map[ServerId]bool)

	// Rearm the election timer to detect an election timeout
	s.setupElectionTimer()
}

func (s *Server) onElectionTimeout() {
	if s.state != ServerStateCandidate {
		Panicf("election cannot timeout in state %v", s.state)
	}

	// If the current election timed out, we have to start a new election. We
	// reset the state to "follower" so that startElection is called on a
	// clean slate.

	s.Log.Debug(1, "election timeout in term %d", s.persistentState.CurrentTerm)

	s.state = ServerStateFollower

	// Immediately start a new election
	s.startElection()
}

func (s *Server) updatePersistentState(state PersistentState) error {
	if err := s.persistentStore.Write(state); err != nil {
		s.Log.Error("cannot write persistent state: %v", err)
		return err
	}

	s.persistentState = state
	return nil
}
