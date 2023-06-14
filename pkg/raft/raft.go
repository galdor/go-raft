package raft

type ServerId = string

type ServerAddress = string

type ServerSet map[ServerId]ServerData

type ServerData struct {
	LocalAddress  ServerAddress `json:"local_address"`
	PublicAddress ServerAddress `json:"public_address"`
}

type ServerState string

const (
	ServerStateFollower  ServerState = "follower"
	ServerStateCandidate ServerState = "candidate"
	ServerStateLeader    ServerState = "leader"
)

type Term int64

type LogIndex int64

type LogEntry struct {
	Term Term
	Data []byte
}

type PersistentState struct {
	CurrentTerm Term
	VotedFor    ServerId
}
