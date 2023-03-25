package raft

type ServerId string

type ServerAddress string

type ServerSet map[ServerId]ServerData

type ServerData struct {
	LocalAddress  ServerAddress `json:"localAddress"`
	PublicAddress ServerAddress `json:"publicAddress"`
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
