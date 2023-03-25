package raft

import (
	"encoding/json"
	"fmt"
)

type RPCMsg interface {
	GetType() string
	GetTerm() Term

	fmt.Stringer
}

type IncomingRPCMsg struct {
	SourceId ServerId
	Msg      RPCMsg
}

type RPCRequestVoteRequest struct {
	Term         Term
	CandidateId  ServerId
	LastLogIndex LogIndex
	LastLogTerm  Term
}

func (msg *RPCRequestVoteRequest) GetType() string {
	return "requestVoteRequest"
}

func (msg *RPCRequestVoteRequest) GetTerm() Term {
	return msg.Term
}

func (msg *RPCRequestVoteRequest) String() string {
	return fmt.Sprintf("RequestVoteRequest{term: %d, candidateId: %q, "+
		"lastLogIndex: %d, lastLogTerm: %d}",
		msg.Term, msg.CandidateId, msg.LastLogIndex, msg.LastLogTerm)
}

type RPCRequestVoteResponse struct {
	Term        Term
	VoteGranted bool
}

func (msg *RPCRequestVoteResponse) GetType() string {
	return "requestVoteResponse"
}

func (msg *RPCRequestVoteResponse) GetTerm() Term {
	return msg.Term
}

func (msg *RPCRequestVoteResponse) String() string {
	return fmt.Sprintf("RequestVoteResponse{term: %d, voteGranted: %v}",
		msg.Term, msg.VoteGranted)
}

type RPCAppendEntriesRequest struct {
	Term         Term
	LeaderId     ServerId
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex
}

func (msg *RPCAppendEntriesRequest) GetType() string {
	return "appendEntriesRequest"
}

func (msg *RPCAppendEntriesRequest) GetTerm() Term {
	return msg.Term
}

func (msg *RPCAppendEntriesRequest) String() string {
	return fmt.Sprintf("AppendEntriesRequest{term: %d, leaderId: %q, "+
		"prevLogIndex: %d, prevLogTerm: %d, %d entries, leaderCommit: %d}",
		msg.Term, msg.LeaderId, msg.PrevLogIndex, msg.PrevLogTerm,
		len(msg.Entries), msg.LeaderCommit)
}

type RPCAppendEntriesResponse struct {
	Term    Term
	Success bool
}

func (msg *RPCAppendEntriesResponse) GetType() string {
	return "appendEntriesResponse"
}

func (msg *RPCAppendEntriesResponse) GetTerm() Term {
	return msg.Term
}

func (msg *RPCAppendEntriesResponse) String() string {
	return fmt.Sprintf("AppendEntriesResponse{term: %d, success: %v}",
		msg.Term, msg.Success)
}

func EncodeRPCMsg(msg RPCMsg) ([]byte, error) {
	value := struct {
		Type  string `json:"type"`
		Value RPCMsg `json:"value"`
	}{
		Type:  msg.GetType(),
		Value: msg,
	}

	return json.Marshal(value)
}

func DecodeRPCMsg(data []byte) (RPCMsg, error) {
	var value struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}

	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}

	var msg RPCMsg

	switch value.Type {
	case "requestVoteRequest":
		msg = &RPCRequestVoteRequest{}

	case "requestVoteResponse":
		msg = &RPCRequestVoteResponse{}

	case "appendEntriesRequest":
		msg = &RPCAppendEntriesRequest{}

	case "appendEntriesResponse":
		msg = &RPCAppendEntriesResponse{}

	default:
		return nil, fmt.Errorf("unknown message type %q", value.Type)
	}

	if err := json.Unmarshal(value.Value, &msg); err != nil {
		return nil, err
	}

	return msg, nil
}
