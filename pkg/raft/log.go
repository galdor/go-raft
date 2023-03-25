package raft

type Logger interface {
	Debug(int, string, ...interface{})
	Info(string, ...interface{})
	Error(string, ...interface{})
}
