package main

import (
	"fmt"
	"sync"
)

type Store struct {
	Entries map[string]string

	mu sync.Mutex
}

func NewStore() *Store {
	s := Store{
		Entries: make(map[string]string),
	}

	return &s
}

func (s *Store) ApplyOp(vop Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch op := vop.(type) {
	case *OpPut:
		s.putEntry(op.Key, op.Value)

	case *OpDelete:
		s.deleteEntry(op.Key)

	default:
		return fmt.Errorf("unhandled op %#v", vop)
	}

	return nil
}

func (s *Store) getEntry(key string) (string, bool) {
	value, found := s.Entries[key]
	return value, found
}

func (s *Store) putEntry(key, value string) {
	s.Entries[key] = value
}

func (s *Store) deleteEntry(key string) {
	delete(s.Entries, key)
}
