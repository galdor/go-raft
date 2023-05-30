package main

import "sync"

type Store struct {
	Entries map[string]string

	mu sync.RWMutex
}

func NewStore() *Store {
	s := Store{
		Entries: make(map[string]string),
	}

	return &s
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	value, found := s.Entries[key]
	s.mu.RUnlock()

	return value, found
}

func (s *Store) Put(key, value string) {
	s.mu.Lock()
	s.Entries[key] = value
	s.mu.Unlock()
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	delete(s.Entries, key)
	s.mu.Unlock()
}
