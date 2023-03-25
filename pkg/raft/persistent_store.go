package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type PersistentStore struct {
	filePath string
	file     *os.File
}

func NewPersistentStore(filePath string) *PersistentStore {
	return &PersistentStore{
		filePath: filePath,
	}
}

func (s *PersistentStore) Open() error {
	flags := os.O_RDWR | os.O_CREATE
	file, err := os.OpenFile(s.filePath, flags, 0600)
	if err != nil {
		return fmt.Errorf("cannot open %q: %w", s.filePath, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()

		return fmt.Errorf("cannot stat %q: %w", s.filePath, err)
	}

	s.file = file

	if info.Size() == 0 {
		if err := s.Write(PersistentState{}); err != nil {
			file.Close()

			return fmt.Errorf("cannot write default state to %q: %w",
				s.filePath, err)
		}
	}

	return nil
}

func (s *PersistentStore) Close() {
	s.file.Close()
}

func (s *PersistentStore) Read(state *PersistentState) error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek %q: %w", s.filePath, err)
	}

	d := json.NewDecoder(s.file)
	if err := d.Decode(state); err != nil {
		return fmt.Errorf("cannot read json data from %q: %w",
			s.filePath, err)
	}

	return nil
}

func (s *PersistentStore) Write(state PersistentState) error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek %q: %w", s.filePath, err)
	}

	if err := s.file.Truncate(0); err != nil {
		return fmt.Errorf("cannot truncate %q: %w", s.filePath, err)
	}

	e := json.NewEncoder(s.file)
	if err := e.Encode(&state); err != nil {
		return fmt.Errorf("cannot write json data to %q: %w", s.filePath, err)
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("cannot sync %q: %w", s.filePath, err)
	}

	return nil
}
