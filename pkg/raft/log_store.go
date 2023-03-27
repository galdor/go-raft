package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

type LogStore struct {
	filePath string
	file     *os.File

	lastIndex LogIndex
	lastTerm  Term
}

func NewLogStore(filePath string) *LogStore {
	return &LogStore{
		filePath: filePath,
	}
}

func (s *LogStore) Open(replayFn func(*LogEntry) error) error {
	flags := os.O_RDWR | os.O_CREATE
	file, err := os.OpenFile(s.filePath, flags, 0600)
	if err != nil {
		return fmt.Errorf("cannot open %q: %w", s.filePath, err)
	}

	s.file = file

	for {
		entry, err := s.readEntry(file)
		if err != nil {
			file.Close()
			return fmt.Errorf("cannot read entry: %w", err)
		} else if entry == nil {
			break
		}

		s.lastIndex++
		s.lastTerm = entry.Term

		if replayFn != nil {
			if err := replayFn(entry); err != nil {
				file.Close()
				return fmt.Errorf("cannot reply entry %d (term %d): %w",
					s.lastIndex, s.lastTerm, err)
			}
		}
	}

	return nil
}

func (s *LogStore) Close() {
	s.file.Close()
}

func (s *LogStore) LastIndex() LogIndex {
	return s.lastIndex
}

func (s *LogStore) LastTerm() Term {
	return s.lastTerm
}

func (s *LogStore) AppendEntry(entry LogEntry) error {
	if err := s.writeEntry(s.file, entry); err != nil {
		return err
	}

	s.lastIndex++
	s.lastTerm = entry.Term

	return nil
}

func (s *LogStore) readEntry(r io.Reader) (*LogEntry, error) {
	var header [16]byte

	if _, err := io.ReadAtLeast(r, header[:], 16); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		} else if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("truncated header")
		}

		return nil, err
	}

	var entry LogEntry

	u64 := binary.BigEndian.Uint64(header[0:])
	if u64 < 8 || u64 > math.MaxInt32 {
		return nil, fmt.Errorf("invalid size field %d", u64)
	}

	dataSize := int(u64) - 8

	u64 = binary.BigEndian.Uint64(header[8:])
	if u64 > math.MaxInt64 {
		return nil, fmt.Errorf("invalid term %d", u64)
	}

	entry.Term = Term(u64)

	entry.Data = make([]byte, dataSize)
	if _, err := io.ReadAtLeast(r, entry.Data, dataSize); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("truncated data")
		}

		return nil, err
	}

	return &entry, nil
}

func (s *LogStore) writeEntry(w io.Writer, entry LogEntry) error {
	var header [16]byte

	binary.BigEndian.PutUint64(header[0:], uint64(8+len(entry.Data)))
	binary.BigEndian.PutUint64(header[8:], uint64(entry.Term))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	if _, err := w.Write(entry.Data); err != nil {
		return err
	}

	return nil
}
