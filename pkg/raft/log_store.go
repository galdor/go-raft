package raft

type LogStore struct {
	entries []LogEntry
}

func NewLogStore() *LogStore {
	return &LogStore{}
}

func (s *LogStore) Open() error {
	s.entries = make([]LogEntry, 0)
	return nil
}

func (s *LogStore) Close() {
}

func (s *LogStore) LastIndex() LogIndex {
	return LogIndex(len(s.entries))
}

func (s *LogStore) LastTerm() Term {
	nbEntries := len(s.entries)

	if nbEntries == 0 {
		return 0
	}

	return s.entries[nbEntries-1].Term
}

func (s *LogStore) AppendEntry(entry LogEntry) error {
	s.entries = append(s.entries, entry)
	return nil
}
