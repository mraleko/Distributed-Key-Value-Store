package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"dkv/raft"
)

const (
	recTypeState    = "state"
	recTypeEntry    = "entry"
	recTypeTruncate = "truncate"
	recTypeCommit   = "commit"
)

type walRecord struct {
	Type        string         `json:"type"`
	CurrentTerm uint64         `json:"currentTerm,omitempty"`
	VotedFor    string         `json:"votedFor,omitempty"`
	CommitIndex uint64         `json:"commitIndex,omitempty"`
	FromIndex   uint64         `json:"fromIndex,omitempty"`
	Entry       *raft.LogEntry `json:"entry,omitempty"`
}

// WAL implements raft.StableStore with append-only JSON records.
type WAL struct {
	mu   sync.Mutex
	path string
	file *os.File
}

// OpenWAL creates/opens a WAL at dataDir/wal.log.
func OpenWAL(dataDir string) (*WAL, error) {
	if dataDir == "" {
		return nil, errors.New("storage: empty data-dir")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: mkdir data-dir: %w", err)
	}
	path := filepath.Join(dataDir, "wal.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open wal: %w", err)
	}
	return &WAL{path: path, file: f}, nil
}

// Load replays WAL records into a persistent state snapshot.
func (w *WAL) Load() (raft.PersistentState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return raft.PersistentState{}, fmt.Errorf("storage: seek wal: %w", err)
	}

	state := raft.PersistentState{Log: make([]raft.LogEntry, 0)}
	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return raft.PersistentState{}, fmt.Errorf("storage: decode wal record: %w", err)
		}
		switch rec.Type {
		case recTypeState:
			state.CurrentTerm = rec.CurrentTerm
			state.VotedFor = rec.VotedFor
		case recTypeEntry:
			if rec.Entry == nil {
				return raft.PersistentState{}, errors.New("storage: entry record missing entry")
			}
			entry := *rec.Entry
			entry.Command = append([]byte(nil), entry.Command...)
			state.Log = append(state.Log, entry)
		case recTypeTruncate:
			if rec.FromIndex == 0 {
				state.Log = state.Log[:0]
				break
			}
			if rec.FromIndex <= uint64(len(state.Log)) {
				state.Log = state.Log[:rec.FromIndex-1]
			}
		case recTypeCommit:
			state.CommitIndex = rec.CommitIndex
		default:
			return raft.PersistentState{}, fmt.Errorf("storage: unknown wal record type %q", rec.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		return raft.PersistentState{}, fmt.Errorf("storage: scan wal: %w", err)
	}
	if state.CommitIndex > uint64(len(state.Log)) {
		state.CommitIndex = uint64(len(state.Log))
	}
	if _, err := w.file.Seek(0, 2); err != nil {
		return raft.PersistentState{}, fmt.Errorf("storage: seek wal end: %w", err)
	}
	return state, nil
}

// SaveState persists currentTerm and votedFor.
func (w *WAL) SaveState(currentTerm uint64, votedFor string) error {
	return w.appendRecord(walRecord{
		Type:        recTypeState,
		CurrentTerm: currentTerm,
		VotedFor:    votedFor,
	})
}

// SaveCommitIndex persists commit progress.
func (w *WAL) SaveCommitIndex(commitIndex uint64) error {
	return w.appendRecord(walRecord{
		Type:        recTypeCommit,
		CommitIndex: commitIndex,
	})
}

// AppendEntry appends a new log entry record.
func (w *WAL) AppendEntry(entry raft.LogEntry) error {
	entry.Command = append([]byte(nil), entry.Command...)
	return w.appendRecord(walRecord{
		Type:  recTypeEntry,
		Entry: &entry,
	})
}

// TruncateLog truncates log entries from fromIndex onward.
func (w *WAL) TruncateLog(fromIndex uint64) error {
	return w.appendRecord(walRecord{
		Type:      recTypeTruncate,
		FromIndex: fromIndex,
	})
}

func (w *WAL) appendRecord(rec walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	encoded, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("storage: marshal wal record: %w", err)
	}
	if _, err := w.file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("storage: write wal record: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("storage: sync wal record: %w", err)
	}
	return nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
