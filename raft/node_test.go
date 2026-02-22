package raft

import (
	"context"
	"errors"
	"testing"
)

type memStore struct {
	state PersistentState
}

func newMemStore(initial PersistentState) *memStore {
	copyState := PersistentState{
		CurrentTerm: initial.CurrentTerm,
		VotedFor:    initial.VotedFor,
		CommitIndex: initial.CommitIndex,
		Log:         append([]LogEntry(nil), initial.Log...),
	}
	return &memStore{state: copyState}
}

func (m *memStore) Load() (PersistentState, error) {
	state := m.state
	state.Log = append([]LogEntry(nil), state.Log...)
	return state, nil
}

func (m *memStore) SaveState(currentTerm uint64, votedFor string) error {
	m.state.CurrentTerm = currentTerm
	m.state.VotedFor = votedFor
	return nil
}

func (m *memStore) SaveCommitIndex(commitIndex uint64) error {
	m.state.CommitIndex = commitIndex
	return nil
}

func (m *memStore) AppendEntry(entry LogEntry) error {
	m.state.Log = append(m.state.Log, entry)
	return nil
}

func (m *memStore) TruncateLog(fromIndex uint64) error {
	if fromIndex == 0 {
		m.state.Log = nil
		return nil
	}
	if fromIndex <= uint64(len(m.state.Log)) {
		m.state.Log = m.state.Log[:fromIndex-1]
	}
	return nil
}

func (m *memStore) Close() error { return nil }

type noopRPC struct{}

func (noopRPC) RequestVote(context.Context, string, RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, errors.New("not implemented")
}

func (noopRPC) AppendEntries(context.Context, string, AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{}, errors.New("not implemented")
}

func TestRequestVoteHigherTermGrantsVote(t *testing.T) {
	store := newMemStore(PersistentState{CurrentTerm: 1})
	node, err := NewNode(Config{
		ID:        "n1",
		Address:   "http://127.0.0.1:9001",
		Cluster:   map[string]string{"n1": "http://127.0.0.1:9001", "n2": "http://127.0.0.1:9002"},
		Storage:   store,
		RPCClient: noopRPC{},
	})
	if err != nil {
		t.Fatalf("NewNode error: %v", err)
	}

	resp := node.HandleRequestVote(RequestVoteRequest{
		Term:         2,
		CandidateID:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !resp.VoteGranted {
		t.Fatalf("expected vote granted")
	}
	if resp.Term != 2 {
		t.Fatalf("expected response term 2, got %d", resp.Term)
	}
	status := node.Status()
	if status.CurrentTerm != 2 {
		t.Fatalf("expected node term 2, got %d", status.CurrentTerm)
	}
	if store.state.VotedFor != "n2" {
		t.Fatalf("expected persisted vote for n2, got %q", store.state.VotedFor)
	}
}

func TestAppendEntriesTruncatesConflictingSuffix(t *testing.T) {
	store := newMemStore(PersistentState{
		CurrentTerm: 2,
		Log: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
			{Index: 3, Term: 2, Command: []byte("c")},
		},
	})
	node, err := NewNode(Config{
		ID:        "n1",
		Address:   "http://127.0.0.1:9001",
		Cluster:   map[string]string{"n1": "http://127.0.0.1:9001", "n2": "http://127.0.0.1:9002"},
		Storage:   store,
		RPCClient: noopRPC{},
	})
	if err != nil {
		t.Fatalf("NewNode error: %v", err)
	}

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         3,
		LeaderID:     "n2",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Index: 3, Term: 3, Command: []byte("x")},
			{Index: 4, Term: 3, Command: []byte("y")},
		},
		LeaderCommit: 4,
	})
	if !resp.Success {
		t.Fatalf("append should succeed: %#v", resp)
	}
	if len(node.log) != 4 {
		t.Fatalf("expected 4 log entries, got %d", len(node.log))
	}
	terms := []uint64{node.log[0].Term, node.log[1].Term, node.log[2].Term, node.log[3].Term}
	expected := []uint64{1, 1, 3, 3}
	for i := range expected {
		if terms[i] != expected[i] {
			t.Fatalf("unexpected term at index %d: got %d want %d", i+1, terms[i], expected[i])
		}
	}
	if node.commitIndex != 4 {
		t.Fatalf("expected commit index 4, got %d", node.commitIndex)
	}
}

func TestAppendEntriesHigherTermStepsDownLeader(t *testing.T) {
	store := newMemStore(PersistentState{CurrentTerm: 2})
	node, err := NewNode(Config{
		ID:        "n1",
		Address:   "http://127.0.0.1:9001",
		Cluster:   map[string]string{"n1": "http://127.0.0.1:9001", "n2": "http://127.0.0.1:9002"},
		Storage:   store,
		RPCClient: noopRPC{},
	})
	if err != nil {
		t.Fatalf("NewNode error: %v", err)
	}

	node.mu.Lock()
	node.role = RoleLeader
	node.currentTerm = 2
	node.mu.Unlock()

	resp := node.HandleAppendEntries(AppendEntriesRequest{
		Term:         3,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	})
	if !resp.Success {
		t.Fatalf("expected success when stepping down")
	}
	status := node.Status()
	if status.Role != RoleFollower {
		t.Fatalf("expected follower role, got %s", status.Role)
	}
	if status.CurrentTerm != 3 {
		t.Fatalf("expected term 3, got %d", status.CurrentTerm)
	}
	if status.LeaderID != "n2" {
		t.Fatalf("expected leader n2, got %q", status.LeaderID)
	}
}
