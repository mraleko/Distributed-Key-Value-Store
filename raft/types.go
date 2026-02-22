package raft

import "context"

// Role is the server role in the Raft protocol.
type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

// LogEntry is a replicated command entry.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command"`
}

// ApplyMsg is delivered in-order for committed entries.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// RequestVoteRequest is the Raft vote RPC request.
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}

// RequestVoteResponse is the Raft vote RPC response.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

// AppendEntriesRequest is the Raft append RPC request.
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex uint64     `json:"prevLogIndex"`
	PrevLogTerm  uint64     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leaderCommit"`
}

// AppendEntriesResponse is the Raft append RPC response.
type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflictIndex,omitempty"`
	ConflictTerm  uint64 `json:"conflictTerm,omitempty"`
	MatchIndex    uint64 `json:"matchIndex,omitempty"`
}

// InstallSnapshotRequest is optional snapshot replication RPC input.
type InstallSnapshotRequest struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leaderId"`
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`
	Data              []byte `json:"data"`
}

// InstallSnapshotResponse is optional snapshot replication RPC output.
type InstallSnapshotResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// Status is exposed for operational inspection.
type Status struct {
	ID          string `json:"id"`
	Role        Role   `json:"role"`
	CurrentTerm uint64 `json:"currentTerm"`
	LeaderID    string `json:"leaderId"`
	CommitIndex uint64 `json:"commitIndex"`
	LastApplied uint64 `json:"lastApplied"`
}

// RPCClient sends Raft RPCs to peers.
type RPCClient interface {
	RequestVote(ctx context.Context, target string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, target string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}

// PersistentState represents replayed WAL state.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string
	CommitIndex uint64
	Log         []LogEntry
}

// StableStore provides Raft durable persistence.
type StableStore interface {
	Load() (PersistentState, error)
	SaveState(currentTerm uint64, votedFor string) error
	SaveCommitIndex(commitIndex uint64) error
	AppendEntry(entry LogEntry) error
	TruncateLog(fromIndex uint64) error
	Close() error
}

// Config controls node behavior.
type Config struct {
	ID                  string
	Address             string
	Cluster             map[string]string
	Storage             StableStore
	RPCClient           RPCClient
	ElectionTimeoutMin  int // milliseconds
	ElectionTimeoutMax  int // milliseconds
	HeartbeatIntervalMS int // milliseconds
}

// NotLeaderError indicates the target node is not leader for writes.
type NotLeaderError struct {
	LeaderID   string
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderAddr != "" {
		return "not leader; redirect to " + e.LeaderAddr
	}
	return "not leader"
}
