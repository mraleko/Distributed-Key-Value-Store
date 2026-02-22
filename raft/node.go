package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	defaultElectionMinMS = 350
	defaultElectionMaxMS = 700
	defaultHeartbeatMS   = 120
)

// Node is a single Raft server instance.
type Node struct {
	mu sync.Mutex

	id      string
	address string
	cluster map[string]string
	peerIDs []string

	rpc     RPCClient
	storage StableStore
	logger  *log.Logger

	role        Role
	currentTerm uint64
	votedFor    string
	leaderID    string

	log         []LogEntry
	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	waiters map[uint64][]chan error

	applyCh       chan ApplyMsg
	replicateNow  chan struct{}
	stopCh        chan struct{}
	wg            sync.WaitGroup
	started       bool
	electionRun   bool
	electionUntil time.Time
	electionDur   time.Duration

	heartbeatInterval time.Duration
	electionMin       int
	electionMax       int

	rnd *rand.Rand
}

// NewNode constructs a Raft node and loads persistent state.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: missing node id")
	}
	if cfg.Storage == nil {
		return nil, errors.New("raft: missing stable storage")
	}
	if cfg.RPCClient == nil {
		return nil, errors.New("raft: missing rpc client")
	}
	if cfg.Cluster == nil {
		return nil, errors.New("raft: missing cluster map")
	}

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	eMin := cfg.ElectionTimeoutMin
	if eMin <= 0 {
		eMin = defaultElectionMinMS
	}
	eMax := cfg.ElectionTimeoutMax
	if eMax <= 0 {
		eMax = defaultElectionMaxMS
	}
	if eMax < eMin {
		eMax = eMin
	}
	hb := cfg.HeartbeatIntervalMS
	if hb <= 0 {
		hb = defaultHeartbeatMS
	}

	loaded, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}
	if err := validateLog(loaded.Log); err != nil {
		return nil, fmt.Errorf("raft: invalid persisted log: %w", err)
	}

	clusterCopy := make(map[string]string, len(cfg.Cluster)+1)
	for id, addr := range cfg.Cluster {
		clusterCopy[id] = addr
	}
	if cfg.Address != "" {
		clusterCopy[cfg.ID] = cfg.Address
	}

	peerIDs := make([]string, 0, len(clusterCopy))
	for id := range clusterCopy {
		if id != cfg.ID {
			peerIDs = append(peerIDs, id)
		}
	}
	sort.Strings(peerIDs)

	n := &Node{
		id:                cfg.ID,
		address:           cfg.Address,
		cluster:           clusterCopy,
		peerIDs:           peerIDs,
		rpc:               cfg.RPCClient,
		storage:           cfg.Storage,
		logger:            logger,
		role:              RoleFollower,
		currentTerm:       loaded.CurrentTerm,
		votedFor:          loaded.VotedFor,
		log:               append([]LogEntry(nil), loaded.Log...),
		commitIndex:       loaded.CommitIndex,
		lastApplied:       0,
		nextIndex:         make(map[string]uint64),
		matchIndex:        make(map[string]uint64),
		waiters:           make(map[uint64][]chan error),
		applyCh:           make(chan ApplyMsg, 8192),
		replicateNow:      make(chan struct{}, 1),
		stopCh:            make(chan struct{}),
		heartbeatInterval: time.Duration(hb) * time.Millisecond,
		electionMin:       eMin,
		electionMax:       eMax,
		rnd:               rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	if n.commitIndex > n.lastLogIndexLocked() {
		n.commitIndex = n.lastLogIndexLocked()
	}
	n.resetElectionDeadlineLocked()
	n.logf("node initialized")
	return n, nil
}

// Start begins Raft background loops.
func (n *Node) Start() error {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return nil
	}
	n.started = true
	n.mu.Unlock()

	n.wg.Add(3)
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.replayCommittedLoop()
	return nil
}

// Stop gracefully terminates loops and closes storage.
func (n *Node) Stop() error {
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		return nil
	}
	n.started = false
	n.notifyAllWaitersLocked(errors.New("node stopped"))
	close(n.stopCh)
	n.mu.Unlock()

	n.wg.Wait()
	close(n.applyCh)
	return n.storage.Close()
}

// ApplyCh emits committed entries in order.
func (n *Node) ApplyCh() <-chan ApplyMsg {
	return n.applyCh
}

// Status returns current node status.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		Role:        n.role,
		CurrentTerm: n.currentTerm,
		LeaderID:    n.leaderID,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
}

// LeaderAddress returns the best-known leader endpoint.
func (n *Node) LeaderAddress() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == "" {
		return ""
	}
	return n.cluster[n.leaderID]
}

// Propose replicates a command and returns after commit.
func (n *Node) Propose(ctx context.Context, command []byte) error {
	cmdCopy := append([]byte(nil), command...)

	n.mu.Lock()
	if n.role != RoleLeader {
		err := n.notLeaderErrorLocked()
		n.mu.Unlock()
		return err
	}
	entry, err := n.appendLocalEntryLocked(cmdCopy)
	if err != nil {
		n.mu.Unlock()
		return fmt.Errorf("raft: persist append: %w", err)
	}
	index := entry.Index
	waitCh := make(chan error, 1)
	n.waiters[index] = append(n.waiters[index], waitCh)
	n.logf("accepted proposal index=%d", index)
	n.mu.Unlock()

	n.triggerReplication()

	select {
	case err := <-waitCh:
		return err
	case <-ctx.Done():
		n.mu.Lock()
		n.removeWaiterLocked(index, waitCh)
		n.mu.Unlock()
		return ctx.Err()
	case <-n.stopCh:
		return errors.New("node stopped")
	}
}

// HandleRequestVote serves RequestVote RPC.
func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, "")
	}

	upToDate := n.isCandidateLogUpToDateLocked(req.LastLogIndex, req.LastLogTerm)
	grant := false
	if upToDate && (n.votedFor == "" || n.votedFor == req.CandidateID) {
		n.votedFor = req.CandidateID
		n.resetElectionDeadlineLocked()
		if err := n.storage.SaveState(n.currentTerm, n.votedFor); err != nil {
			n.logf("persist vote error=%v", err)
		}
		grant = true
	}
	if grant {
		n.logf("granted vote to=%s", req.CandidateID)
	}
	return RequestVoteResponse{Term: n.currentTerm, VoteGranted: grant}
}

// HandleAppendEntries serves AppendEntries RPC.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	if req.Term < n.currentTerm {
		resp := AppendEntriesResponse{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: n.lastLogIndexLocked() + 1,
		}
		n.mu.Unlock()
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	} else {
		if n.role != RoleFollower {
			n.becomeFollowerLocked(req.Term, req.LeaderID)
		} else {
			n.leaderID = req.LeaderID
			n.resetElectionDeadlineLocked()
		}
	}

	lastIndex := n.lastLogIndexLocked()
	if req.PrevLogIndex > lastIndex {
		resp := AppendEntriesResponse{
			Term:          n.currentTerm,
			Success:       false,
			ConflictIndex: lastIndex + 1,
		}
		n.mu.Unlock()
		return resp
	}
	if req.PrevLogIndex > 0 {
		localPrevTerm := n.logTermAtLocked(req.PrevLogIndex)
		if localPrevTerm != req.PrevLogTerm {
			conflictTerm := localPrevTerm
			conflictIndex := req.PrevLogIndex
			for conflictIndex > 1 && n.logTermAtLocked(conflictIndex-1) == conflictTerm {
				conflictIndex--
			}
			resp := AppendEntriesResponse{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: conflictIndex,
				ConflictTerm:  conflictTerm,
			}
			n.mu.Unlock()
			return resp
		}
	}

	if len(req.Entries) > 0 {
		if err := n.appendFromLeaderLocked(req.Entries); err != nil {
			n.logf("append from leader failed error=%v", err)
			resp := AppendEntriesResponse{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: n.lastLogIndexLocked() + 1,
			}
			n.mu.Unlock()
			return resp
		}
	}

	if req.LeaderCommit > n.commitIndex {
		newCommit := req.LeaderCommit
		if newCommit > n.lastLogIndexLocked() {
			newCommit = n.lastLogIndexLocked()
		}
		if newCommit != n.commitIndex {
			n.commitIndex = newCommit
			if err := n.storage.SaveCommitIndex(n.commitIndex); err != nil {
				n.logf("persist commit index error=%v", err)
			}
		}
	}
	applied := n.applyCommittedLocked()
	matchIndex := req.PrevLogIndex + uint64(len(req.Entries))
	if matchIndex < n.lastLogIndexLocked() {
		matchIndex = n.lastLogIndexLocked()
	}
	resp := AppendEntriesResponse{
		Term:       n.currentTerm,
		Success:    true,
		MatchIndex: matchIndex,
	}
	n.mu.Unlock()

	n.deliverApply(applied)
	return resp
}

func (n *Node) replayCommittedLoop() {
	defer n.wg.Done()

	n.mu.Lock()
	start := uint64(1)
	end := n.commitIndex
	msgs := make([]ApplyMsg, 0, int(end))
	for i := start; i <= end; i++ {
		entry := n.log[int(i-1)]
		msgs = append(msgs, ApplyMsg{Index: entry.Index, Term: entry.Term, Command: append([]byte(nil), entry.Command...)})
		n.lastApplied = i
	}
	n.mu.Unlock()

	n.deliverApply(msgs)
}

func (n *Node) electionLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			start := n.role != RoleLeader && !n.electionRun && time.Now().After(n.electionUntil)
			if start {
				n.electionRun = true
			}
			n.mu.Unlock()
			if start {
				go n.runElection()
			}
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) runElection() {
	defer func() {
		n.mu.Lock()
		n.electionRun = false
		n.mu.Unlock()
	}()

	n.mu.Lock()
	if n.role == RoleLeader {
		n.mu.Unlock()
		return
	}
	n.role = RoleCandidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionDeadlineLocked()
	electionTimeout := n.electionDur
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.logTermAtLocked(lastLogIndex)
	if err := n.storage.SaveState(n.currentTerm, n.votedFor); err != nil {
		n.logf("persist election state error=%v", err)
	}
	peers := n.peerTargetsLocked()
	majority := n.majorityLocked()
	n.logf("election started term=%d", term)
	n.mu.Unlock()

	type voteResult struct {
		resp RequestVoteResponse
		err  error
	}
	results := make(chan voteResult, len(peers))

	for _, p := range peers {
		peerAddr := p.addr
		go func(addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			resp, err := n.rpc.RequestVote(ctx, addr, RequestVoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			results <- voteResult{resp: resp, err: err}
		}(peerAddr)
	}

	votes := 1
	timeout := time.NewTimer(electionTimeout)
	defer timeout.Stop()

	for received := 0; received < len(peers); received++ {
		select {
		case result := <-results:
			if result.err != nil {
				continue
			}
			n.mu.Lock()
			if n.currentTerm != term || n.role != RoleCandidate {
				n.mu.Unlock()
				return
			}
			if result.resp.Term > n.currentTerm {
				n.becomeFollowerLocked(result.resp.Term, "")
				n.mu.Unlock()
				return
			}
			if result.resp.VoteGranted {
				votes++
				if votes >= majority {
					n.becomeLeaderLocked()
					n.mu.Unlock()
					n.triggerReplication()
					return
				}
			}
			n.mu.Unlock()
		case <-timeout.C:
			return
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) heartbeatLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.broadcastAppendEntries()
		case <-n.replicateNow:
			n.broadcastAppendEntries()
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	peers := n.peerTargetsLocked()
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer.id, peer.addr)
	}
}

func (n *Node) replicateToPeer(peerID, peerAddr string) {
	for {
		n.mu.Lock()
		if n.role != RoleLeader {
			n.mu.Unlock()
			return
		}
		term := n.currentTerm
		next := n.nextIndex[peerID]
		if next == 0 {
			next = n.lastLogIndexLocked() + 1
			next = maxU64(1, next)
			n.nextIndex[peerID] = next
		}
		prevIndex := next - 1
		prevTerm := n.logTermAtLocked(prevIndex)
		entries := n.copyEntriesFromLocked(next)
		leaderCommit := n.commitIndex
		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		resp, err := n.rpc.AppendEntries(ctx, peerAddr, AppendEntriesRequest{
			Term:         term,
			LeaderID:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		})
		cancel()
		if err != nil {
			return
		}

		n.mu.Lock()
		if n.role != RoleLeader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		if resp.Term > n.currentTerm {
			n.becomeFollowerLocked(resp.Term, "")
			n.mu.Unlock()
			return
		}
		if resp.Success {
			match := prevIndex + uint64(len(entries))
			if match > n.matchIndex[peerID] {
				n.matchIndex[peerID] = match
			}
			nextCandidate := match + 1
			if nextCandidate > n.nextIndex[peerID] {
				n.nextIndex[peerID] = nextCandidate
			}
			applied := n.advanceCommitForLeaderLocked()
			n.mu.Unlock()
			n.deliverApply(applied)
			return
		}

		backoff := n.backoffNextIndexLocked(peerID, resp)
		if backoff < 1 {
			backoff = 1
		}
		n.nextIndex[peerID] = backoff
		n.mu.Unlock()

		select {
		case <-time.After(20 * time.Millisecond):
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) appendFromLeaderLocked(entries []LogEntry) error {
	for i := 0; i < len(entries); i++ {
		incoming := entries[i]
		if incoming.Index == 0 {
			return errors.New("incoming entry has zero index")
		}
		lastLogIndex := n.lastLogIndexLocked()
		if incoming.Index <= lastLogIndex {
			existingTerm := n.logTermAtLocked(incoming.Index)
			if existingTerm == incoming.Term {
				continue
			}
			if err := n.storage.TruncateLog(incoming.Index); err != nil {
				return fmt.Errorf("persist truncate: %w", err)
			}
			n.log = n.log[:int(incoming.Index-1)]
		} else if incoming.Index != lastLogIndex+1 {
			return fmt.Errorf("non-contiguous incoming index=%d expected=%d", incoming.Index, lastLogIndex+1)
		}
		for j := i; j < len(entries); j++ {
			entry := entries[j]
			expectedIndex := n.lastLogIndexLocked() + 1
			if entry.Index != expectedIndex {
				return fmt.Errorf("non-contiguous append index=%d expected=%d", entry.Index, expectedIndex)
			}
			entry.Command = append([]byte(nil), entry.Command...)
			if err := n.storage.AppendEntry(entry); err != nil {
				return fmt.Errorf("persist append: %w", err)
			}
			n.log = append(n.log, entry)
		}
		return nil
	}
	return nil
}

func (n *Node) advanceCommitForLeaderLocked() []ApplyMsg {
	majority := n.majorityLocked()
	lastIndex := n.lastLogIndexLocked()
	for idx := lastIndex; idx > n.commitIndex; idx-- {
		if n.logTermAtLocked(idx) != n.currentTerm {
			continue
		}
		votes := 1 // self
		for _, peerID := range n.peerIDs {
			if n.matchIndex[peerID] >= idx {
				votes++
			}
		}
		if votes >= majority {
			n.commitIndex = idx
			if err := n.storage.SaveCommitIndex(n.commitIndex); err != nil {
				n.logf("persist leader commit index error=%v", err)
			}
			return n.applyCommittedLocked()
		}
	}
	return nil
}

func (n *Node) applyCommittedLocked() []ApplyMsg {
	if n.commitIndex <= n.lastApplied {
		return nil
	}
	msgs := make([]ApplyMsg, 0, int(n.commitIndex-n.lastApplied))
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[int(n.lastApplied-1)]
		msgs = append(msgs, ApplyMsg{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: append([]byte(nil), entry.Command...),
		})
		n.notifyWaitersIndexLocked(entry.Index, nil)
	}
	return msgs
}

func (n *Node) deliverApply(msgs []ApplyMsg) {
	for _, msg := range msgs {
		select {
		case n.applyCh <- msg:
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) triggerReplication() {
	select {
	case n.replicateNow <- struct{}{}:
	default:
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = RoleLeader
	n.leaderID = n.id
	lastIndex := n.lastLogIndexLocked()
	for _, peerID := range n.peerIDs {
		n.nextIndex[peerID] = lastIndex + 1
		n.matchIndex[peerID] = 0
	}
	n.matchIndex[n.id] = lastIndex
	n.nextIndex[n.id] = lastIndex + 1
	// Append a no-op in the new term so prior-term tail entries can be
	// committed after election, per Raft's commit rule.
	if _, err := n.appendLocalEntryLocked(nil); err != nil {
		n.logf("append leader noop failed error=%v", err)
	}
	n.logf("became leader")
}

func (n *Node) becomeFollowerLocked(term uint64, leaderID string) {
	prevRole := n.role
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.role = RoleFollower
	n.leaderID = leaderID
	n.resetElectionDeadlineLocked()
	if err := n.storage.SaveState(n.currentTerm, n.votedFor); err != nil {
		n.logf("persist follower state error=%v", err)
	}
	if prevRole == RoleLeader || prevRole == RoleCandidate {
		n.notifyAllWaitersLocked(n.notLeaderErrorLocked())
	}
}

func (n *Node) notLeaderErrorLocked() error {
	addr := ""
	if n.leaderID != "" {
		addr = n.cluster[n.leaderID]
	}
	return &NotLeaderError{LeaderID: n.leaderID, LeaderAddr: addr}
}

func (n *Node) resetElectionDeadlineLocked() {
	delta := n.electionMax - n.electionMin
	if delta <= 0 {
		n.electionDur = time.Duration(n.electionMin) * time.Millisecond
	} else {
		n.electionDur = time.Duration(n.electionMin+n.rnd.Intn(delta+1)) * time.Millisecond
	}
	n.electionUntil = time.Now().Add(n.electionDur)
}

func (n *Node) isCandidateLogUpToDateLocked(lastIndex, lastTerm uint64) bool {
	myLastIndex := n.lastLogIndexLocked()
	myLastTerm := n.logTermAtLocked(myLastIndex)
	if lastTerm != myLastTerm {
		return lastTerm > myLastTerm
	}
	return lastIndex >= myLastIndex
}

func (n *Node) backoffNextIndexLocked(peerID string, resp AppendEntriesResponse) uint64 {
	if resp.ConflictTerm != 0 {
		lastOfTerm := n.findLastIndexOfTermLocked(resp.ConflictTerm)
		if lastOfTerm > 0 {
			return lastOfTerm + 1
		}
	}
	if resp.ConflictIndex != 0 {
		return resp.ConflictIndex
	}
	if n.nextIndex[peerID] > 1 {
		return n.nextIndex[peerID] - 1
	}
	return 1
}

func (n *Node) findLastIndexOfTermLocked(term uint64) uint64 {
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Term == term {
			return n.log[i].Index
		}
		if i == 0 {
			break
		}
	}
	return 0
}

func (n *Node) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) logTermAtLocked(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index > uint64(len(n.log)) {
		return 0
	}
	return n.log[int(index-1)].Term
}

func (n *Node) copyEntriesFromLocked(fromIndex uint64) []LogEntry {
	if fromIndex == 0 {
		fromIndex = 1
	}
	if fromIndex > n.lastLogIndexLocked() {
		return nil
	}
	start := int(fromIndex - 1)
	entries := make([]LogEntry, 0, len(n.log)-int(start))
	for _, entry := range n.log[start:] {
		entries = append(entries, LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: append([]byte(nil), entry.Command...),
		})
	}
	return entries
}

type peerTarget struct {
	id   string
	addr string
}

func (n *Node) peerTargetsLocked() []peerTarget {
	out := make([]peerTarget, 0, len(n.peerIDs))
	for _, id := range n.peerIDs {
		addr := n.cluster[id]
		if addr == "" {
			continue
		}
		out = append(out, peerTarget{id: id, addr: addr})
	}
	return out
}

func (n *Node) majorityLocked() int {
	total := len(n.peerIDs) + 1
	return total/2 + 1
}

func (n *Node) notifyWaitersIndexLocked(index uint64, err error) {
	chs := n.waiters[index]
	delete(n.waiters, index)
	for _, ch := range chs {
		ch <- err
	}
}

func (n *Node) notifyAllWaitersLocked(err error) {
	for idx, chs := range n.waiters {
		for _, ch := range chs {
			ch <- err
		}
		delete(n.waiters, idx)
	}
}

func (n *Node) removeWaiterLocked(index uint64, target chan error) {
	chs := n.waiters[index]
	if len(chs) == 0 {
		return
	}
	filtered := chs[:0]
	for _, ch := range chs {
		if ch != target {
			filtered = append(filtered, ch)
		}
	}
	if len(filtered) == 0 {
		delete(n.waiters, index)
		return
	}
	n.waiters[index] = filtered
}

func (n *Node) logf(format string, args ...any) {
	n.logger.Printf("node=%s term=%d role=%s "+format, append([]any{n.id, n.currentTerm, n.role}, args...)...)
}

func (n *Node) appendLocalEntryLocked(command []byte) (LogEntry, error) {
	index := n.lastLogIndexLocked() + 1
	entry := LogEntry{
		Index:   index,
		Term:    n.currentTerm,
		Command: append([]byte(nil), command...),
	}
	if err := n.storage.AppendEntry(entry); err != nil {
		return LogEntry{}, err
	}
	n.log = append(n.log, entry)
	n.matchIndex[n.id] = index
	n.nextIndex[n.id] = index + 1
	return entry, nil
}

func validateLog(entries []LogEntry) error {
	for i, entry := range entries {
		expected := uint64(i + 1)
		if entry.Index != expected {
			return fmt.Errorf("entry index %d, expected %d", entry.Index, expected)
		}
	}
	return nil
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
