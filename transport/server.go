package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"dkv/kv"
	"dkv/raft"
)

// Server serves client API and Raft RPC endpoints.
type Server struct {
	node *raft.Node
	kv   *kv.Store

	httpServer *http.Server
	logger     *log.Logger
}

func NewServer(listenAddr string, node *raft.Node, store *kv.Store) *Server {
	mux := http.NewServeMux()
	s := &Server{
		node:   node,
		kv:     store,
		logger: log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),
		httpServer: &http.Server{
			Addr:              listenAddr,
			Handler:           mux,
			ReadHeaderTimeout: 3 * time.Second,
			ReadTimeout:       8 * time.Second,
			WriteTimeout:      8 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
	}
	mux.HandleFunc("/raft/requestVote", s.handleRequestVote)
	mux.HandleFunc("/raft/appendEntries", s.handleAppendEntries)
	mux.HandleFunc("/raft/installSnapshot", s.handleInstallSnapshot)
	mux.HandleFunc("/kv/put", s.handlePut)
	mux.HandleFunc("/kv/del", s.handleDel)
	mux.HandleFunc("/kv/get", s.handleGet)
	mux.HandleFunc("/cluster/status", s.handleStatus)
	return s
}

func (s *Server) Run(errCh chan<- error) {
	err := s.httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		errCh <- err
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req raft.RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	resp := s.node.HandleRequestVote(req)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req raft.AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	resp := s.node.HandleAppendEntries(req)
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleInstallSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req raft.InstallSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	_ = req
	writeJSON(w, http.StatusNotImplemented, raft.InstallSnapshotResponse{
		Term:    s.node.Status().CurrentTerm,
		Success: false,
	})
}

type putRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type delRequest struct {
	Key string `json:"key"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	cmd, err := kv.EncodePut(req.Key, req.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.node.Propose(ctx, cmd); err != nil {
		s.handleProposeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req delRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	cmd, err := kv.EncodeDel(req.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.node.Propose(ctx, cmd); err != nil {
		s.handleProposeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key query parameter is required", http.StatusBadRequest)
		return
	}
	value, found := s.kv.Get(key)
	writeJSON(w, http.StatusOK, map[string]any{
		"found": found,
		"value": value,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	status := s.node.Status()
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleProposeError(w http.ResponseWriter, r *http.Request, err error) {
	var notLeader *raft.NotLeaderError
	if errors.As(err, &notLeader) {
		if notLeader.LeaderAddr != "" {
			location := strings.TrimRight(notLeader.LeaderAddr, "/") + r.URL.RequestURI()
			w.Header().Set("Location", location)
			writeJSON(w, http.StatusTemporaryRedirect, map[string]any{
				"error":      "not leader",
				"leaderId":   notLeader.LeaderID,
				"leaderAddr": notLeader.LeaderAddr,
			})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "leader unknown",
		})
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "write timeout"})
		return
	}
	s.logger.Printf("client write error: %v", err)
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": fmt.Sprintf("write failed: %v", err)})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
