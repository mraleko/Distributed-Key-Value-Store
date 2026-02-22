package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"dkv/raft"
)

// RaftHTTPClient sends Raft RPCs over HTTP/JSON.
type RaftHTTPClient struct {
	client *http.Client
}

func NewRaftHTTPClient() *RaftHTTPClient {
	return &RaftHTTPClient{
		client: &http.Client{Timeout: 800 * time.Millisecond},
	}
}

func (c *RaftHTTPClient) RequestVote(ctx context.Context, target string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	var resp raft.RequestVoteResponse
	if err := c.postJSON(ctx, target, "/raft/requestVote", req, &resp); err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return resp, nil
}

func (c *RaftHTTPClient) AppendEntries(ctx context.Context, target string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	var resp raft.AppendEntriesResponse
	if err := c.postJSON(ctx, target, "/raft/appendEntries", req, &resp); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return resp, nil
}

func (c *RaftHTTPClient) postJSON(ctx context.Context, target, path string, reqBody any, out any) error {
	encoded, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("transport: marshal request: %w", err)
	}
	url := strings.TrimRight(target, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("transport: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("transport: post %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("transport: post %s: status=%d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("transport: decode response: %w", err)
	}
	return nil
}
