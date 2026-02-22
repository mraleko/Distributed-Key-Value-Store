package kv

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

const (
	OpPut = "put"
	OpDel = "del"
)

// Command is a deterministic key-value state-machine operation.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func EncodePut(key, value string) ([]byte, error) {
	if key == "" {
		return nil, errors.New("kv: key is required")
	}
	return json.Marshal(Command{Op: OpPut, Key: key, Value: value})
}

func EncodeDel(key string) ([]byte, error) {
	if key == "" {
		return nil, errors.New("kv: key is required")
	}
	return json.Marshal(Command{Op: OpDel, Key: key})
}

func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return Command{}, fmt.Errorf("kv: decode command: %w", err)
	}
	if cmd.Key == "" {
		return Command{}, errors.New("kv: command key is required")
	}
	switch cmd.Op {
	case OpPut, OpDel:
		return cmd, nil
	default:
		return Command{}, fmt.Errorf("kv: unsupported op %q", cmd.Op)
	}
}

// Store is the deterministic KV state machine.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Apply(cmd Command) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
	case OpDel:
		delete(s.data, cmd.Key)
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
