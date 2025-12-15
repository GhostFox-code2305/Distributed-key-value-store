package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

type Store struct {
	data map[string][]byte
	wal  *WAL
	mu   sync.RWMutex
}

func NewStore(dataDir string) (*Store, error) {
	wal, err := NewWAL(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	store := &Store{
		data: make(map[string][]byte),
		wal:  wal,
	}

	if err := store.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	return store, nil
}

func (s *Store) Put(key string, value []byte) error {
	entry := Entry{
		Timestamp: time.Now().UnixNano(),
		Op:        OpPut,
		Key:       []byte(key),
		Value:     value,
	}

	if err := s.wal.Write(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	s.data[key] = valueCopy

	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return valueCopy, nil
}

func (s *Store) Delete(key string) error {
	entry := Entry{
		Timestamp: time.Now().UnixNano(),
		Op:        OpDelete,
		Key:       []byte(key),
		Value:     nil,
	}

	if err := s.wal.Write(entry); err != nil {
		return fmt.Errorf("failed to write delete to WAL: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)

	return nil
}

func (s *Store) recover() error {
	entries, err := s.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	for _, entry := range entries {
		switch entry.Op {
		case OpPut:
			s.data[string(entry.Key)] = entry.Value
		case OpDelete:
			delete(s.data, string(entry.Key))
		}
	}

	return nil
}

func (s *Store) Close() error {
	return s.wal.Close()
}

func (s *Store) Stats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"num_keys": len(s.data),
	}
}
