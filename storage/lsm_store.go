package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	// MemTableSizeThreshold is the size limit before flushing to disk (64MB)
	MemTableSizeThreshold = 64 * 1024 * 1024
)

// LSMStore is a Log-Structured Merge-Tree based key-value store
type LSMStore struct {
	memTable       *MemTable
	immutableTable *MemTable  // MemTable being flushed
	sstables       []*SSTable // Sorted by newest to oldest
	wal            *WAL
	dataDir        string
	nextTableID    int
	mu             sync.RWMutex
	flushMu        sync.Mutex
}

// NewLSMStore creates a new LSM-based store
func NewLSMStore(dataDir string) (*LSMStore, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	wal, err := NewWAL(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	store := &LSMStore{
		memTable:    NewMemTable(),
		dataDir:     dataDir,
		sstables:    make([]*SSTable, 0),
		wal:         wal,
		nextTableID: 0,
	}

	// Load existing SSTables
	if err := store.loadSSTables(); err != nil {
		return nil, fmt.Errorf("failed to load SSTables: %w", err)
	}

	// Recover from WAL
	if err := store.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	return store, nil
}

// Put stores a key-value pair
func (s *LSMStore) Put(key string, value []byte) error {
	// Write to WAL first (durability)
	entry := Entry{
		Timestamp: time.Now().UnixNano(),
		Op:        OpPut,
		Key:       []byte(key),
		Value:     value,
	}

	if err := s.wal.Write(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Write to MemTable
	s.mu.Lock()
	s.memTable.Put([]byte(key), value)
	memSize := s.memTable.Size()
	s.mu.Unlock()

	// Check if MemTable is full
	if memSize >= MemTableSizeThreshold {
		if err := s.maybeFlush(); err != nil {
			return fmt.Errorf("failed to flush MemTable: %w", err)
		}
	}

	return nil
}

// Get retrieves a value by key
func (s *LSMStore) Get(key string) ([]byte, error) {
	keyBytes := []byte(key)

	s.mu.RLock()

	// Check MemTable first
	if value, found := s.memTable.Get(keyBytes); found {
		s.mu.RUnlock()
		return value, nil
	}

	// Check immutable MemTable (if being flushed)
	if s.immutableTable != nil {
		if value, found := s.immutableTable.Get(keyBytes); found {
			s.mu.RUnlock()
			return value, nil
		}
	}

	// Check SSTables (newest to oldest)
	sstables := make([]*SSTable, len(s.sstables))
	copy(sstables, s.sstables)
	s.mu.RUnlock()

	for _, sst := range sstables {
		value, found, err := sst.Get(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading SSTable: %w", err)
		}
		if found {
			// Check for tombstone
			if bytes.Equal(value, []byte("__TOMBSTONE__")) {
				return nil, ErrKeyNotFound
			}
			return value, nil
		}
	}

	return nil, ErrKeyNotFound
}

// Delete removes a key-value pair
func (s *LSMStore) Delete(key string) error {
	// Write to WAL
	entry := Entry{
		Timestamp: time.Now().UnixNano(),
		Op:        OpDelete,
		Key:       []byte(key),
		Value:     nil,
	}

	if err := s.wal.Write(entry); err != nil {
		return fmt.Errorf("failed to write delete to WAL: %w", err)
	}

	// Write tombstone to MemTable
	s.mu.Lock()
	s.memTable.Delete([]byte(key))
	memSize := s.memTable.Size()
	s.mu.Unlock()

	// Check if MemTable is full
	if memSize >= MemTableSizeThreshold {
		if err := s.maybeFlush(); err != nil {
			return fmt.Errorf("failed to flush MemTable: %w", err)
		}
	}

	return nil
}

// maybeFlush flushes MemTable to disk if needed
func (s *LSMStore) maybeFlush() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()

	// Double-check size after acquiring lock
	if s.memTable.Size() < MemTableSizeThreshold {
		s.mu.Unlock()
		return nil
	}

	// Move current MemTable to immutable
	s.immutableTable = s.memTable
	s.memTable = NewMemTable()

	tableToFlush := s.immutableTable
	tableID := s.nextTableID
	s.nextTableID++

	s.mu.Unlock()

	// Flush to disk (no locks held during I/O)
	if err := s.flushToDisk(tableToFlush, tableID); err != nil {
		return err
	}

	// Clear immutable table and reset WAL
	s.mu.Lock()
	s.immutableTable = nil
	s.mu.Unlock()

	if err := s.wal.Reset(); err != nil {
		return fmt.Errorf("failed to reset WAL: %w", err)
	}

	return nil
}

// flushToDisk writes MemTable entries to a new SSTable
func (s *LSMStore) flushToDisk(memTable *MemTable, tableID int) error {
	writer, err := NewSSTableWriter(s.dataDir, tableID)
	if err != nil {
		return err
	}

	// Get all entries in sorted order
	entries := memTable.Iterator()

	// Write to SSTable
	for _, entry := range entries {
		if err := writer.Write(entry.Key, entry.Value); err != nil {
			return fmt.Errorf("failed to write entry to SSTable: %w", err)
		}
	}

	// Finalize the SSTable
	if err := writer.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize SSTable: %w", err)
	}

	// Open the new SSTable and add to list
	sst, err := OpenSSTable(writer.filePath)
	if err != nil {
		return fmt.Errorf("failed to open new SSTable: %w", err)
	}

	s.mu.Lock()
	// Add to front (newest)
	s.sstables = append([]*SSTable{sst}, s.sstables...)
	s.mu.Unlock()

	return nil
}

// loadSSTables loads existing SSTables from disk
func (s *LSMStore) loadSSTables() error {
	pattern := filepath.Join(s.dataDir, "sstable_*.db")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	// Sort files by name (which includes ID) to get newest first
	sort.Slice(files, func(i, j int) bool {
		return files[i] > files[j]
	})

	for _, file := range files {
		sst, err := OpenSSTable(file)
		if err != nil {
			return fmt.Errorf("failed to open SSTable %s: %w", file, err)
		}
		s.sstables = append(s.sstables, sst)

		// Update nextTableID
		var id int
		fmt.Sscanf(filepath.Base(file), "sstable_%d.db", &id)
		if id >= s.nextTableID {
			s.nextTableID = id + 1
		}
	}

	return nil
}

// recover replays WAL entries to restore state
func (s *LSMStore) recover() error {
	entries, err := s.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	for _, entry := range entries {
		switch entry.Op {
		case OpPut:
			s.memTable.Put(entry.Key, entry.Value)
		case OpDelete:
			s.memTable.Delete(entry.Key)
		}
	}

	return nil
}

// Close closes the store
func (s *LSMStore) Close() error {
	// Flush any remaining data
	if s.memTable.Size() > 0 {
		if err := s.maybeFlush(); err != nil {
			return err
		}
	}

	return s.wal.Close()
}

// Stats returns storage statistics
func (s *LSMStore) Stats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"memtable_size": s.memTable.Size(),
		"num_sstables":  len(s.sstables),
	}
}
