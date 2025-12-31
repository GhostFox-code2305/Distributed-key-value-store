package storage

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// CompactionManager handles background compaction of SSTables
type CompactionManager struct {
	store          *LSMStore
	stopCh         chan struct{}
	wg             sync.WaitGroup
	mu             sync.Mutex
	running        bool
	compactionRate time.Duration
	stats          CompactionStats
}

// CompactionStats tracks compaction metrics
type CompactionStats struct {
	TotalCompactions    int64
	TotalBytesReclaimed int64
	TotalKeysRemoved    int64
	LastCompactionTime  time.Time
	mu                  sync.RWMutex
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(store *LSMStore) *CompactionManager {
	return &CompactionManager{
		store:          store,
		stopCh:         make(chan struct{}),
		compactionRate: 30 * time.Second, // Run compaction every 30 seconds
		stats:          CompactionStats{},
	}
}

// Start begins the background compaction process
func (cm *CompactionManager) Start() {
	cm.mu.Lock()
	if cm.running {
		cm.mu.Unlock()
		return
	}
	cm.running = true
	cm.mu.Unlock()

	cm.wg.Add(1)
	go cm.compactionLoop()
	log.Println("🔄 Compaction manager started")
}

// Stop halts the background compaction process
func (cm *CompactionManager) Stop() {
	cm.mu.Lock()
	if !cm.running {
		cm.mu.Unlock()
		return
	}
	cm.running = false
	cm.mu.Unlock()

	close(cm.stopCh)
	cm.wg.Wait()
	log.Println("🛑 Compaction manager stopped")
}

// compactionLoop runs periodic compaction checks
func (cm *CompactionManager) compactionLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(cm.compactionRate)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			if err := cm.maybeCompact(); err != nil {
				log.Printf("⚠️  Compaction error: %v", err)
			}
		}
	}
}

// maybeCompact checks if compaction is needed and performs it
func (cm *CompactionManager) maybeCompact() error {
	cm.store.mu.RLock()
	numSSTables := len(cm.store.sstables)
	cm.store.mu.RUnlock()

	// Trigger compaction if we have more than 4 SSTables
	if numSSTables <= 4 {
		return nil
	}

	log.Printf("🔄 Starting compaction (%d SSTables)", numSSTables)
	startTime := time.Now()

	if err := cm.compact(); err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	duration := time.Since(startTime)
	log.Printf("✅ Compaction completed in %v", duration)

	cm.stats.mu.Lock()
	cm.stats.TotalCompactions++
	cm.stats.LastCompactionTime = time.Now()
	cm.stats.mu.Unlock()

	return nil
}

// compact performs the actual compaction
func (cm *CompactionManager) compact() error {
	cm.store.mu.Lock()

	// Select SSTables to compact (all of them in simple size-tiered compaction)
	sstablesToCompact := cm.store.sstables
	if len(sstablesToCompact) == 0 {
		cm.store.mu.Unlock()
		return nil
	}

	// Create copies of SSTable references
	compactTables := make([]*SSTable, len(sstablesToCompact))
	copy(compactTables, sstablesToCompact)

	// Get next table ID
	newTableID := cm.store.nextTableID
	cm.store.nextTableID++

	cm.store.mu.Unlock()

	// Perform merge (without holding locks for I/O)
	mergedEntries, stats, err := cm.mergeSSTables(compactTables)
	if err != nil {
		return fmt.Errorf("failed to merge SSTables: %w", err)
	}

	// Write merged data to new SSTable
	writer, err := NewSSTableWriter(cm.store.dataDir, newTableID)
	if err != nil {
		return fmt.Errorf("failed to create new SSTable: %w", err)
	}

	for _, entry := range mergedEntries {
		if err := writer.Write(entry.Key, entry.Value); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
	}

	if err := writer.Finalize(); err != nil {
		return fmt.Errorf("failed to finalize SSTable: %w", err)
	}

	// Open the new compacted SSTable
	newSSTable, err := OpenSSTable(writer.filePath)
	if err != nil {
		return fmt.Errorf("failed to open new SSTable: %w", err)
	}

	// Update store: replace old SSTables with new one
	cm.store.mu.Lock()

	// Remove old SSTables from the list
	cm.store.sstables = []*SSTable{newSSTable}

	// Get file paths of old SSTables for deletion
	oldFiles := make([]string, len(compactTables))
	for i, sst := range compactTables {
		oldFiles[i] = sst.FilePath()
	}

	cm.store.mu.Unlock()

	// Delete old SSTable files
	for _, filePath := range oldFiles {
		if err := os.Remove(filePath); err != nil {
			log.Printf("⚠️  Failed to delete old SSTable %s: %v", filePath, err)
		}
	}

	// Update stats
	cm.stats.mu.Lock()
	cm.stats.TotalKeysRemoved += stats.KeysRemoved
	cm.stats.TotalBytesReclaimed += stats.BytesReclaimed
	cm.stats.mu.Unlock()

	log.Printf("📊 Compaction stats: %d keys removed, %d bytes reclaimed",
		stats.KeysRemoved, stats.BytesReclaimed)

	return nil
}

// MergeStats tracks statistics from a merge operation
type MergeStats struct {
	KeysRemoved    int64
	BytesReclaimed int64
}

// mergeSSTables merges multiple SSTables into a single sorted list
func (cm *CompactionManager) mergeSSTables(sstables []*SSTable) ([]Entry, *MergeStats, error) {
	// Collect all entries from all SSTables
	type keyEntry struct {
		key      string
		value    []byte
		tableIdx int // Which SSTable this came from (lower = newer)
	}

	allEntries := make(map[string]*keyEntry)
	totalOriginalSize := int64(0)

	// Read all entries from all SSTables
	for tableIdx, sst := range sstables {
		for _, indexEntry := range sst.index {
			key := string(indexEntry.Key)
			value, found, err := sst.Get(indexEntry.Key)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read from SSTable: %w", err)
			}
			if !found {
				continue
			}

			totalOriginalSize += int64(len(indexEntry.Key) + len(value))

			// Keep the newest version (lower tableIdx = newer)
			if existing, exists := allEntries[key]; !exists || tableIdx < existing.tableIdx {
				allEntries[key] = &keyEntry{
					key:      key,
					value:    value,
					tableIdx: tableIdx,
				}
			}
		}
	}

	// Filter out tombstones and convert to sorted list
	var result []Entry
	tombstone := []byte("__TOMBSTONE__")
	keysRemoved := int64(0)

	for _, entry := range allEntries {
		// Skip tombstones (deleted keys)
		if bytes.Equal(entry.value, tombstone) {
			keysRemoved++
			continue
		}

		result = append(result, Entry{
			Key:   []byte(entry.key),
			Value: entry.value,
		})
	}

	// Sort by key
	sortEntries(result)

	// Calculate bytes reclaimed
	newSize := int64(0)
	for _, entry := range result {
		newSize += int64(len(entry.Key) + len(entry.Value))
	}
	bytesReclaimed := totalOriginalSize - newSize

	stats := &MergeStats{
		KeysRemoved:    keysRemoved,
		BytesReclaimed: bytesReclaimed,
	}

	return result, stats, nil
}

// sortEntries sorts entries by key
func sortEntries(entries []Entry) {
	// Simple bubble sort for now (could use sort.Slice for better performance)
	n := len(entries)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if bytes.Compare(entries[j].Key, entries[j+1].Key) > 0 {
				entries[j], entries[j+1] = entries[j+1], entries[j]
			}
		}
	}
}

// GetStats returns compaction statistics
func (cm *CompactionManager) GetStats() map[string]interface{} {
	cm.stats.mu.RLock()
	defer cm.stats.mu.RUnlock()

	return map[string]interface{}{
		"total_compactions":     cm.stats.TotalCompactions,
		"total_bytes_reclaimed": cm.stats.TotalBytesReclaimed,
		"total_keys_removed":    cm.stats.TotalKeysRemoved,
		"last_compaction":       cm.stats.LastCompactionTime.Format(time.RFC3339),
	}
}

// ForceCompact triggers an immediate compaction (useful for testing)
func (cm *CompactionManager) ForceCompact() error {
	log.Println("🔄 Forcing compaction...")
	return cm.compact()
}
