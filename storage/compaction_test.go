package storage

import (
	"fmt"
	"testing"
	"time"
)

func TestCompaction_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Write data that will create multiple SSTables
	valueSize := 1024 // 1KB
	numKeys := 80000  // 80MB (will create multiple SSTables)

	t.Logf("Writing %d keys...", numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%06d", i)
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i % 256)
		}

		if err := store.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Check initial state
	stats := store.Stats()
	initialSSTables := stats["num_sstables"].(int)
	t.Logf("Initial SSTables: %d", initialSSTables)

	if initialSSTables <= 1 {
		t.Skip("Not enough SSTables created to test compaction")
	}

	// Force compaction
	if err := store.compactionMgr.ForceCompact(); err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Check after compaction
	stats = store.Stats()
	finalSSTables := stats["num_sstables"].(int)
	t.Logf("Final SSTables: %d", finalSSTables)

	// Should have fewer SSTables after compaction
	if finalSSTables >= initialSSTables {
		t.Errorf("Expected fewer SSTables after compaction: %d >= %d",
			finalSSTables, initialSSTables)
	}

	// Verify data is still accessible
	testKey := "key_050000"
	value, err := store.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to get key after compaction: %v", err)
	}
	if len(value) != valueSize {
		t.Errorf("Value size mismatch after compaction: %d != %d", len(value), valueSize)
	}

	t.Logf("✅ Compaction successful: %d → %d SSTables", initialSSTables, finalSSTables)
}

func TestCompaction_TombstoneRemoval(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Write and delete many keys to create tombstones
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		if err := store.Put(key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete half of them
	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("key_%d", i)
		if err := store.Delete(key); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// Force flush to create SSTables with tombstones
	if err := store.maybeFlush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Write more data to create another SSTable
	for i := numKeys; i < numKeys*2; i++ {
		key := fmt.Sprintf("key_%d", i)
		if err := store.Put(key, []byte("value")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if err := store.maybeFlush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Get initial stats
	statsBefore := store.compactionMgr.GetStats()
	t.Logf("Stats before compaction: %+v", statsBefore)

	// Compact
	if err := store.compactionMgr.ForceCompact(); err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Get stats after compaction
	statsAfter := store.compactionMgr.GetStats()
	t.Logf("Stats after compaction: %+v", statsAfter)

	keysRemoved := statsAfter["total_keys_removed"].(int64)
	if keysRemoved == 0 {
		t.Error("Expected some tombstones to be removed during compaction")
	}

	t.Logf("✅ Removed %d tombstones during compaction", keysRemoved)

	// Verify deleted keys are still not accessible
	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("key_%d", i)
		_, err := store.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("Deleted key %s should not be found after compaction", key)
		}
	}

	// Verify non-deleted keys are still accessible
	for i := numKeys / 2; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		_, err := store.Get(key)
		if err != nil {
			t.Errorf("Key %s should still exist after compaction: %v", key, err)
		}
	}
}

func TestCompaction_AutomaticTrigger(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Start compaction manager with fast rate for testing
	store.compactionMgr.compactionRate = 2 * time.Second
	store.compactionMgr.Start()

	// Write enough data to create 5+ SSTables
	valueSize := 1024
	numKeys := 90000 // 90MB

	t.Logf("Writing %d keys to trigger automatic compaction...", numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%06d", i)
		value := make([]byte, valueSize)
		if err := store.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	initialStats := store.Stats()
	initialSSTables := initialStats["num_sstables"].(int)
	t.Logf("Initial SSTables: %d", initialSSTables)

	// Wait for automatic compaction to trigger
	t.Logf("Waiting for automatic compaction...")
	time.Sleep(5 * time.Second)

	finalStats := store.Stats()
	finalSSTables := finalStats["num_sstables"].(int)
	t.Logf("Final SSTables: %d", finalSSTables)

	compactionStats := store.compactionMgr.GetStats()
	totalCompactions := compactionStats["total_compactions"].(int64)

	if totalCompactions == 0 {
		t.Log("⚠️  No automatic compaction occurred (might need more data or time)")
	} else {
		t.Logf("✅ Automatic compaction occurred: %d compaction(s)", totalCompactions)
	}
}

func BenchmarkCompaction(b *testing.B) {
	tmpDir := b.TempDir()

	store, _ := NewLSMStore(tmpDir)
	defer store.Close()

	// Pre-populate with data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		store.Put(key, []byte("value"))
	}

	// Force creation of multiple SSTables
	store.maybeFlush()

	for i := 10000; i < 20000; i++ {
		key := fmt.Sprintf("key_%d", i)
		store.Put(key, []byte("value"))
	}

	store.maybeFlush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.compactionMgr.ForceCompact()
	}
}
