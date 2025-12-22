package storage

import (
	"fmt"
	"testing"
)

func TestLSMStore_BasicOperations(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create LSM store: %v", err)
	}
	defer store.Close()

	// Test Put
	if err := store.Put("key1", []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	value, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", value)
	}

	// Test Delete
	if err := store.Delete("key1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get after delete should fail
	_, err = store.Get("key1")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after delete, got: %v", err)
	}
}

func TestLSMStore_MemTableFlush(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create LSM store: %v", err)
	}
	defer store.Close()

	// Write enough data to trigger flush (>64MB)
	valueSize := 1024 // 1KB per value
	numKeys := 70000  // 70MB total

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i % 256)
		}

		if err := store.Put(key, value); err != nil {
			t.Fatalf("Put failed at key %d: %v", i, err)
		}
	}

	// Check stats - should have created SSTables
	stats := store.Stats()
	numSSTables := stats["num_sstables"].(int)

	if numSSTables == 0 {
		t.Error("Expected at least one SSTable after writing 70MB")
	}

	t.Logf("Created %d SSTables", numSSTables)

	// Verify we can still read the data
	value, err := store.Get("key_100")
	if err != nil {
		t.Fatalf("Failed to get key after flush: %v", err)
	}
	if len(value) != valueSize {
		t.Errorf("Expected value size %d, got %d", valueSize, len(value))
	}
}

func TestLSMStore_ReadFromSSTable(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create LSM store: %v", err)
	}

	// Write data and force flush
	testData := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "red",
	}

	for k, v := range testData {
		if err := store.Put(k, []byte(v)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Manually flush
	if err := store.maybeFlush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	store.Close()

	// Reopen store - data should be in SSTables
	store2, err := NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	defer store2.Close()

	// Verify all data
	for k, expectedValue := range testData {
		value, err := store2.Get(k)
		if err != nil {
			t.Fatalf("Get failed for key %s: %v", k, err)
		}
		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", k, expectedValue, value)
		}
	}
}

func TestLSMStore_CrashRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	store1, err := NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Write some data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		if err := store1.Put(key, []byte(value)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Close without explicit flush (data in MemTable + WAL)
	store1.Close()

	// Reopen - should recover from WAL
	store2, err := NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	defer store2.Close()

	// Verify all data recovered
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		expectedValue := fmt.Sprintf("value_%d", i)

		value, err := store2.Get(key)
		if err != nil {
			t.Fatalf("Get failed after recovery for key %s: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, value)
		}
	}
}

func TestMemTable_SkipList(t *testing.T) {
	mem := NewMemTable()

	// Insert in random order
	testData := map[string]string{
		"zebra":  "stripes",
		"apple":  "red",
		"mango":  "yellow",
		"banana": "yellow",
	}

	for k, v := range testData {
		mem.Put([]byte(k), []byte(v))
	}

	// Get all entries - should be sorted
	entries := mem.Iterator()

	if len(entries) != len(testData) {
		t.Errorf("Expected %d entries, got %d", len(testData), len(entries))
	}

	// Verify sorted order
	prevKey := ""
	for _, entry := range entries {
		key := string(entry.Key)
		if prevKey != "" && key < prevKey {
			t.Errorf("Entries not in sorted order: %s came after %s", key, prevKey)
		}
		prevKey = key
	}

	t.Logf("Sorted order: ")
	for _, entry := range entries {
		t.Logf("  %s: %s", entry.Key, entry.Value)
	}
}

func BenchmarkLSMStore_Put(b *testing.B) {
	tmpDir := b.TempDir()
	store, _ := NewLSMStore(tmpDir)
	defer store.Close()

	value := []byte("benchmark_value_with_reasonable_length")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		store.Put(key, value)
	}
}

func BenchmarkLSMStore_Get(b *testing.B) {
	tmpDir := b.TempDir()
	store, _ := NewLSMStore(tmpDir)
	defer store.Close()

	// Prepopulate
	value := []byte("benchmark_value")
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		store.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%10000)
		store.Get(key)
	}
}
