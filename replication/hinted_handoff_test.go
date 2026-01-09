package replication

import (
	"testing"
	"time"
)

func TestHintedHandoff_StoreAndRetrieve(t *testing.T) {
	tmpDir := t.TempDir()
	hh, err := NewHintedHandoff(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff: %v", err)
	}

	// Store a hint
	err = hh.StoreHint("node2", "test_key", []byte("test_value"), time.Now().UnixNano(), 1)
	if err != nil {
		t.Fatalf("Failed to store hint: %v", err)
	}

	// Retrieve hints
	hints := hh.GetHints("node2")
	if len(hints) != 1 {
		t.Fatalf("Expected 1 hint, got %d", len(hints))
	}

	if hints[0].Key != "test_key" {
		t.Errorf("Expected key 'test_key', got '%s'", hints[0].Key)
	}

	if string(hints[0].Value) != "test_value" {
		t.Errorf("Expected value 'test_value', got '%s'", hints[0].Value)
	}
}

func TestHintedHandoff_ClearHints(t *testing.T) {
	tmpDir := t.TempDir()
	hh, err := NewHintedHandoff(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff: %v", err)
	}

	// Store hints
	hh.StoreHint("node2", "key1", []byte("value1"), time.Now().UnixNano(), 1)
	hh.StoreHint("node2", "key2", []byte("value2"), time.Now().UnixNano(), 2)

	if hh.GetHintCountForNode("node2") != 2 {
		t.Errorf("Expected 2 hints for node2")
	}

	// Clear hints
	err = hh.ClearHints("node2")
	if err != nil {
		t.Fatalf("Failed to clear hints: %v", err)
	}

	if hh.GetHintCountForNode("node2") != 0 {
		t.Errorf("Expected 0 hints after clear, got %d", hh.GetHintCountForNode("node2"))
	}
}

func TestHintedHandoff_RemoveHint(t *testing.T) {
	tmpDir := t.TempDir()
	hh, err := NewHintedHandoff(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff: %v", err)
	}

	// Store multiple hints
	hh.StoreHint("node2", "key1", []byte("value1"), time.Now().UnixNano(), 1)
	hh.StoreHint("node2", "key2", []byte("value2"), time.Now().UnixNano(), 2)
	hh.StoreHint("node2", "key3", []byte("value3"), time.Now().UnixNano(), 3)

	// Remove middle hint
	hh.RemoveHint("node2", 1)

	hints := hh.GetHints("node2")
	if len(hints) != 2 {
		t.Errorf("Expected 2 hints after removal, got %d", len(hints))
	}

	// Verify remaining hints
	if hints[0].Key != "key1" || hints[1].Key != "key3" {
		t.Error("Wrong hints remained after removal")
	}
}

func TestHintedHandoff_CleanupOldHints(t *testing.T) {
	tmpDir := t.TempDir()
	hh, err := NewHintedHandoff(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff: %v", err)
	}

	// Set a very short max age for testing
	hh.maxAge = 100 * time.Millisecond

	// Store hints
	hh.StoreHint("node2", "key1", []byte("value1"), time.Now().UnixNano(), 1)

	// Wait for hints to become old
	time.Sleep(150 * time.Millisecond)

	// Store a new hint
	hh.StoreHint("node2", "key2", []byte("value2"), time.Now().UnixNano(), 2)

	// Cleanup old hints
	removed := hh.CleanupOldHints()

	if removed != 1 {
		t.Errorf("Expected 1 hint removed, got %d", removed)
	}

	hints := hh.GetHints("node2")
	if len(hints) != 1 {
		t.Errorf("Expected 1 hint remaining, got %d", len(hints))
	}

	if hints[0].Key != "key2" {
		t.Error("Wrong hint remained after cleanup")
	}
}

func TestHintedHandoff_MaxHints(t *testing.T) {
	tmpDir := t.TempDir()
	hh, err := NewHintedHandoff(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff: %v", err)
	}

	// Set a low max for testing
	hh.maxHints = 5

	// Store up to max
	for i := 0; i < 5; i++ {
		err := hh.StoreHint("node2", "key", []byte("value"), time.Now().UnixNano(), int64(i))
		if err != nil {
			t.Fatalf("Failed to store hint %d: %v", i, err)
		}
	}

	// Try to store beyond max
	err = hh.StoreHint("node2", "key6", []byte("value6"), time.Now().UnixNano(), 6)
	if err == nil {
		t.Error("Expected error when exceeding max hints")
	}
}

func TestHintedHandoff_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first instance and store hints
	hh1, err := NewHintedHandoff(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff: %v", err)
	}

	hh1.StoreHint("node2", "key1", []byte("value1"), time.Now().UnixNano(), 1)
	hh1.StoreHint("node2", "key2", []byte("value2"), time.Now().UnixNano(), 2)

	// Give it time to persist
	time.Sleep(100 * time.Millisecond)

	// Create second instance (should load from disk)
	hh2, err := NewHintedHandoff(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create second hinted handoff: %v", err)
	}

	hints := hh2.GetHints("node2")
	if len(hints) != 2 {
		t.Errorf("Expected 2 hints loaded from disk, got %d", len(hints))
	}
}

func TestHintedHandoff_GetHintCount(t *testing.T) {
	tmpDir := t.TempDir()
	hh, err := NewHintedHandoff(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create hinted handoff: %v", err)
	}

	hh.StoreHint("node2", "key1", []byte("value1"), time.Now().UnixNano(), 1)
	hh.StoreHint("node3", "key2", []byte("value2"), time.Now().UnixNano(), 2)
	hh.StoreHint("node3", "key3", []byte("value3"), time.Now().UnixNano(), 3)

	totalCount := hh.GetHintCount()
	if totalCount != 3 {
		t.Errorf("Expected total count 3, got %d", totalCount)
	}

	node2Count := hh.GetHintCountForNode("node2")
	if node2Count != 1 {
		t.Errorf("Expected 1 hint for node2, got %d", node2Count)
	}

	node3Count := hh.GetHintCountForNode("node3")
	if node3Count != 2 {
		t.Errorf("Expected 2 hints for node3, got %d", node3Count)
	}
}
