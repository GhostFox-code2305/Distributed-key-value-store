package server

import (
	"context"
	"testing"

	"kvstore/proto"
	"kvstore/storage"
)

func TestGRPCServer_PutAndGet(t *testing.T) {
	// Create test store
	tmpDir := t.TempDir()
	store, err := storage.NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create gRPC server
	server := NewGRPCServer(store)
	ctx := context.Background()

	// Test Put
	putResp, err := server.Put(ctx, &proto.PutRequest{
		Key:   "test_key",
		Value: []byte("test_value"),
	})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if !putResp.Success {
		t.Errorf("Put unsuccessful: %s", putResp.Error)
	}

	// Test Get
	getResp, err := server.Get(ctx, &proto.GetRequest{
		Key: "test_key",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !getResp.Found {
		t.Error("Key not found")
	}
	if string(getResp.Value) != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", getResp.Value)
	}
}

func TestGRPCServer_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server := NewGRPCServer(store)
	ctx := context.Background()

	// Put a value
	server.Put(ctx, &proto.PutRequest{
		Key:   "delete_me",
		Value: []byte("temp"),
	})

	// Delete it
	delResp, err := server.Delete(ctx, &proto.DeleteRequest{
		Key: "delete_me",
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !delResp.Success {
		t.Errorf("Delete unsuccessful: %s", delResp.Error)
	}

	// Try to get it (should not be found)
	getResp, err := server.Get(ctx, &proto.GetRequest{
		Key: "delete_me",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if getResp.Found {
		t.Error("Deleted key should not be found")
	}
}

func TestGRPCServer_Stats(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server := NewGRPCServer(store)
	ctx := context.Background()

	// Put some data
	for i := 0; i < 10; i++ {
		server.Put(ctx, &proto.PutRequest{
			Key:   string(rune('a' + i)),
			Value: []byte("value"),
		})
	}

	// Get stats
	statsResp, err := server.Stats(ctx, &proto.StatsRequest{})
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if statsResp.MemtableSize == 0 {
		t.Error("MemTable size should not be 0")
	}

	t.Logf("Stats: MemTable=%d bytes, SSTables=%d",
		statsResp.MemtableSize, statsResp.NumSstables)
}

func TestGRPCServer_Compact(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewLSMStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server := NewGRPCServer(store)
	ctx := context.Background()

	// Compact (should succeed even with no data)
	compactResp, err := server.Compact(ctx, &proto.CompactRequest{})
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	if !compactResp.Success {
		t.Errorf("Compact unsuccessful: %s", compactResp.Error)
	}
}
