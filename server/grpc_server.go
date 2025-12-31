package server

import (
	"context"
	"log"

	"kvstore/proto"
	"kvstore/storage"
)

// GRPCServer implements the KVStore gRPC service
type GRPCServer struct {
	proto.UnimplementedKVStoreServer
	store *storage.LSMStore
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(store *storage.LSMStore) *GRPCServer {
	return &GRPCServer{
		store: store,
	}
}

// Put stores a key-value pair
func (s *GRPCServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	log.Printf("📝 PUT: key=%s, value_size=%d bytes", req.Key, len(req.Value))

	err := s.store.Put(req.Key, req.Value)
	if err != nil {
		log.Printf("❌ PUT failed: %v", err)
		return &proto.PutResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &proto.PutResponse{
		Success: true,
	}, nil
}

// Get retrieves a value by key
func (s *GRPCServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	log.Printf("🔍 GET: key=%s", req.Key)

	value, err := s.store.Get(req.Key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			log.Printf("⚠️  Key not found: %s", req.Key)
			return &proto.GetResponse{
				Found: false,
			}, nil
		}
		log.Printf("❌ GET failed: %v", err)
		return &proto.GetResponse{
			Found: false,
			Error: err.Error(),
		}, nil
	}

	log.Printf("✅ GET success: key=%s, value_size=%d bytes", req.Key, len(value))
	return &proto.GetResponse{
		Value: value,
		Found: true,
	}, nil
}

// Delete removes a key-value pair
func (s *GRPCServer) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	log.Printf("🗑️  DELETE: key=%s", req.Key)

	err := s.store.Delete(req.Key)
	if err != nil {
		log.Printf("❌ DELETE failed: %v", err)
		return &proto.DeleteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &proto.DeleteResponse{
		Success: true,
	}, nil
}

// Stats returns storage statistics
func (s *GRPCServer) Stats(ctx context.Context, req *proto.StatsRequest) (*proto.StatsResponse, error) {
	log.Printf("📊 STATS requested")

	stats := s.store.Stats()

	response := &proto.StatsResponse{
		MemtableSize:      stats["memtable_size"].(int64),
		NumSstables:       int32(stats["num_sstables"].(int)),
		BloomFilterHits:   stats["bloom_filter_hits"].(int64),
		BloomFilterMisses: stats["bloom_filter_misses"].(int64),
	}

	// Add compaction stats if available
	if val, ok := stats["compaction_total_compactions"]; ok {
		response.CompactionTotalCompactions = val.(int64)
	}
	if val, ok := stats["compaction_total_keys_removed"]; ok {
		response.CompactionTotalKeysRemoved = val.(int64)
	}
	if val, ok := stats["compaction_total_bytes_reclaimed"]; ok {
		response.CompactionTotalBytesReclaimed = val.(int64)
	}
	if val, ok := stats["compaction_last_compaction"]; ok {
		response.CompactionLastCompaction = val.(string)
	}

	return response, nil
}

// Compact triggers manual compaction
func (s *GRPCServer) Compact(ctx context.Context, req *proto.CompactRequest) (*proto.CompactResponse, error) {
	log.Printf("🔄 COMPACT requested")

	err := s.store.CompactionManager().ForceCompact()
	if err != nil {
		log.Printf("❌ COMPACT failed: %v", err)
		return &proto.CompactResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	log.Printf("✅ COMPACT completed")
	return &proto.CompactResponse{
		Success: true,
	}, nil
}

// Close gracefully shuts down the server
func (s *GRPCServer) Close() error {
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}
