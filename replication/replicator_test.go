package replication

import (
	"testing"
	"time"
)

func TestResolveConflict_LastWriteWins(t *testing.T) {
	now := time.Now().UnixNano()

	responses := []ReplicaResponse{
		{NodeID: "node1", Success: true, Value: []byte("old"), Timestamp: now - 1000, Version: 1},
		{NodeID: "node2", Success: true, Value: []byte("newer"), Timestamp: now, Version: 2},
		{NodeID: "node3", Success: true, Value: []byte("oldest"), Timestamp: now - 2000, Version: 0},
	}

	latest := ResolveConflict(responses)

	if latest == nil {
		t.Fatal("ResolveConflict returned nil")
	}

	if latest.NodeID != "node2" {
		t.Errorf("Expected node2 (newest), got %s", latest.NodeID)
	}

	if string(latest.Value) != "newer" {
		t.Errorf("Expected 'newer', got '%s'", latest.Value)
	}
}

func TestResolveConflict_TiebreakerByVersion(t *testing.T) {
	now := time.Now().UnixNano()

	// Same timestamp, different versions
	responses := []ReplicaResponse{
		{NodeID: "node1", Success: true, Value: []byte("v1"), Timestamp: now, Version: 1},
		{NodeID: "node2", Success: true, Value: []byte("v2"), Timestamp: now, Version: 2},
	}

	latest := ResolveConflict(responses)

	if latest.Version != 2 {
		t.Errorf("Expected version 2, got %d", latest.Version)
	}

	if latest.NodeID != "node2" {
		t.Errorf("Expected node2, got %s", latest.NodeID)
	}
}

func TestQuorumReached(t *testing.T) {
	testCases := []struct {
		name      string
		responses []ReplicaResponse
		quorum    int
		expected  bool
	}{
		{
			name: "Quorum reached (2/3)",
			responses: []ReplicaResponse{
				{NodeID: "node1", Success: true},
				{NodeID: "node2", Success: true},
				{NodeID: "node3", Success: false},
			},
			quorum:   2,
			expected: true,
		},
		{
			name: "Quorum not reached (1/3)",
			responses: []ReplicaResponse{
				{NodeID: "node1", Success: true},
				{NodeID: "node2", Success: false},
				{NodeID: "node3", Success: false},
			},
			quorum:   2,
			expected: false,
		},
		{
			name: "All succeed (3/3)",
			responses: []ReplicaResponse{
				{NodeID: "node1", Success: true},
				{NodeID: "node2", Success: true},
				{NodeID: "node3", Success: true},
			},
			quorum:   2,
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := QuorumReached(tc.responses, tc.quorum)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestNeedsReadRepair(t *testing.T) {
	now := time.Now().UnixNano()

	testCases := []struct {
		name      string
		responses []ReplicaResponse
		expected  bool
	}{
		{
			name: "Consistent replicas",
			responses: []ReplicaResponse{
				{NodeID: "node1", Timestamp: now, Version: 1},
				{NodeID: "node2", Timestamp: now, Version: 1},
				{NodeID: "node3", Timestamp: now, Version: 1},
			},
			expected: false,
		},
		{
			name: "Inconsistent timestamps",
			responses: []ReplicaResponse{
				{NodeID: "node1", Timestamp: now, Version: 1},
				{NodeID: "node2", Timestamp: now - 1000, Version: 1},
			},
			expected: true,
		},
		{
			name: "Inconsistent versions",
			responses: []ReplicaResponse{
				{NodeID: "node1", Timestamp: now, Version: 2},
				{NodeID: "node2", Timestamp: now, Version: 1},
			},
			expected: true,
		},
		{
			name:      "Single response",
			responses: []ReplicaResponse{{NodeID: "node1", Timestamp: now, Version: 1}},
			expected:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NeedsReadRepair(tc.responses)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestGetOutdatedReplicas(t *testing.T) {
	now := time.Now().UnixNano()

	responses := []ReplicaResponse{
		{NodeID: "node1", Timestamp: now, Version: 2},        // Latest
		{NodeID: "node2", Timestamp: now - 1000, Version: 1}, // Outdated
		{NodeID: "node3", Timestamp: now - 2000, Version: 0}, // Outdated
	}

	latest := &responses[0]
	outdated := GetOutdatedReplicas(responses, latest)

	if len(outdated) != 2 {
		t.Errorf("Expected 2 outdated replicas, got %d", len(outdated))
	}

	// Check that node2 and node3 are in the outdated list
	outdatedMap := make(map[string]bool)
	for _, nodeID := range outdated {
		outdatedMap[nodeID] = true
	}

	if !outdatedMap["node2"] || !outdatedMap["node3"] {
		t.Error("Expected node2 and node3 to be outdated")
	}
}

func TestGenerateTimestampAndVersion(t *testing.T) {
	ts1 := GenerateTimestamp()
	time.Sleep(1 * time.Millisecond)
	ts2 := GenerateTimestamp()

	if ts2 <= ts1 {
		t.Error("Second timestamp should be greater than first")
	}

	version := GenerateVersion(ts1)
	if version != ts1 {
		t.Errorf("Version should equal timestamp, got %d != %d", version, ts1)
	}
}
