package cluster

import (
	"fmt"
	"math"
	"testing"
)

func TestHashRing_AddNode(t *testing.T) {
	ring := NewHashRing(10) // Small number for testing

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	if ring.GetNodeCount() != 3 {
		t.Errorf("Expected 3 nodes, got %d", ring.GetNodeCount())
	}

	// Check virtual nodes were added
	if len(ring.sortedHashes) != 30 {
		t.Errorf("Expected 30 virtual nodes (3 nodes * 10), got %d", len(ring.sortedHashes))
	}
}

func TestHashRing_RemoveNode(t *testing.T) {
	ring := NewHashRing(10)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	ring.RemoveNode("node2")

	if ring.GetNodeCount() != 2 {
		t.Errorf("Expected 2 nodes after removal, got %d", ring.GetNodeCount())
	}

	// Check virtual nodes were removed
	if len(ring.sortedHashes) != 20 {
		t.Errorf("Expected 20 virtual nodes (2 nodes * 10), got %d", len(ring.sortedHashes))
	}
}

func TestHashRing_GetNode(t *testing.T) {
	ring := NewHashRing(256)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Test that same key always goes to same node
	key := "test_key"
	node1, err := ring.GetNode(key)
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}

	node2, err := ring.GetNode(key)
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}

	if node1 != node2 {
		t.Errorf("Same key returned different nodes: %s vs %s", node1, node2)
	}

	t.Logf("Key '%s' maps to node '%s'", key, node1)
}

func TestHashRing_Distribution(t *testing.T) {
	ring := NewHashRing(256)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Simulate 10000 keys
	distribution := ring.GetKeyDistribution(10000)

	t.Logf("Key distribution across nodes:")
	for node, count := range distribution {
		percentage := float64(count) / 100.0
		t.Logf("  %s: %d keys (%.2f%%)", node, count, percentage)
	}

	// Check that distribution is reasonably balanced (within 20% of ideal)
	idealPerNode := 10000 / 3 // ~3333 per node
	tolerance := float64(idealPerNode) * 0.2

	for node, count := range distribution {
		diff := math.Abs(float64(count) - float64(idealPerNode))
		if diff > tolerance {
			t.Errorf("Node %s has poor distribution: %d keys (expected ~%d ± %.0f)",
				node, count, idealPerNode, tolerance)
		}
	}
}

func TestHashRing_ConsistentAfterNodeRemoval(t *testing.T) {
	ring := NewHashRing(256)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Get node assignments before removal
	keysBefore := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		node, _ := ring.GetNode(key)
		keysBefore[key] = node
	}

	// Remove a node
	ring.RemoveNode("node2")

	// Check how many keys moved
	moved := 0
	for key, oldNode := range keysBefore {
		newNode, _ := ring.GetNode(key)
		if oldNode == "node2" {
			// Keys on removed node should move
			if newNode == "node2" {
				t.Error("Key still assigned to removed node")
			}
		} else {
			// Keys on other nodes should mostly stay
			if oldNode != newNode {
				moved++
			}
		}
	}

	// With consistent hashing, most keys should stay on their original nodes
	movedPercentage := float64(moved) / float64(len(keysBefore)) * 100
	t.Logf("After removing node2: %.2f%% of keys moved to different nodes", movedPercentage)

	// Ideally, only ~33% should move (keys from node2)
	// We allow up to 50% due to some redistribution
	if movedPercentage > 50 {
		t.Errorf("Too many keys moved: %.2f%% (expected < 50%%)", movedPercentage)
	}
}

func TestHashRing_EmptyRing(t *testing.T) {
	ring := NewHashRing(256)

	_, err := ring.GetNode("test_key")
	if err == nil {
		t.Error("Expected error when getting node from empty ring")
	}
}

func TestHashRing_VirtualNodes(t *testing.T) {
	// Test with different numbers of virtual nodes
	testCases := []int{10, 100, 256, 500}

	for _, vnodes := range testCases {
		t.Run(fmt.Sprintf("vnodes=%d", vnodes), func(t *testing.T) {
			ring := NewHashRing(vnodes)
			ring.AddNode("node1")
			ring.AddNode("node2")

			distribution := ring.GetKeyDistribution(10000)

			// Calculate standard deviation
			mean := 5000.0
			var variance float64
			for _, count := range distribution {
				diff := float64(count) - mean
				variance += diff * diff
			}
			stddev := math.Sqrt(variance / 2)

			t.Logf("Virtual nodes: %d, Stddev: %.2f", vnodes, stddev)

			// More virtual nodes = better distribution (lower stddev)
			// This is just informational, not a strict test
		})
	}
}

func BenchmarkHashRing_GetNode(b *testing.B) {
	ring := NewHashRing(256)
	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%1000)
		ring.GetNode(key)
	}
}

func BenchmarkHashRing_AddNode(b *testing.B) {
	ring := NewHashRing(256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := fmt.Sprintf("node_%d", i)
		ring.AddNode(nodeID)
	}
}

func TestHashRing_GetPreferenceList(t *testing.T) {
	ring := NewHashRing(256)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	// Test getting preference list for a key
	key := "test_key"
	preferenceList, err := ring.GetPreferenceList(key, 3)
	if err != nil {
		t.Fatalf("GetPreferenceList failed: %v", err)
	}

	if len(preferenceList) != 3 {
		t.Errorf("Expected 3 nodes in preference list, got %d", len(preferenceList))
	}

	// Check all nodes are unique
	seen := make(map[string]bool)
	for _, nodeID := range preferenceList {
		if seen[nodeID] {
			t.Errorf("Duplicate node in preference list: %s", nodeID)
		}
		seen[nodeID] = true
	}

	t.Logf("Preference list for key '%s': %v", key, preferenceList)

	// Test with N > number of nodes
	preferenceList2, err := ring.GetPreferenceList(key, 10)
	if err != nil {
		t.Fatalf("GetPreferenceList failed: %v", err)
	}

	// Should return all 3 nodes (not 10)
	if len(preferenceList2) != 3 {
		t.Errorf("Expected 3 nodes (max available), got %d", len(preferenceList2))
	}
}

func TestHashRing_GetPreferenceListConsistency(t *testing.T) {
	ring := NewHashRing(256)

	ring.AddNode("node1")
	ring.AddNode("node2")
	ring.AddNode("node3")

	key := "consistent_key"

	// Get preference list multiple times
	list1, _ := ring.GetPreferenceList(key, 3)
	list2, _ := ring.GetPreferenceList(key, 3)
	list3, _ := ring.GetPreferenceList(key, 3)

	// Should be identical every time
	for i := 0; i < len(list1); i++ {
		if list1[i] != list2[i] || list1[i] != list3[i] {
			t.Error("Preference list is not consistent across calls")
		}
	}

	t.Logf("Consistent preference list: %v", list1)
}
