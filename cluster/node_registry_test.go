package cluster

import (
	"testing"
)

func TestNodeRegistry_RegisterNode(t *testing.T) {
	registry := NewNodeRegistry(256)

	err := registry.RegisterNode("node1", "localhost:50051")
	if err != nil {
		t.Fatalf("Failed to register node: %v", err)
	}

	if registry.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", registry.GetNodeCount())
	}

	// Try to register same node again
	err = registry.RegisterNode("node1", "localhost:50051")
	if err == nil {
		t.Error("Expected error when registering duplicate node")
	}
}

func TestNodeRegistry_UnregisterNode(t *testing.T) {
	registry := NewNodeRegistry(256)

	registry.RegisterNode("node1", "localhost:50051")
	registry.RegisterNode("node2", "localhost:50052")

	err := registry.UnregisterNode("node1")
	if err != nil {
		t.Fatalf("Failed to unregister node: %v", err)
	}

	if registry.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node after unregister, got %d", registry.GetNodeCount())
	}

	// Try to unregister non-existent node
	err = registry.UnregisterNode("node3")
	if err == nil {
		t.Error("Expected error when unregistering non-existent node")
	}
}

func TestNodeRegistry_GetNode(t *testing.T) {
	registry := NewNodeRegistry(256)

	registry.RegisterNode("node1", "localhost:50051")

	node, err := registry.GetNode("node1")
	if err != nil {
		t.Fatalf("Failed to get node: %v", err)
	}

	if node.ID != "node1" {
		t.Errorf("Expected node1, got %s", node.ID)
	}

	if node.Address != "localhost:50051" {
		t.Errorf("Expected localhost:50051, got %s", node.Address)
	}
}

func TestNodeRegistry_GetNodeForKey(t *testing.T) {
	registry := NewNodeRegistry(256)

	registry.RegisterNode("node1", "localhost:50051")
	registry.RegisterNode("node2", "localhost:50052")
	registry.RegisterNode("node3", "localhost:50053")

	key := "test_key"
	node, err := registry.GetNodeForKey(key)
	if err != nil {
		t.Fatalf("Failed to get node for key: %v", err)
	}

	t.Logf("Key '%s' maps to node '%s' at %s", key, node.ID, node.Address)

	// Same key should always map to same node
	node2, err := registry.GetNodeForKey(key)
	if err != nil {
		t.Fatalf("Failed to get node for key (2nd time): %v", err)
	}

	if node.ID != node2.ID {
		t.Errorf("Key mapped to different nodes: %s vs %s", node.ID, node2.ID)
	}
}

func TestNodeRegistry_GetAllNodes(t *testing.T) {
	registry := NewNodeRegistry(256)

	registry.RegisterNode("node1", "localhost:50051")
	registry.RegisterNode("node2", "localhost:50052")
	registry.RegisterNode("node3", "localhost:50053")

	nodes := registry.GetAllNodes()

	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// Check all nodes are present
	nodeIDs := make(map[string]bool)
	for _, node := range nodes {
		nodeIDs[node.ID] = true
	}

	if !nodeIDs["node1"] || !nodeIDs["node2"] || !nodeIDs["node3"] {
		t.Error("Not all nodes were returned")
	}
}

func TestNodeRegistry_GetNodeAddresses(t *testing.T) {
	registry := NewNodeRegistry(256)

	registry.RegisterNode("node1", "localhost:50051")
	registry.RegisterNode("node2", "localhost:50052")

	addresses := registry.GetNodeAddresses()

	if len(addresses) != 2 {
		t.Errorf("Expected 2 addresses, got %d", len(addresses))
	}

	if addresses["node1"] != "localhost:50051" {
		t.Errorf("Wrong address for node1: %s", addresses["node1"])
	}

	if addresses["node2"] != "localhost:50052" {
		t.Errorf("Wrong address for node2: %s", addresses["node2"])
	}
}

func TestNodeRegistry_KeyDistribution(t *testing.T) {
	registry := NewNodeRegistry(256)

	registry.RegisterNode("node1", "localhost:50051")
	registry.RegisterNode("node2", "localhost:50052")
	registry.RegisterNode("node3", "localhost:50053")

	distribution := registry.GetKeyDistribution(10000)

	t.Logf("Key distribution across nodes (10000 keys):")
	for nodeID, count := range distribution {
		percentage := float64(count) / 100.0
		t.Logf("  %s: %d keys (%.2f%%)", nodeID, count, percentage)
	}

	// Each node should get roughly 33% of keys
	for _, count := range distribution {
		if count < 3000 || count > 3700 {
			t.Errorf("Unbalanced distribution: %d keys (expected ~3333)", count)
		}
	}
}
