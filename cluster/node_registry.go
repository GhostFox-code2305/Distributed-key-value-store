package cluster

import (
	"fmt"
	"sync"
	"time"
)

// Node represents a node in the cluster
type Node struct {
	ID      string    // Unique node identifier
	Address string    // Network address (host:port)
	AddedAt time.Time // When node was added
}

// NodeRegistry tracks all nodes in the cluster
type NodeRegistry struct {
	nodes    map[string]*Node // nodeID -> Node
	hashRing *HashRing
	mu       sync.RWMutex
}

// NewNodeRegistry creates a new node registry
func NewNodeRegistry(virtualNodes int) *NodeRegistry {
	return &NodeRegistry{
		nodes:    make(map[string]*Node),
		hashRing: NewHashRing(virtualNodes),
	}
}

// RegisterNode adds a node to the registry
func (nr *NodeRegistry) RegisterNode(nodeID, address string) error {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	if _, exists := nr.nodes[nodeID]; exists {
		return fmt.Errorf("node %s already registered", nodeID)
	}

	node := &Node{
		ID:      nodeID,
		Address: address,
		AddedAt: time.Now(),
	}

	nr.nodes[nodeID] = node
	nr.hashRing.AddNode(nodeID)

	return nil
}

// UnregisterNode removes a node from the registry
func (nr *NodeRegistry) UnregisterNode(nodeID string) error {
	nr.mu.Lock()
	defer nr.mu.Unlock()

	if _, exists := nr.nodes[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	delete(nr.nodes, nodeID)
	nr.hashRing.RemoveNode(nodeID)

	return nil
}

// GetNode returns information about a specific node
func (nr *NodeRegistry) GetNode(nodeID string) (*Node, error) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	node, exists := nr.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	return node, nil
}

// GetNodeForKey returns the node responsible for a key
func (nr *NodeRegistry) GetNodeForKey(key string) (*Node, error) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	nodeID, err := nr.hashRing.GetNode(key)
	if err != nil {
		return nil, err
	}

	node, exists := nr.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node %s found in hash ring but not in registry", nodeID)
	}

	return node, nil
}

// GetAllNodes returns all registered nodes
func (nr *NodeRegistry) GetAllNodes() []*Node {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	nodes := make([]*Node, 0, len(nr.nodes))
	for _, node := range nr.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetNodeCount returns the number of registered nodes
func (nr *NodeRegistry) GetNodeCount() int {
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	return len(nr.nodes)
}

// GetNodeAddresses returns a map of nodeID -> address
func (nr *NodeRegistry) GetNodeAddresses() map[string]string {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	addresses := make(map[string]string)
	for id, node := range nr.nodes {
		addresses[id] = node.Address
	}
	return addresses
}

// GetKeyDistribution returns statistics about key distribution
func (nr *NodeRegistry) GetKeyDistribution(numKeys int) map[string]int {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	return nr.hashRing.GetKeyDistribution(numKeys)
}
