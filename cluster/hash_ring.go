package cluster

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const (
	// DefaultVirtualNodes is the number of virtual nodes per physical node
	DefaultVirtualNodes = 256
)

// HashRing implements consistent hashing with virtual nodes
type HashRing struct {
	virtualNodes int
	ring         map[uint32]string // hash -> node ID
	sortedHashes []uint32          // sorted list of hashes
	nodes        map[string]bool   // set of physical nodes
	mu           sync.RWMutex
}

// NewHashRing creates a new hash ring
func NewHashRing(virtualNodes int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	return &HashRing{
		virtualNodes: virtualNodes,
		ring:         make(map[uint32]string),
		sortedHashes: make([]uint32, 0),
		nodes:        make(map[string]bool),
	}
}

// AddNode adds a physical node to the ring
func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.nodes[nodeID] {
		return // Node already exists
	}

	hr.nodes[nodeID] = true

	// Add virtual nodes
	for i := 0; i < hr.virtualNodes; i++ {
		virtualKey := fmt.Sprintf("%s-vnode-%d", nodeID, i)
		hash := hr.hashKey(virtualKey)
		hr.ring[hash] = nodeID
		hr.sortedHashes = append(hr.sortedHashes, hash)
	}

	// Sort the hashes
	sort.Slice(hr.sortedHashes, func(i, j int) bool {
		return hr.sortedHashes[i] < hr.sortedHashes[j]
	})
}

// RemoveNode removes a physical node from the ring
func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if !hr.nodes[nodeID] {
		return // Node doesn't exist
	}

	delete(hr.nodes, nodeID)

	// Remove virtual nodes
	newHashes := make([]uint32, 0)
	for _, hash := range hr.sortedHashes {
		if hr.ring[hash] != nodeID {
			newHashes = append(newHashes, hash)
		} else {
			delete(hr.ring, hash)
		}
	}

	hr.sortedHashes = newHashes
}

// GetNode returns the node responsible for a given key
func (hr *HashRing) GetNode(key string) (string, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.sortedHashes) == 0 {
		return "", fmt.Errorf("no nodes in hash ring")
	}

	hash := hr.hashKey(key)

	// Binary search to find the first node >= hash
	idx := sort.Search(len(hr.sortedHashes), func(i int) bool {
		return hr.sortedHashes[i] >= hash
	})

	// Wrap around if we're past the end
	if idx >= len(hr.sortedHashes) {
		idx = 0
	}

	nodeHash := hr.sortedHashes[idx]
	return hr.ring[nodeHash], nil
}

// GetNodes returns all physical nodes in the ring
func (hr *HashRing) GetNodes() []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	nodes := make([]string, 0, len(hr.nodes))
	for node := range hr.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetNodeCount returns the number of physical nodes
func (hr *HashRing) GetNodeCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.nodes)
}

// hashKey hashes a key to a uint32 using MD5
func (hr *HashRing) hashKey(key string) uint32 {
	hash := md5.Sum([]byte(key))
	// Take first 4 bytes and convert to uint32
	return binary.BigEndian.Uint32(hash[:4])
}

// GetDistribution returns how many virtual nodes each physical node has
func (hr *HashRing) GetDistribution() map[string]int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	distribution := make(map[string]int)
	for _, nodeID := range hr.ring {
		distribution[nodeID]++
	}
	return distribution
}

// GetKeyDistribution simulates distributing N keys and returns count per node
func (hr *HashRing) GetKeyDistribution(numKeys int) map[string]int {
	distribution := make(map[string]int)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		node, err := hr.GetNode(key)
		if err == nil {
			distribution[node]++
		}
	}

	return distribution
}

// GetPreferenceList returns N nodes responsible for a key (primary + replicas)
// Returns nodes in clockwise order starting from the primary node
func (hr *HashRing) GetPreferenceList(key string, n int) ([]string, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.sortedHashes) == 0 {
		return nil, fmt.Errorf("no nodes in hash ring")
	}

	if n > len(hr.nodes) {
		n = len(hr.nodes) // Can't have more replicas than nodes
	}

	hash := hr.hashKey(key)

	// Binary search to find the first node >= hash
	idx := sort.Search(len(hr.sortedHashes), func(i int) bool {
		return hr.sortedHashes[i] >= hash
	})

	// Wrap around if we're past the end
	if idx >= len(hr.sortedHashes) {
		idx = 0
	}

	// Collect unique physical nodes in clockwise order
	result := make([]string, 0, n)
	seen := make(map[string]bool)

	for len(result) < n && len(seen) < len(hr.nodes) {
		nodeHash := hr.sortedHashes[idx]
		nodeID := hr.ring[nodeHash]

		if !seen[nodeID] {
			result = append(result, nodeID)
			seen[nodeID] = true
		}

		idx = (idx + 1) % len(hr.sortedHashes)
	}

	return result, nil
}
