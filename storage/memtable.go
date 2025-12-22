package storage

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	maxLevel    = 16  // Maximum level for skip list
	probability = 0.5 // Probability for level promotion
)

// MemTable is an in-memory sorted structure using Skip List
type MemTable struct {
	head      *skipNode
	maxLevel  int
	size      int64 // Size in bytes
	mu        sync.RWMutex
	tombstone []byte // Special marker for deletions
}

type skipNode struct {
	key     []byte
	value   []byte
	forward []*skipNode
}

// NewMemTable creates a new MemTable
func NewMemTable() *MemTable {
	return &MemTable{
		head:      &skipNode{forward: make([]*skipNode, maxLevel)},
		maxLevel:  1,
		tombstone: []byte("__TOMBSTONE__"),
	}
}

// Put inserts or updates a key-value pair
func (m *MemTable) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Calculate size impact
	keySize := int64(len(key))
	valueSize := int64(len(value))

	// Find the position and update path
	update := make([]*skipNode, maxLevel)
	current := m.head

	for i := m.maxLevel - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// Check if key already exists
	current = current.forward[0]
	if current != nil && bytes.Equal(current.key, key) {
		// Update existing value
		oldValueSize := int64(len(current.value))
		m.size = m.size - oldValueSize + valueSize
		current.value = value
		return
	}

	// Insert new node
	level := m.randomLevel()
	if level > m.maxLevel {
		for i := m.maxLevel; i < level; i++ {
			update[i] = m.head
		}
		m.maxLevel = level
	}

	newNode := &skipNode{
		key:     key,
		value:   value,
		forward: make([]*skipNode, level),
	}

	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	m.size += keySize + valueSize + 8 // 8 bytes overhead per entry
}

// Get retrieves a value by key
func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	current := m.head
	for i := m.maxLevel - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]
	if current != nil && bytes.Equal(current.key, key) {
		// Check for tombstone (deleted key)
		if bytes.Equal(current.value, m.tombstone) {
			return nil, false
		}
		return current.value, true
	}

	return nil, false
}

// Delete marks a key as deleted using tombstone
func (m *MemTable) Delete(key []byte) {
	m.Put(key, m.tombstone)
}

// Size returns the approximate size in bytes
func (m *MemTable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Iterator returns all key-value pairs in sorted order
func (m *MemTable) Iterator() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var entries []Entry
	current := m.head.forward[0]

	for current != nil {
		entries = append(entries, Entry{
			Key:   current.key,
			Value: current.value,
		})
		current = current.forward[0]
	}

	return entries
}

// randomLevel generates a random level for new node
func (m *MemTable) randomLevel() int {
	level := 1
	for level < maxLevel && rand.Float64() < probability {
		level++
	}
	return level
}

// Clear removes all entries (used after flushing to disk)
func (m *MemTable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.head = &skipNode{forward: make([]*skipNode, maxLevel)}
	m.maxLevel = 1
	m.size = 0
}
