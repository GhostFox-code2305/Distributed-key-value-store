package replication

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Hint represents a write that should be replayed to a node when it comes back
type Hint struct {
	TargetNode string    `json:"target_node"` // Node that should receive this write
	Key        string    `json:"key"`
	Value      []byte    `json:"value"`
	Timestamp  int64     `json:"timestamp"`
	Version    int64     `json:"version"`
	CreatedAt  time.Time `json:"created_at"`
}

// HintedHandoff manages hints for temporarily unavailable nodes
type HintedHandoff struct {
	hints    map[string][]Hint // targetNode -> list of hints
	hintsDir string            // Directory to persist hints
	mu       sync.RWMutex
	maxHints int           // Maximum hints per node
	maxAge   time.Duration // Maximum age of hints
}

// NewHintedHandoff creates a new hinted handoff manager
func NewHintedHandoff(hintsDir string) (*HintedHandoff, error) {
	if err := os.MkdirAll(hintsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create hints directory: %w", err)
	}

	hh := &HintedHandoff{
		hints:    make(map[string][]Hint),
		hintsDir: hintsDir,
		maxHints: 10000,          // Max 10k hints per node
		maxAge:   24 * time.Hour, // Keep hints for 24 hours max
	}

	// Load existing hints from disk
	if err := hh.loadHints(); err != nil {
		log.Printf("⚠️  Warning: Failed to load hints: %v", err)
	}

	return hh, nil
}

// StoreHint stores a hint for a temporarily unavailable node
func (hh *HintedHandoff) StoreHint(targetNode, key string, value []byte, timestamp, version int64) error {
	hh.mu.Lock()
	defer hh.mu.Unlock()

	hint := Hint{
		TargetNode: targetNode,
		Key:        key,
		Value:      value,
		Timestamp:  timestamp,
		Version:    version,
		CreatedAt:  time.Now(),
	}

	// Check if we've reached max hints for this node
	if len(hh.hints[targetNode]) >= hh.maxHints {
		return fmt.Errorf("max hints reached for node %s", targetNode)
	}

	hh.hints[targetNode] = append(hh.hints[targetNode], hint)

	// Persist to disk synchronously (fixed for Windows compatibility)
	if err := hh.persistHintsLocked(targetNode); err != nil {
		log.Printf("⚠️  Failed to persist hints for %s: %v", targetNode, err)
	}

	log.Printf("💾 Stored hint for node %s: key=%s", targetNode, key)
	return nil
}

// GetHints returns all hints for a specific node
func (hh *HintedHandoff) GetHints(targetNode string) []Hint {
	hh.mu.RLock()
	defer hh.mu.RUnlock()

	hints, exists := hh.hints[targetNode]
	if !exists {
		return []Hint{}
	}

	// Return a copy to avoid concurrent modification
	result := make([]Hint, len(hints))
	copy(result, hints)
	return result
}

// ClearHints removes all hints for a node (after successful replay)
func (hh *HintedHandoff) ClearHints(targetNode string) error {
	hh.mu.Lock()
	defer hh.mu.Unlock()

	delete(hh.hints, targetNode)

	// Remove hints file from disk
	hintsFile := filepath.Join(hh.hintsDir, fmt.Sprintf("hints_%s.json", targetNode))
	if err := os.Remove(hintsFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove hints file: %w", err)
	}

	log.Printf("🧹 Cleared hints for node %s", targetNode)
	return nil
}

// RemoveHint removes a specific hint (after successful replay)
func (hh *HintedHandoff) RemoveHint(targetNode string, hintIndex int) {
	hh.mu.Lock()
	defer hh.mu.Unlock()

	hints, exists := hh.hints[targetNode]
	if !exists || hintIndex >= len(hints) {
		return
	}

	// Remove hint at index
	hh.hints[targetNode] = append(hints[:hintIndex], hints[hintIndex+1:]...)

	// If no more hints, delete the entry
	if len(hh.hints[targetNode]) == 0 {
		delete(hh.hints, targetNode)
	}
}

// CleanupOldHints removes hints older than maxAge
func (hh *HintedHandoff) CleanupOldHints() int {
	hh.mu.Lock()
	defer hh.mu.Unlock()

	removed := 0
	cutoff := time.Now().Add(-hh.maxAge)

	for targetNode, hints := range hh.hints {
		newHints := make([]Hint, 0)
		for _, hint := range hints {
			if hint.CreatedAt.After(cutoff) {
				newHints = append(newHints, hint)
			} else {
				removed++
			}
		}

		if len(newHints) == 0 {
			delete(hh.hints, targetNode)
		} else {
			hh.hints[targetNode] = newHints
		}
	}

	if removed > 0 {
		log.Printf("🧹 Cleaned up %d old hints", removed)
	}

	return removed
}

// GetHintCount returns the total number of hints
func (hh *HintedHandoff) GetHintCount() int {
	hh.mu.RLock()
	defer hh.mu.RUnlock()

	count := 0
	for _, hints := range hh.hints {
		count += len(hints)
	}
	return count
}

// GetHintCountForNode returns the number of hints for a specific node
func (hh *HintedHandoff) GetHintCountForNode(targetNode string) int {
	hh.mu.RLock()
	defer hh.mu.RUnlock()

	return len(hh.hints[targetNode])
}

// persistHintsLocked saves hints for a node to disk (must be called with lock held)
func (hh *HintedHandoff) persistHintsLocked(targetNode string) error {
	hints, exists := hh.hints[targetNode]
	if !exists || len(hints) == 0 {
		return nil
	}

	hintsFile := filepath.Join(hh.hintsDir, fmt.Sprintf("hints_%s.json", targetNode))

	data, err := json.MarshalIndent(hints, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal hints: %w", err)
	}

	if err := os.WriteFile(hintsFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write hints file: %w", err)
	}

	return nil
}

// loadHints loads hints from disk
func (hh *HintedHandoff) loadHints() error {
	files, err := filepath.Glob(filepath.Join(hh.hintsDir, "hints_*.json"))
	if err != nil {
		return err
	}

	totalHints := 0
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			log.Printf("⚠️  Failed to read hints file %s: %v", file, err)
			continue
		}

		var hints []Hint
		if err := json.Unmarshal(data, &hints); err != nil {
			log.Printf("⚠️  Failed to unmarshal hints from %s: %v", file, err)
			continue
		}

		if len(hints) > 0 {
			targetNode := hints[0].TargetNode
			hh.hints[targetNode] = hints
			totalHints += len(hints)
		}
	}

	if totalHints > 0 {
		log.Printf("📂 Loaded %d hints from disk", totalHints)
	}

	return nil
}

// StartCleanupTask starts a background task to cleanup old hints
func (hh *HintedHandoff) StartCleanupTask(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			hh.CleanupOldHints()
		}
	}()
}
