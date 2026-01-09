// raft/util.go
package raft

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
)

// min returns the minimum of two uint64 values
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two uint64 values
func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// randomInt returns a random integer in [min, max)
func randomInt(min, max int) int {
	if min >= max {
		return min
	}

	var n uint32
	binary.Read(rand.Reader, binary.BigEndian, &n)
	return min + int(n)%(max-min)
}

// Command represents a serializable command
type Command struct {
	Type  string `json:"type"` // "PUT" or "DELETE"
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

// FormatTerm formats a term for logging
func FormatTerm(term uint64) string {
	return fmt.Sprintf("T%d", term)
}

// FormatIndex formats an index for logging
func FormatIndex(index uint64) string {
	return fmt.Sprintf("I%d", index)
}

// FormatLogEntry formats a log entry for logging
func FormatLogEntry(entry *LogEntry) string {
	return fmt.Sprintf("%s:%s", FormatTerm(entry.Term), FormatIndex(entry.Index))
}
