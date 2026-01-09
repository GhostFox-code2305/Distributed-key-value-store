package replication

import (
	"fmt"
	"time"
)

const (
	// ReplicationFactor (N) - number of replicas for each key
	ReplicationFactor = 3

	// WriteQuorum (W) - number of successful writes required
	WriteQuorum = 2

	// ReadQuorum (R) - number of replicas to read for consistency
	// W + R > N ensures strong consistency
	ReadQuorum = 2
)

// ReplicaResponse represents a response from a replica
type ReplicaResponse struct {
	NodeID    string
	Success   bool
	Value     []byte
	Version   int64
	Timestamp int64
	Error     error
}

// GetPreferenceList returns N nodes for a key (primary + replicas)
// Uses the hash ring to find the primary node, then gets next N-1 nodes clockwise
func GetPreferenceList(hashRing interface{}, key string, n int) ([]string, error) {
	// This will be implemented using the hash ring
	// For now, this is a placeholder that will be used by cluster_client
	return nil, fmt.Errorf("not implemented - will be used in cluster_client")
}

// ResolveConflict resolves conflicts between multiple versions using Last-Write-Wins
func ResolveConflict(responses []ReplicaResponse) *ReplicaResponse {
	if len(responses) == 0 {
		return nil
	}

	// Last-Write-Wins: choose the version with the latest timestamp
	latest := &responses[0]
	for i := 1; i < len(responses); i++ {
		resp := &responses[i]

		// Compare timestamps (primary)
		if resp.Timestamp > latest.Timestamp {
			latest = resp
		} else if resp.Timestamp == latest.Timestamp {
			// If timestamps are equal, use version number as tiebreaker
			if resp.Version > latest.Version {
				latest = resp
			}
		}
	}

	return latest
}

// QuorumReached checks if we have enough successful responses for a quorum
func QuorumReached(responses []ReplicaResponse, quorum int) bool {
	successful := 0
	for _, resp := range responses {
		if resp.Success {
			successful++
		}
	}
	return successful >= quorum
}

// GenerateTimestamp generates a timestamp for versioning
func GenerateTimestamp() int64 {
	return time.Now().UnixNano()
}

// GenerateVersion generates a version number based on timestamp
func GenerateVersion(timestamp int64) int64 {
	return timestamp // Simple version: use timestamp as version
}

// NeedsReadRepair checks if replicas have inconsistent data
func NeedsReadRepair(responses []ReplicaResponse) bool {
	if len(responses) <= 1 {
		return false
	}

	// Check if all responses have the same timestamp and version
	firstResp := responses[0]
	for i := 1; i < len(responses); i++ {
		if responses[i].Timestamp != firstResp.Timestamp ||
			responses[i].Version != firstResp.Version {
			return true
		}
	}

	return false
}

// GetOutdatedReplicas returns list of replicas that need repair
func GetOutdatedReplicas(responses []ReplicaResponse, latest *ReplicaResponse) []string {
	outdated := make([]string, 0)

	for _, resp := range responses {
		if resp.NodeID != latest.NodeID &&
			(resp.Timestamp < latest.Timestamp || resp.Version < latest.Version) {
			outdated = append(outdated, resp.NodeID)
		}
	}

	return outdated
}
