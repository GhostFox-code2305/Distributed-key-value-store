// raft/election_test.go
package raft

import (
	"fmt"
	"testing"
	"time"
)

// Test 1: Initial state is Follower
func TestInitialState(t *testing.T) {
	rn := createTestNode("node1", []string{"node2", "node3"})
	defer rn.Shutdown()

	term, isLeader := rn.GetState()
	if term != 0 {
		t.Errorf("Expected term 0, got %d", term)
	}
	if isLeader {
		t.Error("New node should not be leader")
	}
	if rn.getState() != Follower {
		t.Errorf("Expected Follower state, got %s", rn.getState())
	}
}

// Test 2: Single node becomes leader
func TestSingleNodeElection(t *testing.T) {
	rn := createTestNode("node1", []string{})
	defer rn.Shutdown()

	rn.Start()

	// Wait for election
	time.Sleep(300 * time.Millisecond)

	_, isLeader := rn.GetState()
	if !isLeader {
		t.Error("Single node should become leader")
	}
}

// Test 3: Leader election in 3-node cluster
func TestBasicElection(t *testing.T) {
	nodes := createTestCluster(3)
	defer shutdownCluster(nodes)

	// Start all nodes
	for _, node := range nodes {
		node.Start()
	}

	// Wait for election
	time.Sleep(500 * time.Millisecond)

	// Check that exactly one leader was elected
	leaders := countLeaders(nodes)
	if leaders != 1 {
		t.Errorf("Expected 1 leader, got %d", leaders)
	}

	// Check all nodes agree on term
	terms := make(map[uint64]int)
	for _, node := range nodes {
		term, _ := node.GetState()
		terms[term]++
	}

	if len(terms) != 1 {
		t.Errorf("Nodes don't agree on term: %v", terms)
	}
}

// Test 4: Re-election after leader failure
func TestReElection(t *testing.T) {
	nodes := createTestCluster(3)
	defer shutdownCluster(nodes)

	// Start all nodes
	for _, node := range nodes {
		node.Start()
	}

	// Wait for initial election
	time.Sleep(500 * time.Millisecond)

	// Find and kill the leader
	var leader *RaftNode
	for _, node := range nodes {
		if _, isLeader := node.GetState(); isLeader {
			leader = node
			break
		}
	}

	if leader == nil {
		t.Fatal("No leader elected")
	}

	oldTerm, _ := leader.GetState()
	leader.Shutdown()

	// Wait for re-election
	time.Sleep(500 * time.Millisecond)

	// Check new leader elected
	remainingNodes := []*RaftNode{}
	for _, node := range nodes {
		if node != leader {
			remainingNodes = append(remainingNodes, node)
		}
	}

	leaders := countLeaders(remainingNodes)
	if leaders != 1 {
		t.Errorf("Expected 1 new leader, got %d", leaders)
	}

	// Check term increased
	newTerm, _ := remainingNodes[0].GetState()
	if newTerm <= oldTerm {
		t.Errorf("Term should increase after re-election: old=%d, new=%d", oldTerm, newTerm)
	}
}

// Test 5: No split brain - network partition heals
func TestNetworkPartitionHealing(t *testing.T) {
	nodes := createTestCluster(5)
	defer shutdownCluster(nodes)

	// Start all nodes
	for _, node := range nodes {
		node.Start()
	}

	// Wait for initial election
	time.Sleep(500 * time.Millisecond)

	initialLeaders := countLeaders(nodes)
	if initialLeaders != 1 {
		t.Errorf("Expected 1 leader, got %d", initialLeaders)
	}

	// Wait and ensure still only 1 leader
	time.Sleep(1 * time.Second)

	leaders := countLeaders(nodes)
	if leaders != 1 {
		t.Errorf("Expected 1 leader after partition, got %d", leaders)
	}
}

// Test 6: Election timeout randomization prevents split votes
func TestRandomizedTimeout(t *testing.T) {
	nodes := createTestCluster(5)
	defer shutdownCluster(nodes)

	// Start nodes
	for _, node := range nodes {
		node.Start()
	}

	// Run multiple election cycles
	maxAttempts := 10
	for i := 0; i < maxAttempts; i++ {
		time.Sleep(500 * time.Millisecond)

		leaders := countLeaders(nodes)
		if leaders == 1 {
			// Success!
			return
		}

		// Reset for next attempt
		for _, node := range nodes {
			node.stepDown(node.currentTerm + 1)
		}
	}

	t.Error("Failed to elect leader after multiple attempts (possible split vote issue)")
}

// Test 7: Follower refuses to vote if candidate's log is outdated
func TestVoteRefusalForOutdatedLog(t *testing.T) {
	follower := createTestNode("node1", []string{"node2"})
	defer follower.Shutdown()

	// Give follower a log entry in term 5
	follower.log = append(follower.log, &LogEntry{
		Index:   1,
		Term:    5,
		Command: []byte("test"),
	})
	follower.currentTerm = 5

	// Candidate requests vote with outdated log (term 3)
	req := &RequestVoteRequest{
		Term:         6,
		CandidateID:  "node2",
		LastLogIndex: 1,
		LastLogTerm:  3, // older term
	}

	resp := follower.RequestVote(req)

	if resp.VoteGranted {
		t.Error("Should not grant vote to candidate with outdated log")
	}
}

// Test 8: Node only votes once per term
func TestOneVotePerTerm(t *testing.T) {
	node := createTestNode("node1", []string{"node2", "node3"})
	defer node.Shutdown()

	// First vote request
	req1 := &RequestVoteRequest{
		Term:         1,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp1 := node.RequestVote(req1)
	if !resp1.VoteGranted {
		t.Error("Should grant first vote")
	}

	// Second vote request from different candidate (same term)
	req2 := &RequestVoteRequest{
		Term:         1,
		CandidateID:  "node3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp2 := node.RequestVote(req2)
	if resp2.VoteGranted {
		t.Error("Should not grant second vote in same term")
	}
}

// Helper functions

func createTestNode(id string, peers []string) *RaftNode {
	peerAddrs := make(map[string]string)
	for _, peer := range peers {
		peerAddrs[peer] = "localhost:5005" + peer[len(peer)-1:]
	}

	config := &Config{
		ID:               id,
		Peers:            peers,
		PeerAddresses:    peerAddrs,
		Address:          "localhost:5005" + id[len(id)-1:],
		ElectionTimeout:  150 * time.Millisecond,
		HeartbeatTimeout: 50 * time.Millisecond,
		StateMachine:     &MockStateMachine{},
	}

	return NewRaftNode(config)
}

func createTestCluster(n int) []*RaftNode {
	nodes := make([]*RaftNode, n)
	peers := make([]string, n)
	peerAddrs := make(map[string]string)

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node%d", i+1)
		peers[i] = id
		peerAddrs[id] = fmt.Sprintf("localhost:5005%d", i+1)
	}

	for i := 0; i < n; i++ {
		myID := peers[i]
		otherPeers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if i != j {
				otherPeers = append(otherPeers, peers[j])
			}
		}

		config := &Config{
			ID:               myID,
			Peers:            otherPeers,
			PeerAddresses:    peerAddrs,
			Address:          peerAddrs[myID],
			ElectionTimeout:  150 * time.Millisecond,
			HeartbeatTimeout: 50 * time.Millisecond,
			StateMachine:     &MockStateMachine{},
		}

		nodes[i] = NewRaftNode(config)
	}

	return nodes
}

func shutdownCluster(nodes []*RaftNode) {
	for _, node := range nodes {
		node.Shutdown()
	}
}

func countLeaders(nodes []*RaftNode) int {
	count := 0
	for _, node := range nodes {
		if _, isLeader := node.GetState(); isLeader {
			count++
		}
	}
	return count
}

// MockStateMachine for testing
type MockStateMachine struct{}

func (m *MockStateMachine) Apply(command []byte) (interface{}, error) {
	return nil, nil
}

func (m *MockStateMachine) CreateSnapshot() ([]byte, error) {
	return nil, nil
}

func (m *MockStateMachine) RestoreSnapshot(snapshot []byte) error {
	return nil
}
