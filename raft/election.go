// raft/election.go
package raft

import (
	"fmt"
	"time"
)

// startElection initiates a new election
func (rn *RaftNode) startElection() {
	rn.mu.Lock()

	// Become candidate
	oldState := rn.state
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	currentTerm := rn.currentTerm

	// Get log info for RequestVote
	lastLogIndex := uint64(len(rn.log) - 1)
	lastLogTerm := rn.log[lastLogIndex].Term

	rn.mu.Unlock()

	rn.logger.LogStateChange(oldState, Candidate, currentTerm)
	rn.logger.LogElectionStart(currentTerm)

	// Reset election timer
	rn.resetElectionTimer()

	// Vote for self
	votesReceived := 1
	votesNeeded := len(rn.peers)/2 + 1

	// Request votes from all peers
	voteCh := make(chan bool, len(rn.peers))

	for _, peer := range rn.peers {
		go func(peerID string) {
			vote := rn.requestVote(peerID, currentTerm, lastLogIndex, lastLogTerm)
			voteCh <- vote
		}(peer)
	}

	// Collect votes (with timeout)
	timeout := time.After(rn.electionTimeout)

	for i := 0; i < len(rn.peers); i++ {
		select {
		case vote := <-voteCh:
			if vote {
				votesReceived++
				if votesReceived >= votesNeeded {
					rn.logger.LogElectionWon(currentTerm, uint64(votesReceived), uint64(votesNeeded))
					rn.becomeLeader(currentTerm)
					return
				}
			}

		case <-timeout:
			rn.logger.LogElectionLost(currentTerm, uint64(votesReceived), uint64(votesNeeded))
			return

		case <-rn.shutdownCh:
			return
		}
	}

	rn.logger.LogElectionLost(currentTerm, uint64(votesReceived), uint64(votesNeeded))
}

// becomeLeader transitions node to leader state
func (rn *RaftNode) becomeLeader(term uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Only become leader if still in the same term
	if rn.currentTerm != term || rn.state != Candidate {
		rn.logger.Debug("Cannot become leader: term mismatch or not candidate (currentTerm=%d, term=%d, state=%s)",
			rn.currentTerm, term, rn.state)
		return
	}

	oldState := rn.state
	rn.state = Leader
	rn.logger.LogStateChange(oldState, Leader, term)

	// Initialize leader state
	lastLogIndex := uint64(len(rn.log) - 1)
	for peer := range rn.nextIndex {
		rn.nextIndex[peer] = lastLogIndex + 1
		rn.matchIndex[peer] = 0
	}

	// Stop election timer, start heartbeat timer
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
		rn.logger.Debug("Stopped election timer")
	}

	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}
	rn.heartbeatTimer = time.NewTimer(rn.heartbeatTimeout)
	rn.logger.Debug("Started heartbeat timer (%v)", rn.heartbeatTimeout)

	// Send immediate heartbeat to establish leadership
	go rn.sendHeartbeats()
}

// requestVote sends RequestVote RPC to a peer
func (rn *RaftNode) requestVote(peerID string, term, lastLogIndex, lastLogTerm uint64) bool {
	req := &RequestVoteRequest{
		Term:         term,
		CandidateID:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	resp, err := rn.rpcClient.RequestVote(rn.peerAddresses[peerID], req)
	if err != nil {
		rn.logger.Debug("RequestVote to %s failed: %v", peerID, err)
		return false
	}

	// If peer has higher term, step down
	if resp.Term > term {
		rn.stepDown(resp.Term)
		return false
	}

	return resp.VoteGranted
}

// RequestVote RPC handler
func (rn *RaftNode) RequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	rn.mu.Lock()

	rn.logger.Debug("Received RequestVote from %s (term=%d, myTerm=%d)",
		req.CandidateID, req.Term, rn.currentTerm)

	// Reply false if term < currentTerm
	if req.Term < rn.currentTerm {
		currentTerm := rn.currentTerm
		rn.mu.Unlock()
		return &RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
		rn.state = Follower
	}

	// Check if we can grant vote
	voteGranted := false

	// Grant vote if:
	// 1. We haven't voted yet in this term, or we already voted for this candidate
	// 2. Candidate's log is at least as up-to-date as ours
	if (rn.votedFor == "" || rn.votedFor == req.CandidateID) &&
		rn.isLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		voteGranted = true
		rn.votedFor = req.CandidateID

		rn.logger.LogVoteGranted(req.CandidateID, req.Term)
	} else {
		reason := fmt.Sprintf("votedFor=%s, logUpToDate=%v",
			rn.votedFor, rn.isLogUpToDate(req.LastLogIndex, req.LastLogTerm))
		rn.logger.LogVoteDenied(req.CandidateID, req.Term, reason)
	}

	currentTerm := rn.currentTerm
	rn.mu.Unlock()

	// Reset timeout if we grant vote (OUTSIDE the lock)
	if voteGranted {
		rn.resetElectionTimer()
	}

	return &RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: voteGranted,
	}
}

// isLogUpToDate checks if candidate's log is at least as up-to-date as ours
func (rn *RaftNode) isLogUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	lastIndex := uint64(len(rn.log) - 1)
	lastTerm := rn.log[lastIndex].Term

	// If last log term differs, higher term wins
	if candidateLastTerm != lastTerm {
		return candidateLastTerm >= lastTerm
	}

	// Same term: longer log wins
	return candidateLastIndex >= lastIndex
}

// stepDown converts to follower (called when discovering higher term)
func (rn *RaftNode) stepDown(term uint64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if term > rn.currentTerm {
		rn.logger.LogStepDown(rn.currentTerm, term)

		oldState := rn.state
		rn.currentTerm = term
		rn.votedFor = ""
		rn.state = Follower

		if oldState != Follower {
			rn.logger.LogStateChange(oldState, Follower, term)
		}

		if rn.heartbeatTimer != nil {
			rn.heartbeatTimer.Stop()
		}
		rn.resetElectionTimer()
	}
}

// sendHeartbeats sends empty AppendEntries RPCs to all peers
func (rn *RaftNode) sendHeartbeats() {
	rn.mu.RLock()
	if rn.state != Leader {
		rn.mu.RUnlock()
		return
	}

	currentTerm := rn.currentTerm
	commitIndex := rn.commitIndex
	peerCount := len(rn.peers)
	rn.mu.RUnlock()

	rn.logger.LogHeartbeatSent(currentTerm, peerCount)

	for _, peer := range rn.peers {
		go func(peerID string) {
			// Get log info for this peer
			rn.mu.RLock()
			prevLogIndex := rn.nextIndex[peerID] - 1
			prevLogTerm := uint64(0)
			if prevLogIndex > 0 && prevLogIndex < uint64(len(rn.log)) {
				prevLogTerm = rn.log[prevLogIndex].Term
			}
			rn.mu.RUnlock()

			// Send empty AppendEntries (heartbeat)
			req := &AppendEntriesRequest{
				Term:         currentTerm,
				LeaderID:     rn.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil, // empty = heartbeat
				LeaderCommit: commitIndex,
			}

			resp, err := rn.rpcClient.AppendEntries(rn.peerAddresses[peerID], req)
			if err != nil {
				return
			}

			// If peer has higher term, step down
			if resp.Term > currentTerm {
				rn.stepDown(resp.Term)
			}
		}(peer)
	}
}

// AppendEntries RPC handler (for Week 7: heartbeats only)
func (rn *RaftNode) AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	rn.mu.Lock()

	// Reply false if term < currentTerm
	if req.Term < rn.currentTerm {
		rn.mu.Unlock()
		return &AppendEntriesResponse{
			Term:    rn.currentTerm,
			Success: false,
		}
	}

	// If RPC request contains term T > currentTerm: update term, become follower
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
		if rn.state != Follower {
			oldState := rn.state
			rn.state = Follower
			rn.logger.LogStateChange(oldState, Follower, req.Term)
		}
	}

	currentTerm := rn.currentTerm
	rn.mu.Unlock()

	// Reset election timeout - we heard from the leader (OUTSIDE the lock)
	rn.resetElectionTimer()

	// For Week 7: just log heartbeat reception
	if len(req.Entries) == 0 {
		rn.logger.LogHeartbeatReceived(req.LeaderID, req.Term)
	} else {
		rn.logger.LogAppendEntries(req.LeaderID, req.Term, req.PrevLogIndex, len(req.Entries))
	}

	// Week 7: Always succeed (we'll add log consistency checks in Week 8)
	return &AppendEntriesResponse{
		Term:    currentTerm,
		Success: true,
	}
}

// RequestVoteRequest is the RPC request structure
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResponse is the RPC response structure
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesRequest is used for both heartbeats and log replication
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*LogEntry
	LeaderCommit uint64
}

// AppendEntriesResponse is the response structure
type AppendEntriesResponse struct {
	Term    uint64
	Success bool

	// Optimization: help leader find conflicting entry faster
	ConflictTerm  uint64 // term of conflicting entry
	ConflictIndex uint64 // first index for ConflictTerm
}

// RPC transport interfaces (implement with gRPC)
type RPCServer interface {
	Start(address string) error
	Stop()
}

type RPCClient interface {
	RequestVote(address string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(address string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}
