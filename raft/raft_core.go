// raft/raft_core.go
package raft

import (
	"sync"
	"time"
)

// NodeState represents the current state of a Raft node
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RaftNode represents a single node in the Raft cluster
type RaftNode struct {
	mu sync.RWMutex

	// Persistent state (must survive crashes)
	currentTerm uint64
	votedFor    string // nodeID we voted for in currentTerm
	log         []*LogEntry

	// Volatile state (all nodes)
	commitIndex uint64 // highest log entry known to be committed
	lastApplied uint64 // highest log entry applied to state machine
	state       NodeState

	// Volatile state (leaders only - reinitialized after election)
	nextIndex  map[string]uint64 // for each peer, index of next log entry to send
	matchIndex map[string]uint64 // for each peer, highest log entry known to be replicated

	// Node identity
	id            string
	peers         []string // other node IDs
	address       string   // this node's address
	peerAddresses map[string]string

	// Timers
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
	electionTimer    *time.Timer
	heartbeatTimer   *time.Timer

	// Channels
	applyCh    chan ApplyMsg // send committed entries here
	shutdownCh chan struct{} // signal shutdown
	newEntryCh chan struct{} // signal new log entry for leader

	// RPC transport
	rpcServer RPCServer
	rpcClient RPCClient

	// State machine (your LSM store)
	stateMachine StateMachine

	// Logging
	logger *Logger
}

// LogEntry represents a single command in the replicated log
type LogEntry struct {
	Index   uint64
	Term    uint64
	Command []byte // serialized command (PUT/DELETE)
}

// ApplyMsg is sent on applyCh when an entry is committed
type ApplyMsg struct {
	Index   uint64
	Command []byte
	Term    uint64
}

// StateMachine interface - your LSMStore implements this
type StateMachine interface {
	Apply(command []byte) (interface{}, error)
	CreateSnapshot() ([]byte, error)
	RestoreSnapshot(snapshot []byte) error
}

// Config holds node configuration
type Config struct {
	ID               string
	Peers            []string
	PeerAddresses    map[string]string
	Address          string
	ElectionTimeout  time.Duration // 150-300ms randomized
	HeartbeatTimeout time.Duration // 50ms
	StateMachine     StateMachine
}

// NewRaftNode creates a new Raft node
func NewRaftNode(config *Config) *RaftNode {
	rn := &RaftNode{
		id:               config.ID,
		peers:            config.Peers,
		peerAddresses:    config.PeerAddresses,
		address:          config.Address,
		currentTerm:      0,
		votedFor:         "",
		log:              []*LogEntry{{Index: 0, Term: 0}}, // dummy entry at index 0
		commitIndex:      0,
		lastApplied:      0,
		state:            Follower,
		nextIndex:        make(map[string]uint64),
		matchIndex:       make(map[string]uint64),
		electionTimeout:  config.ElectionTimeout,
		heartbeatTimeout: config.HeartbeatTimeout,
		applyCh:          make(chan ApplyMsg, 100),
		shutdownCh:       make(chan struct{}),
		newEntryCh:       make(chan struct{}, 1),
		stateMachine:     config.StateMachine,
		logger:           NewLogger(config.ID, DEBUG), // DEBUG to see heartbeats
	}

	// Initialize peer tracking
	for _, peer := range rn.peers {
		rn.nextIndex[peer] = 1
		rn.matchIndex[peer] = 0
	}

	// Initialize RPC components
	rn.rpcServer = NewGRPCRaftServer(rn)
	rn.rpcClient = NewGRPCRaftClient()

	return rn
}

// Start begins the Raft node's operation
func (rn *RaftNode) Start() error {
	rn.logger.Info("Starting Raft node at %s", rn.address)

	// Initialize timers BEFORE starting event loop
	rn.electionTimer = time.NewTimer(rn.electionTimeout)
	rn.heartbeatTimer = time.NewTimer(rn.heartbeatTimeout)
	rn.heartbeatTimer.Stop() // Stop heartbeat timer initially (only leaders send heartbeats)

	// Start RPC server
	if err := rn.rpcServer.Start(rn.address); err != nil {
		return err
	}

	// Randomize election timer
	rn.resetElectionTimer()

	// Main event loop
	go rn.run()

	return nil
}

// run is the main event loop
func (rn *RaftNode) run() {
	for {
		select {
		case <-rn.shutdownCh:
			return

		case <-rn.electionTimer.C:
			// Election timeout - become candidate
			rn.logger.LogElectionTimeout()
			rn.startElection()

		case <-rn.heartbeatTimer.C:
			// Leader sends heartbeats
			if rn.getState() == Leader {
				rn.sendHeartbeats()
				rn.resetHeartbeatTimer()
			}

		case <-rn.newEntryCh:
			// Leader has new entries to replicate
			if rn.getState() == Leader {
				rn.replicateLog()
			}
		}
	}
}

// replicateLog replicates new log entries (placeholder for Week 8)
func (rn *RaftNode) replicateLog() {
	// Week 8: Implement log replication
}

// GetState returns current term and whether this node is the leader
func (rn *RaftNode) GetState() (uint64, bool) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentTerm, rn.state == Leader
}

func (rn *RaftNode) getState() NodeState {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.state
}

// Shutdown stops the Raft node
func (rn *RaftNode) Shutdown() {
	rn.logger.Info("Shutting down Raft node")
	close(rn.shutdownCh)

	// Stop timers
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}

	rn.rpcServer.Stop()
}

// Helper: reset election timer with randomized timeout
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	// Randomize: baseTimeout + [0, 150ms]
	timeout := rn.electionTimeout + time.Duration(randomInt(0, 150))*time.Millisecond
	rn.electionTimer = time.NewTimer(timeout)
}

func (rn *RaftNode) resetHeartbeatTimer() {
	if rn.heartbeatTimer != nil {
		rn.heartbeatTimer.Stop()
	}
	rn.heartbeatTimer = time.NewTimer(rn.heartbeatTimeout)
}
