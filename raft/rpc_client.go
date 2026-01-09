// raft/rpc_client.go
package raft

import (
	"context"
	"time"

	pb "kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCRaftClient implements the RPC client for Raft
type GRPCRaftClient struct {
	connections map[string]*grpc.ClientConn
	timeout     time.Duration
}

// NewGRPCRaftClient creates a new gRPC client
func NewGRPCRaftClient() *GRPCRaftClient {
	return &GRPCRaftClient{
		connections: make(map[string]*grpc.ClientConn),
		timeout:     2 * time.Second,
	}
}

// getConnection gets or creates a connection to a peer
func (c *GRPCRaftClient) getConnection(address string) (*grpc.ClientConn, error) {
	if conn, ok := c.connections[address]; ok {
		return conn, nil
	}

	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	c.connections[address] = conn
	return conn, nil
}

// RequestVote sends a RequestVote RPC to a peer
func (c *GRPCRaftClient) RequestVote(address string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	client := pb.NewKVStoreClient(conn)

	// Convert to protobuf
	pbReq := &pb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	pbResp, err := client.RequestVote(ctx, pbReq)
	if err != nil {
		return nil, err
	}

	// Convert back to internal type
	return &RequestVoteResponse{
		Term:        pbResp.Term,
		VoteGranted: pbResp.VoteGranted,
	}, nil
}

// AppendEntries sends an AppendEntries RPC to a peer
func (c *GRPCRaftClient) AppendEntries(address string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	conn, err := c.getConnection(address)
	if err != nil {
		return nil, err
	}

	client := pb.NewKVStoreClient(conn)

	// Convert entries to protobuf
	pbEntries := make([]*pb.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		pbEntries[i] = &pb.LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		}
	}

	pbReq := &pb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: req.LeaderCommit,
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	pbResp, err := client.AppendEntries(ctx, pbReq)
	if err != nil {
		return nil, err
	}

	// Convert back to internal type
	return &AppendEntriesResponse{
		Term:          pbResp.Term,
		Success:       pbResp.Success,
		ConflictTerm:  pbResp.ConflictTerm,
		ConflictIndex: pbResp.ConflictIndex,
	}, nil
}

// Close closes all connections
func (c *GRPCRaftClient) Close() {
	for _, conn := range c.connections {
		conn.Close()
	}
}
