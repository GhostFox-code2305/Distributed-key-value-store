// raft/rpc_server.go
package raft

import (
	"context"
	"net"

	pb "kvstore/proto"

	"google.golang.org/grpc"
)

// GRPCRaftServer implements the gRPC server for Raft RPCs
type GRPCRaftServer struct {
	pb.UnimplementedKVStoreServer
	node     *RaftNode
	server   *grpc.Server
	listener net.Listener
}

// NewGRPCRaftServer creates a new gRPC server
func NewGRPCRaftServer(node *RaftNode) *GRPCRaftServer {
	return &GRPCRaftServer{
		node: node,
	}
}

// Start starts the gRPC server
func (s *GRPCRaftServer) Start(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	s.listener = lis

	s.server = grpc.NewServer()
	pb.RegisterKVStoreServer(s.server, s)

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.node.logger.Error("gRPC server error: %v", err)
		}
	}()

	return nil
}

// Stop stops the gRPC server
func (s *GRPCRaftServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// RequestVote handles RequestVote RPC
func (s *GRPCRaftServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	// Convert protobuf to internal type
	internalReq := &RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	// Call Raft node
	internalResp := s.node.RequestVote(internalReq)

	// Convert back to protobuf
	return &pb.RequestVoteResponse{
		Term:        internalResp.Term,
		VoteGranted: internalResp.VoteGranted,
	}, nil
}

// AppendEntries handles AppendEntries RPC
func (s *GRPCRaftServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	// Convert protobuf entries to internal type
	entries := make([]*LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		entries[i] = &LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		}
	}

	internalReq := &AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}

	// Call Raft node
	internalResp := s.node.AppendEntries(internalReq)

	// Convert back to protobuf
	return &pb.AppendEntriesResponse{
		Term:          internalResp.Term,
		Success:       internalResp.Success,
		ConflictTerm:  internalResp.ConflictTerm,
		ConflictIndex: internalResp.ConflictIndex,
	}, nil
}
