package client

import (
	"context"
	"fmt"
	"time"

	"kvstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// KVClient is a gRPC client for the KVStore service
type KVClient struct {
	conn   *grpc.ClientConn
	client proto.KVStoreClient
}

// NewKVClient creates a new KV client
func NewKVClient(serverAddr string) (*KVClient, error) {
	// Set up connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &KVClient{
		conn:   conn,
		client: proto.NewKVStoreClient(conn),
	}, nil
}

// Put stores a key-value pair
func (c *KVClient) Put(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Put(ctx, &proto.PutRequest{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("Put RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("Put failed: %s", resp.Error)
	}

	return nil
}

// Get retrieves a value by key
func (c *KVClient) Get(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Get(ctx, &proto.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, fmt.Errorf("Get RPC failed: %w", err)
	}

	if !resp.Found {
		if resp.Error != "" {
			return nil, fmt.Errorf("Get failed: %s", resp.Error)
		}
		return nil, fmt.Errorf("key not found")
	}

	return resp.Value, nil
}

// Delete removes a key-value pair
func (c *KVClient) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Delete(ctx, &proto.DeleteRequest{
		Key: key,
	})
	if err != nil {
		return fmt.Errorf("Delete RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("Delete failed: %s", resp.Error)
	}

	return nil
}

// Stats returns storage statistics
func (c *KVClient) Stats() (*proto.StatsResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Stats(ctx, &proto.StatsRequest{})
	if err != nil {
		return nil, fmt.Errorf("Stats RPC failed: %w", err)
	}

	return resp, nil
}

// Compact triggers manual compaction
func (c *KVClient) Compact() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := c.client.Compact(ctx, &proto.CompactRequest{})
	if err != nil {
		return fmt.Errorf("Compact RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("Compact failed: %s", resp.Error)
	}

	return nil
}

// Close closes the connection
func (c *KVClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
