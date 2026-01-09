package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"kvstore/proto"
	"kvstore/replication"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClusterClient is a client that can communicate with multiple nodes
type ClusterClient struct {
	registry          *NodeRegistry
	connections       map[string]*grpc.ClientConn    // nodeID -> connection
	clients           map[string]proto.KVStoreClient // nodeID -> gRPC client
	hintedHandoff     *replication.HintedHandoff
	replicationFactor int
	writeQuorum       int
	readQuorum        int
}

// NewClusterClient creates a new cluster client
func NewClusterClient(nodeAddresses map[string]string) (*ClusterClient, error) {
	registry := NewNodeRegistry(DefaultVirtualNodes)
	connections := make(map[string]*grpc.ClientConn)
	clients := make(map[string]proto.KVStoreClient)

	// Connect to all nodes
	for nodeID, address := range nodeAddresses {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		conn, err := grpc.DialContext(ctx, address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			// Clean up existing connections
			for _, c := range connections {
				c.Close()
			}
			return nil, fmt.Errorf("failed to connect to node %s at %s: %w", nodeID, address, err)
		}

		connections[nodeID] = conn
		clients[nodeID] = proto.NewKVStoreClient(conn)

		// Register node
		if err := registry.RegisterNode(nodeID, address); err != nil {
			return nil, err
		}
	}

	// Initialize hinted handoff
	hintedHandoff, err := replication.NewHintedHandoff("./hints")
	if err != nil {
		return nil, fmt.Errorf("failed to create hinted handoff: %w", err)
	}

	// Start cleanup task for old hints
	hintedHandoff.StartCleanupTask(1 * time.Hour)

	return &ClusterClient{
		registry:          registry,
		connections:       connections,
		clients:           clients,
		hintedHandoff:     hintedHandoff,
		replicationFactor: replication.ReplicationFactor,
		writeQuorum:       replication.WriteQuorum,
		readQuorum:        replication.ReadQuorum,
	}, nil
}

// Put stores a key-value pair with replication
func (cc *ClusterClient) Put(key string, value []byte) error {
	// Get preference list (N nodes for replication)
	preferenceList, err := cc.registry.hashRing.GetPreferenceList(key, cc.replicationFactor)
	if err != nil {
		return fmt.Errorf("failed to get preference list: %w", err)
	}

	log.Printf("🎯 PUT %s → replicas: %v (W=%d)", key, preferenceList, cc.writeQuorum)

	// Generate version and timestamp
	timestamp := replication.GenerateTimestamp()
	version := replication.GenerateVersion(timestamp)

	// Write to replicas in parallel
	type result struct {
		nodeID  string
		success bool
		err     error
	}

	resultChan := make(chan result, len(preferenceList))
	var wg sync.WaitGroup

	for _, nodeID := range preferenceList {
		wg.Add(1)
		go func(nID string) {
			defer wg.Done()

			client, exists := cc.clients[nID]
			if !exists {
				resultChan <- result{nodeID: nID, success: false, err: fmt.Errorf("no client for node")}
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Use ReplicaPut for internal replication
			resp, err := client.ReplicaPut(ctx, &proto.ReplicaPutRequest{
				Key:       key,
				Value:     value,
				Timestamp: timestamp,
				Version:   version,
			})

			if err != nil {
				resultChan <- result{nodeID: nID, success: false, err: err}
				return
			}

			resultChan <- result{nodeID: nID, success: resp.Success, err: nil}
		}(nodeID)
	}

	// Wait for all writes to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var responses []replication.ReplicaResponse
	for res := range resultChan {
		responses = append(responses, replication.ReplicaResponse{
			NodeID:  res.nodeID,
			Success: res.success,
			Error:   res.err,
		})

		if !res.success && res.err != nil {
			log.Printf("⚠️  Failed to write to %s: %v", res.nodeID, res.err)
			// Store hint for failed node
			cc.hintedHandoff.StoreHint(res.nodeID, key, value, timestamp, version)
		}
	}

	// Check if write quorum is satisfied
	if !replication.QuorumReached(responses, cc.writeQuorum) {
		successCount := 0
		for _, r := range responses {
			if r.Success {
				successCount++
			}
		}
		return fmt.Errorf("write quorum not reached: %d/%d successful (need %d)",
			successCount, len(responses), cc.writeQuorum)
	}

	log.Printf("✅ PUT successful: %d/%d replicas (quorum: %d)",
		len(responses), cc.replicationFactor, cc.writeQuorum)

	return nil
}

// Get retrieves a value by key with quorum reads
func (cc *ClusterClient) Get(key string) ([]byte, error) {
	// Get preference list (N nodes for replication)
	preferenceList, err := cc.registry.hashRing.GetPreferenceList(key, cc.replicationFactor)
	if err != nil {
		return nil, fmt.Errorf("failed to get preference list: %w", err)
	}

	log.Printf("🎯 GET %s → replicas: %v (R=%d)", key, preferenceList, cc.readQuorum)

	// Read from replicas in parallel
	type result struct {
		nodeID    string
		value     []byte
		found     bool
		version   int64
		timestamp int64
		err       error
	}

	resultChan := make(chan result, len(preferenceList))
	var wg sync.WaitGroup

	for _, nodeID := range preferenceList {
		wg.Add(1)
		go func(nID string) {
			defer wg.Done()

			client, exists := cc.clients[nID]
			if !exists {
				resultChan <- result{nodeID: nID, found: false, err: fmt.Errorf("no client for node")}
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Use ReplicaGet for quorum reads
			resp, err := client.ReplicaGet(ctx, &proto.ReplicaGetRequest{
				Key: key,
			})

			if err != nil {
				resultChan <- result{nodeID: nID, found: false, err: err}
				return
			}

			resultChan <- result{
				nodeID:    nID,
				value:     resp.Value,
				found:     resp.Found,
				version:   resp.Version,
				timestamp: resp.Timestamp,
				err:       nil,
			}
		}(nodeID)
	}

	// Wait for all reads to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var responses []replication.ReplicaResponse
	for res := range resultChan {
		if res.found {
			responses = append(responses, replication.ReplicaResponse{
				NodeID:    res.nodeID,
				Success:   true,
				Value:     res.value,
				Version:   res.version,
				Timestamp: res.timestamp,
			})
		}
	}

	// Check if read quorum is satisfied
	if len(responses) < cc.readQuorum {
		return nil, fmt.Errorf("read quorum not reached: %d/%d successful (need %d)",
			len(responses), cc.replicationFactor, cc.readQuorum)
	}

	// No responses means key not found
	if len(responses) == 0 {
		return nil, fmt.Errorf("key not found")
	}

	// Resolve conflicts (Last-Write-Wins)
	latest := replication.ResolveConflict(responses)
	if latest == nil {
		return nil, fmt.Errorf("failed to resolve conflict")
	}

	log.Printf("✅ GET successful: found on %d/%d replicas, version=%d",
		len(responses), cc.replicationFactor, latest.Version)

	// Check if read repair is needed
	if replication.NeedsReadRepair(responses) {
		log.Printf("🔧 Read repair needed for key %s", key)
		outdated := replication.GetOutdatedReplicas(responses, latest)
		cc.performReadRepair(key, latest, outdated)
	}

	return latest.Value, nil
}

// performReadRepair updates outdated replicas with the latest value
func (cc *ClusterClient) performReadRepair(key string, latest *replication.ReplicaResponse, outdatedNodes []string) {
	// Perform read repair asynchronously
	go func() {
		for _, nodeID := range outdatedNodes {
			client, exists := cc.clients[nodeID]
			if !exists {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := client.ReplicaPut(ctx, &proto.ReplicaPutRequest{
				Key:       key,
				Value:     latest.Value,
				Timestamp: latest.Timestamp,
				Version:   latest.Version,
			})

			if err != nil {
				log.Printf("⚠️  Read repair failed for node %s: %v", nodeID, err)
			} else {
				log.Printf("✅ Read repair completed for node %s", nodeID)
			}
		}
	}()
}

// Delete removes a key-value pair with replication
func (cc *ClusterClient) Delete(key string) error {
	// Get preference list
	preferenceList, err := cc.registry.hashRing.GetPreferenceList(key, cc.replicationFactor)
	if err != nil {
		return fmt.Errorf("failed to get preference list: %w", err)
	}

	log.Printf("🎯 DELETE %s → replicas: %v", key, preferenceList)

	// Delete from replicas in parallel
	type result struct {
		nodeID  string
		success bool
		err     error
	}

	resultChan := make(chan result, len(preferenceList))
	var wg sync.WaitGroup

	for _, nodeID := range preferenceList {
		wg.Add(1)
		go func(nID string) {
			defer wg.Done()

			client, exists := cc.clients[nID]
			if !exists {
				resultChan <- result{nodeID: nID, success: false, err: fmt.Errorf("no client for node")}
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			resp, err := client.Delete(ctx, &proto.DeleteRequest{
				Key: key,
			})

			if err != nil {
				resultChan <- result{nodeID: nID, success: false, err: err}
				return
			}

			resultChan <- result{nodeID: nID, success: resp.Success, err: nil}
		}(nodeID)
	}

	// Wait for all deletes
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var responses []replication.ReplicaResponse
	for res := range resultChan {
		responses = append(responses, replication.ReplicaResponse{
			NodeID:  res.nodeID,
			Success: res.success,
			Error:   res.err,
		})
	}

	// Check if write quorum is satisfied
	if !replication.QuorumReached(responses, cc.writeQuorum) {
		return fmt.Errorf("delete quorum not reached")
	}

	log.Printf("✅ DELETE successful: %d/%d replicas", len(responses), cc.replicationFactor)
	return nil
}

// GetAllStats returns stats from all nodes
func (cc *ClusterClient) GetAllStats() (map[string]*proto.StatsResponse, error) {
	allStats := make(map[string]*proto.StatsResponse)

	for nodeID, client := range cc.clients {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.Stats(ctx, &proto.StatsRequest{})
		if err != nil {
			return nil, fmt.Errorf("Stats RPC to node %s failed: %w", nodeID, err)
		}

		allStats[nodeID] = resp
	}

	return allStats, nil
}

// GetRegistry returns the node registry
func (cc *ClusterClient) GetRegistry() *NodeRegistry {
	return cc.registry
}

// Close closes all connections
func (cc *ClusterClient) Close() error {
	for _, conn := range cc.connections {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

// GetNodeForKey returns which node is responsible for a key (for debugging)
func (cc *ClusterClient) GetNodeForKey(key string) (string, string, error) {
	node, err := cc.registry.GetNodeForKey(key)
	if err != nil {
		return "", "", err
	}
	return node.ID, node.Address, nil
}

// GetHintStats returns statistics about hinted handoff
func (cc *ClusterClient) GetHintStats() map[string]interface{} {
	return map[string]interface{}{
		"total_hints": cc.hintedHandoff.GetHintCount(),
	}
}
