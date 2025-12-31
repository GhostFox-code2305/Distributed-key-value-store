package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"kvstore/proto"
	"kvstore/server"
	"kvstore/storage"

	"google.golang.org/grpc"
)

func main() {
	// Command-line flags
	port := flag.Int("port", 50051, "Port to listen on")
	dataDir := flag.String("data", "./data", "Directory for storing data files")
	flag.Parse()

	printBanner()

	// Create LSM store
	log.Printf("📁 Initializing data directory: %s", *dataDir)
	store, err := storage.NewLSMStore(*dataDir)
	if err != nil {
		log.Fatalf("❌ Failed to create store: %v", err)
	}
	defer store.Close()

	log.Println("✅ LSM Store initialized")
	log.Printf("💾 MemTable threshold: 64MB")
	log.Printf("🔄 Compaction: Enabled")

	// Create gRPC server
	grpcServer := grpc.NewServer()
	kvServer := server.NewGRPCServer(store)
	proto.RegisterKVStoreServer(grpcServer, kvServer)

	// Listen on TCP port
	addr := fmt.Sprintf(":%d", *port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("❌ Failed to listen on %s: %v", addr, err)
	}

	log.Printf("🚀 gRPC Server listening on %s", addr)
	log.Println("📡 Ready to accept connections...")
	log.Println()
	log.Println("Connect using: ./client -server localhost:50051")
	log.Println("Press Ctrl+C to shutdown")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println()
		log.Println("🛑 Shutting down gracefully...")
		grpcServer.GracefulStop()
		kvServer.Close()
		log.Println("👋 Goodbye!")
		os.Exit(0)
	}()

	// Start serving
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("❌ Failed to serve: %v", err)
	}
}

func printBanner() {
	banner := `
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║     🚀 Distributed Key-Value Store (Network Mode)        ║
║                                                           ║
║     Week 4: gRPC Networking ✨                           ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
`
	fmt.Println(banner)
}
