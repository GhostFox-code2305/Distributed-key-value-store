package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"kvstore/storage"
)

func main() {
	dataDir := flag.String("data", "./data", "Directory for storing data files")
	flag.Parse()

	// Create LSM store (new in Week 2!)
	store, err := storage.NewLSMStore(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	log.Printf("🚀 Distributed KV Store started (LSM Tree Mode)")
	log.Printf("📁 Data directory: %s", *dataDir)
	log.Printf("💾 MemTable threshold: 64MB")
	log.Println("📝 Commands: PUT <key> <value>, GET <key>, DELETE <key>, STATS, QUIT")
	log.Println()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				fmt.Println("Usage: PUT <key> <value>")
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")

			if err := store.Put(key, []byte(value)); err != nil {
				fmt.Printf("❌ Error: %v\n", err)
			} else {
				fmt.Println("✅ OK")
			}

		case "GET":
			if len(parts) != 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}
			key := parts[1]

			value, err := store.Get(key)
			if err != nil {
				fmt.Printf("❌ Error: %v\n", err)
			} else {
				fmt.Printf("📦 %s\n", value)
			}

		case "DELETE":
			if len(parts) != 2 {
				fmt.Println("Usage: DELETE <key>")
				continue
			}
			key := parts[1]

			if err := store.Delete(key); err != nil {
				fmt.Printf("❌ Error: %v\n", err)
			} else {
				fmt.Println("🗑️  Deleted")
			}

		case "STATS":
			stats := store.Stats()
			fmt.Printf("📊 Statistics:\n")
			for k, v := range stats {
				fmt.Printf("  %s: %v\n", k, v)
			}

		case "QUIT", "EXIT":
			fmt.Println("👋 Shutting down...")
			return

		default:
			fmt.Println("❓ Unknown command. Available: PUT, GET, DELETE, STATS, QUIT")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
}
