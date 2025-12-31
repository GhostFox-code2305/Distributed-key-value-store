package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"kvstore/client"
	"kvstore/proto"
)

func main() {
	// Command-line flags
	serverAddr := flag.String("server", "localhost:50051", "Server address")
	flag.Parse()

	printBanner()
	log.Printf("📡 Connecting to server: %s", *serverAddr)

	// Connect to server
	kvClient, err := client.NewKVClient(*serverAddr)
	if err != nil {
		log.Fatalf("❌ Failed to connect: %v", err)
	}
	defer kvClient.Close()

	log.Println("✅ Connected to server")
	log.Println()
	printHelp()

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

			if err := kvClient.Put(key, []byte(value)); err != nil {
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

			value, err := kvClient.Get(key)
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

			if err := kvClient.Delete(key); err != nil {
				fmt.Printf("❌ Error: %v\n", err)
			} else {
				fmt.Println("🗑️  Deleted")
			}

		case "STATS":
			stats, err := kvClient.Stats()
			if err != nil {
				fmt.Printf("❌ Error: %v\n", err)
				continue
			}
			printStats(stats)

		case "COMPACT":
			fmt.Println("🔄 Triggering compaction...")
			if err := kvClient.Compact(); err != nil {
				fmt.Printf("❌ Error: %v\n", err)
			} else {
				fmt.Println("✅ Compaction completed")
			}

		case "HELP":
			printHelp()

		case "QUIT", "EXIT":
			fmt.Println("👋 Disconnecting...")
			return

		default:
			fmt.Printf("❓ Unknown command: %s\n", cmd)
			fmt.Println("Type HELP for available commands")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
}

func printBanner() {
	banner := `
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║     🖥️  KVStore CLI Client                               ║
║                                                           ║
║     Week 4: Remote Access via gRPC ✨                    ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
`
	fmt.Println(banner)
}

func printHelp() {
	help := `
📝 Available Commands:
  PUT <key> <value>    Store a key-value pair
  GET <key>            Retrieve value by key
  DELETE <key>         Delete a key
  STATS                Show server statistics
  COMPACT              Trigger manual compaction
  HELP                 Show this help message
  QUIT / EXIT          Disconnect from server
`
	fmt.Println(help)
}

func printStats(stats *proto.StatsResponse) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║                    📊 STATISTICS                          ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════╣")

	// Storage Stats
	fmt.Println("║  💾 Storage:                                              ║")
	fmt.Printf("║     MemTable Size:        %-10d bytes              ║\n", stats.MemtableSize)
	fmt.Printf("║     Number of SSTables:   %-10d                    ║\n", stats.NumSstables)
	fmt.Println("║                                                           ║")

	// Bloom Filter Stats
	fmt.Println("║  🌸 Bloom Filter:                                         ║")
	bloomTotal := stats.BloomFilterHits + stats.BloomFilterMisses
	var bloomHitRate float64
	if bloomTotal > 0 {
		bloomHitRate = float64(stats.BloomFilterHits) / float64(bloomTotal) * 100
	}
	fmt.Printf("║     Hits (skipped reads): %-10d                    ║\n", stats.BloomFilterHits)
	fmt.Printf("║     Misses (disk reads):  %-10d                    ║\n", stats.BloomFilterMisses)
	fmt.Printf("║     Hit Rate:             %-10.1f%%                 ║\n", bloomHitRate)
	fmt.Println("║                                                           ║")

	// Compaction Stats
	fmt.Println("║  🔄 Compaction:                                           ║")
	if stats.CompactionTotalCompactions > 0 {
		fmt.Printf("║     Total Compactions:    %-10d                    ║\n", stats.CompactionTotalCompactions)
		fmt.Printf("║     Keys Removed:         %-10d                    ║\n", stats.CompactionTotalKeysRemoved)
		fmt.Printf("║     Bytes Reclaimed:      %-10d bytes              ║\n", stats.CompactionTotalBytesReclaimed)
		if stats.CompactionLastCompaction != "" {
			fmt.Printf("║     Last Compaction:      %-27s║\n", stats.CompactionLastCompaction)
		}
	} else {
		fmt.Println("║     No compactions yet                                    ║")
	}

	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()
}
