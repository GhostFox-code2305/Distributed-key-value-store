# Distributed Key-Value Store

A production-grade distributed database implementation in Go, featuring LSM trees, Raft consensus, and consistent hashing.

## Week 1: Foundation & Write-Ahead Log ✅

### What We Built

1. **Write-Ahead Log (WAL)**: Ensures durability by logging all operations before applying them
2. **Crash Recovery**: Replays WAL entries on startup to restore state
3. **Basic KV Store**: Simple in-memory storage with Put/Get/Delete operations
4. **CLI Interface**: Interactive command-line interface for testing

### Architecture

```
┌─────────────────────────────────────┐
│         Client (CLI)                │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│       Storage Engine                │
│  ┌─────────────┐  ┌──────────────┐ │
│  │   In-Memory │  │ Write-Ahead  │ │
│  │     Map     │  │     Log      │ │
│  └─────────────┘  └──────────────┘ │
└─────────────────────────────────────┘
```

### Key Features

✅ **Durability**: All writes are persisted to disk via WAL  
✅ **Crash Recovery**: Automatic state restoration on restart  
✅ **Thread-Safe**: Concurrent reads/writes with proper locking  
✅ **Binary Protocol**: Efficient WAL entry format

### WAL Entry Format

```
[timestamp: 8 bytes]
[op_type: 1 byte]      // 1 = PUT, 2 = DELETE
[key_len: 4 bytes]
[key: variable]
[value_len: 4 bytes]
[value: variable]
```

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Unix-like system (Linux/macOS) or Windows with Go installed

### Installation

```bash
# Clone or create the project
mkdir distributed-kv && cd distributed-kv

# Initialize Go module
go mod init github.com/yourusername/distributed-kv

# Create directory structure
mkdir -p storage cmd/server data

# Copy the code files from artifacts
```

### Running the Server

```bash
# Run from project root
go run cmd/server/main.go

# Or specify custom data directory
go run cmd/server/main.go -data ./my-data
```

### Running Tests

```bash
# Run all tests
go test ./storage/...

# Run with verbose output
go test -v ./storage/...

# Run benchmarks
go test -bench=. ./storage/...
```

### Example Usage

```bash
# Start the server
$ go run cmd/server/main.go

# In the CLI:
> PUT user:1 {"name":"Alice","age":30}
OK

> GET user:1
{"name":"Alice","age":30}

> PUT user:2 {"name":"Bob","age":25}
OK

> STATS
Statistics:
  num_keys: 2

> DELETE user:1
OK

> GET user:1
Error: key not found

> QUIT
Shutting down...
```

### Testing Crash Recovery

```bash
# Terminal 1: Start server and add data
$ go run cmd/server/main.go
> PUT test recovery_works
OK
> PUT foo bar
OK
> ^C  # Kill the server (Ctrl+C)

# Terminal 1: Restart server
$ go run cmd/server/main.go
> GET test
recovery_works
> GET foo
bar
```

## Performance Characteristics

**Current Implementation (Week 1):**
- **Write Latency**: ~1-2ms (includes fsync for durability)
- **Read Latency**: ~100ns (in-memory map lookup)
- **Throughput**: 
  - Writes: ~500-1000 ops/sec (limited by fsync)
  - Reads: ~1M+ ops/sec (memory speed)

**Note**: These will change significantly in Week 2 when we implement LSM trees.

## Project Roadmap

- [x] **Week 1**: Foundation & WAL ← **YOU ARE HERE**
- [ ] **Week 2**: LSM Tree (MemTable + SSTables)
- [ ] **Week 3**: Compaction & Optimization
- [ ] **Week 4**: Networking (gRPC)
- [ ] **Week 5**: Consistent Hashing
- [ ] **Week 6**: Replication
- [ ] **Week 7-9**: Raft Consensus
- [ ] **Week 10**: Production Polish

## What's Next?

In **Week 2**, we'll replace the simple in-memory map with a proper LSM Tree:
- MemTable (in-memory sorted structure)
- SSTables (immutable sorted files on disk)
- Efficient range queries
- Better write throughput

## Technical Decisions

### Why WAL?

The Write-Ahead Log ensures **durability** - even if the process crashes, we can replay operations and restore state. This is critical for databases.

### Why `fsync`?

After writing to the WAL, we call `fsync()` to force the OS to flush data to disk. Without this, data could sit in OS buffers and be lost on a power failure.

### Trade-offs

- **Durability vs Speed**: Each write requires an fsync (slow), but guarantees durability
- **Simplicity vs Features**: Week 1 uses a simple map, not optimized for large datasets
- **In-Memory Only**: All data must fit in RAM (we'll fix this with SSTables in Week 2)

## Code Structure

```
distributed-kv/
├── storage/
│   ├── wal.go          # Write-Ahead Log implementation
│   ├── store.go        # Key-value store with WAL integration
│   └── store_test.go   # Comprehensive tests
├── cmd/
│   └── server/
│       └── main.go     # Server entry point with CLI
├── data/               # Generated: WAL and data files
├── go.mod
└── README.md
```

## Contributing

This is a learning project! Feel free to:
- Experiment with the code
- Add new features
- Optimize performance
- Break things and fix them

## Resources

- [Write-Ahead Logging](https://en.wikipedia.org/wiki/Write-ahead_logging)
- [LSM Trees Paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [Designing Data-Intensive Applications](https://dataintensive.net/)

---

**Status**: Week 1 Complete ✅  
**Next**: Week 2 - LSM Tree Implementation
