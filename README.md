# Distributed Key-Value Store

A production-grade distributed database implementation in Go, featuring LSM trees, Raft consensus, and consistent hashing.

## Progress Overview

- [x] **Week 1**: Foundation & WAL ✅
- [x] **Week 2**: LSM Tree (MemTable + SSTables) ✅ ← **I AM HERE**
- [ ] **Week 3**: Compaction & Optimization
- [ ] **Week 4**: Networking (gRPC)
- [ ] **Week 5**: Consistent Hashing
- [ ] **Week 6**: Replication
- [ ] **Week 7-9**: Raft Consensus
- [ ] **Week 10**: Production Polish

---

## Week 2: LSM Tree Implementation ✅

### What I Built

1. **MemTable (Skip List)**: In-memory sorted data structure
   - O(log n) insert/search operations
   - 64MB size threshold before flushing
   - Thread-safe concurrent operations

2. **SSTable (Sorted String Table)**: Immutable on-disk files
   - Sorted key-value pairs for efficient lookups
   - Index block for fast key location (binary search)
   - Footer with metadata

3. **LSM Store**: Orchestrates MemTable and SSTables
   - Write path: WAL → MemTable → SSTable (when full)
   - Read path: MemTable → SSTables (newest to oldest)
   - Automatic flushing when MemTable reaches 64MB

4. **Tombstones**: Proper deletion handling
   - Deletes write tombstone markers
   - Prevents deleted keys from reappearing

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Write Path                             │
│                                                              │
│  PUT(key, value)                                            │
│         │                                                    │
│         ├──► WAL (durability)                               │
│         │                                                    │
│         └──► MemTable (Skip List, 64MB max)                 │
│                    │                                         │
│                    │ When full                               │
│                    ▼                                         │
│              Flush to SSTable                                │
│              (immutable, sorted)                             │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                      Read Path                              │
│                                                              │
│  GET(key)                                                   │
│         │                                                    │
│         ├──► 1. Check MemTable ──────────► Found? Return   │
│         │                                                    │
│         ├──► 2. Check Immutable MemTable ─► Found? Return   │
│         │                                                    │
│         └──► 3. Check SSTables (newest → oldest)            │
│                    │                                         │
│                    └──► Binary search in index              │
│                          Read from data block               │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Key Features

✅ **Skip List MemTable**: O(log n) sorted data structure  
✅ **Automatic Flushing**: 64MB threshold prevents memory overflow  
✅ **SSTable Format**: Efficient on-disk storage with index  
✅ **Crash Recovery**: WAL replay restores MemTable state  
✅ **Tombstone Deletion**: Proper handling of deleted keys  
✅ **Multi-level Reads**: MemTable → SSTables hierarchy

### File Format

#### SSTable Structure
```
┌──────────────────────────────────────┐
│         Data Block                   │
│  [key1_len][key1][val1_len][val1]   │
│  [key2_len][key2][val2_len][val2]   │
│  ...                                 │
├──────────────────────────────────────┤
│         Index Block                  │
│  [key1_len][key1][offset1]          │
│  [key2_len][key2][offset2]          │
│  ...                                 │
├──────────────────────────────────────┤
│         Footer (16 bytes)            │
│  [index_offset: 8 bytes]            │
│  [num_entries: 4 bytes]             │
│  [magic_number: 4 bytes]            │
└──────────────────────────────────────┘
```

---

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Windows/Linux/macOS

### Installation

```bash
# Clone or create the project
mkdir distributed-kv && cd distributed-kv

# Initialize Go module
go mod init kvstore

# Create directory structure
mkdir -p storage cmd/server data
```

### Running the Server

```bash
# Run from project root
go run cmd/server/main.go

# Output:
# 🚀 Distributed KV Store started (LSM Tree Mode)
# 📁 Data directory: ./data
# 💾 MemTable threshold: 64MB
# 📝 Commands: PUT <key> <value>, GET <key>, DELETE <key>, STATS, QUIT
```

### Running Tests

```bash
# Run all tests
go test ./storage/...

# Run with verbose output
go test -v ./storage/...

# Run specific test
go test -v ./storage/ -run TestLSMStore_MemTableFlush

# Run benchmarks
go test -bench=. ./storage/...
```

### Example Usage

```bash
# Start the server
$ go run cmd/server/main.go

# Basic operations
> PUT user:1 {"name":"Alice","age":30}
✅ OK

> GET user:1
📦 {"name":"Alice","age":30}

> DELETE user:1
🗑️  Deleted

> GET user:1
❌ Error: key not found

# Check statistics
> STATS
📊 Statistics:
  memtable_size: 1024
  num_sstables: 0

# Write large amount of data to trigger flush
> PUT large_key_1 [1MB of data]
✅ OK
... (repeat 65 times)

> STATS
📊 Statistics:
  memtable_size: 512
  num_sstables: 1    # SSTable created!
```

### Testing SSTable Flush

```bash
# This script writes enough data to trigger flush
$ go run cmd/server/main.go

> PUT test_1 [paste 1KB of text]
> PUT test_2 [paste 1KB of text]
... (repeat until you see flush happen)

> STATS
📊 Statistics:
  memtable_size: 2048
  num_sstables: 1    # Data flushed to disk!
```

---

## Performance Characteristics

### Week 2 (LSM Tree)

**Write Performance:**
- **MemTable writes**: ~100-200ns (in-memory skip list)
- **With WAL**: ~1-2ms (includes fsync)
- **Throughput**: ~500-1000 writes/sec (WAL-limited)

**Read Performance:**
- **MemTable hits**: ~200ns (skip list lookup)
- **SSTable reads**: ~1-5ms (disk I/O + binary search)
- **Throughput**: 
  - Hot data (in MemTable): ~1M ops/sec
  - Cold data (SSTables): ~200-1000 ops/sec

**Space Efficiency:**
- **Compression**: None (Week 3 feature)
- **Overhead**: ~16 bytes per key-value pair (index + metadata)

**Comparison with Week 1:**

| Metric | Week 1 (Simple Map) | Week 2 (LSM Tree) |
|--------|---------------------|-------------------|
| Max dataset size | RAM limited | Disk limited |
| Write throughput | ~500 ops/sec | ~500 ops/sec |
| Read latency (hot) | ~100ns | ~200ns |
| Read latency (cold) | N/A | ~1-5ms |
| Scalability | Poor | Good |

---

## Code Structure

```
distributed-kv/
├── storage/
│   ├── wal.go           # Write-Ahead Log (Week 1)
│   ├── memtable.go      # Skip List MemTable (Week 2) ✨
│   ├── sstable.go       # SSTable writer/reader (Week 2) ✨
│   ├── lsm_store.go     # LSM orchestration (Week 2) ✨
│   └── lsm_store_test.go # Comprehensive tests (Week 2) ✨
├── cmd/
│   └── server/
│       └── main.go      # Updated CLI (Week 2) ✨
├── data/
│   ├── wal.log         # Write-ahead log
│   ├── sstable_0.db    # First SSTable (created on flush)
│   ├── sstable_1.db    # Second SSTable
│   └── ...
├── go.mod
└── README.md
```

---

## Technical Deep Dive

### Why Skip List for MemTable?

**Advantages:**
- Probabilistic balancing (simpler than Red-Black trees)
- Lock-free read operations possible
- Good cache locality
- Average O(log n) complexity

**Trade-offs:**
- Slightly more memory than hash maps
- Worst-case O(n) (extremely rare)

### Why LSM Tree?

**Perfect for:**
- Write-heavy workloads (sequential writes are fast)
- Large datasets (don't need to fit in RAM)
- Range queries (sorted data)

**Not ideal for:**
- Read-heavy with random access (need to check multiple levels)
- Small datasets that fit in RAM (simpler structures work better)

### SSTable Design Decisions

**Why immutable?**
- Simplifies concurrent access (no locks needed)
- Enables efficient caching
- Makes compaction easier

**Why separate index block?**
- Fast key lookup without scanning entire file
- Binary search on small in-memory index
- Only one disk seek per GET operation

---

## What's Next: Week 3

In **Week 3**, we'll add:
- **Compaction**: Merge overlapping SSTables to reclaim space
- **Bloom Filters**: Skip SSTables that definitely don't have a key
- **Compression**: Reduce disk usage
- **Better stats**: Track read/write amplification

These optimizations will make the database production-ready!

---

## Technical Decisions & Trade-offs

### MemTable Size (64MB)

**Why 64MB?**
- Large enough: Amortizes flush cost
- Small enough: Fits comfortably in RAM
- Industry standard: RocksDB uses 64-256MB

**Trade-offs:**
- Larger = fewer flushes, more memory
- Smaller = faster recovery, less memory

### Tombstones

**Why not delete immediately?**
- SSTables are immutable
- Key might exist in older SSTables
- Compaction will eventually remove tombstones

---

## Common Issues & Solutions

### Issue: "MemTable never flushes"

**Solution**: You need to write >64MB of data. Try this test:

```go
for i := 0; i < 70000; i++ {
    key := fmt.Sprintf("key_%d", i)
    value := make([]byte, 1024) // 1KB
    store.Put(key, value)
}
```

### Issue: "Reads are slow after flush"

**Expected behavior**: Reads from disk (SSTables) are 1000x slower than RAM (MemTable). Week 3 will add Bloom filters to help.

### Issue: "Disk usage grows quickly"

**Expected behavior**: No compaction yet. Each flush creates a new SSTable. Week 3 adds compaction to merge and reclaim space.

---

## Resources

- [Log-Structured Merge-Trees (Original Paper)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [Skip Lists: A Probabilistic Alternative to Balanced Trees](https://15721.courses.cs.cmu.edu/spring2018/papers/08-oltpindexes1/pugh-skiplists-cacm1990.pdf)
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
- [Designing Data-Intensive Applications](https://dataintensive.net/)

---

## Testing Checklist

Week 2 tests:
- [x] Basic Put/Get/Delete operations
- [x] MemTable automatic flush (>64MB)
- [x] Reading from SSTables
- [x] Crash recovery with WAL
- [x] Skip list maintains sorted order
- [x] Tombstone deletion

Run all tests:
```bash
go test -v ./storage/...
```

---

**Status**: Week 2 Complete ✅  
**Next**: Week 3 - Compaction & Optimization  
**Lines of Code**: ~800 (total: ~1600)
