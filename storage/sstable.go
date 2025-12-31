package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// SSTable represents a Sorted String Table (immutable on-disk file)
// Format:
// [Data Block: sorted key-value pairs]
// [Index Block: key -> offset mapping]
// [Bloom Filter Block: serialized bloom filter]
// [Footer: index offset + bloom offset + magic number]

const (
	sstableMagicNumber = 0xDEADBEEF
	indexEntrySize     = 256 // Max key size in index
)

type SSTable struct {
	filePath    string
	index       []IndexEntry
	bloomFilter *BloomFilter // NEW: Bloom filter for fast negative lookups
}

type IndexEntry struct {
	Key    []byte
	Offset int64
}

// SSTableWriter writes MemTable data to disk
type SSTableWriter struct {
	file        *os.File
	writer      *bufio.Writer
	filePath    string
	index       []IndexEntry
	dataOffset  int64
	bloomFilter *BloomFilter // NEW: Build bloom filter as we write
	numKeys     int
}

// NewSSTableWriter creates a new SSTable writer
func NewSSTableWriter(dataDir string, tableID int) (*SSTableWriter, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	filePath := filepath.Join(dataDir, fmt.Sprintf("sstable_%d.db", tableID))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}

	return &SSTableWriter{
		file:       file,
		writer:     bufio.NewWriter(file),
		filePath:   filePath,
		index:      make([]IndexEntry, 0),
		dataOffset: 0,
		numKeys:    0,
	}, nil
}

// Write writes a sorted entry to the SSTable
func (w *SSTableWriter) Write(key, value []byte) error {
	// Lazy initialize bloom filter on first write
	if w.bloomFilter == nil {
		// Estimate: we'll probably write similar number of keys as we have now
		// Start with capacity for 10000 keys, 1% false positive rate
		estimatedKeys := 10000
		w.bloomFilter = NewBloomFilter(estimatedKeys, 0.01)
	}

	// Add key to bloom filter
	w.bloomFilter.Add(key)
	w.numKeys++

	// Record index entry (key -> current offset)
	w.index = append(w.index, IndexEntry{
		Key:    append([]byte(nil), key...), // Copy key
		Offset: w.dataOffset,
	})

	// Write key length (4 bytes)
	keyLen := uint32(len(key))
	if err := binary.Write(w.writer, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	w.dataOffset += 4

	// Write key
	if _, err := w.writer.Write(key); err != nil {
		return err
	}
	w.dataOffset += int64(len(key))

	// Write value length (4 bytes)
	valueLen := uint32(len(value))
	if err := binary.Write(w.writer, binary.LittleEndian, valueLen); err != nil {
		return err
	}
	w.dataOffset += 4

	// Write value
	if _, err := w.writer.Write(value); err != nil {
		return err
	}
	w.dataOffset += int64(len(value))

	return nil
}

// Finalize writes the index, bloom filter, and footer, then closes the file
func (w *SSTableWriter) Finalize() error {
	// Write index block
	indexOffset := w.dataOffset

	for _, entry := range w.index {
		// Write key length
		keyLen := uint32(len(entry.Key))
		if err := binary.Write(w.writer, binary.LittleEndian, keyLen); err != nil {
			return err
		}

		// Write key
		if _, err := w.writer.Write(entry.Key); err != nil {
			return err
		}

		// Write offset
		if err := binary.Write(w.writer, binary.LittleEndian, entry.Offset); err != nil {
			return err
		}
	}

	// Calculate bloom filter offset (after index)
	bloomOffset := indexOffset
	for _, entry := range w.index {
		bloomOffset += int64(4 + len(entry.Key) + 8) // keyLen(4) + key + offset(8)
	}

	// Write bloom filter block
	var bloomData []byte
	if w.bloomFilter != nil {
		bloomData = w.bloomFilter.Serialize()
	} else {
		bloomData = []byte{}
	}

	if len(bloomData) > 0 {
		if _, err := w.writer.Write(bloomData); err != nil {
			return err
		}
	}

	bloomLen := uint32(len(bloomData))

	// Write footer: [index_offset(8)][bloom_offset(8)][bloom_len(4)][num_entries(4)][magic(4)]
	// Total footer size: 28 bytes
	if err := binary.Write(w.writer, binary.LittleEndian, indexOffset); err != nil {
		return err
	}

	if err := binary.Write(w.writer, binary.LittleEndian, bloomOffset); err != nil {
		return err
	}

	if err := binary.Write(w.writer, binary.LittleEndian, bloomLen); err != nil {
		return err
	}

	numEntries := uint32(len(w.index))
	if err := binary.Write(w.writer, binary.LittleEndian, numEntries); err != nil {
		return err
	}

	if err := binary.Write(w.writer, binary.LittleEndian, uint32(sstableMagicNumber)); err != nil {
		return err
	}

	// Flush and close
	if err := w.writer.Flush(); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	return w.file.Close()
}

// OpenSSTable opens an existing SSTable for reading
func OpenSSTable(filePath string) (*SSTable, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable: %w", err)
	}
	defer file.Close()

	// Read footer
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	// Footer is last 28 bytes: [index_offset(8)][bloom_offset(8)][bloom_len(4)][num_entries(4)][magic(4)]
	if fileSize < 28 {
		return nil, fmt.Errorf("invalid SSTable file: too small")
	}

	if _, err := file.Seek(fileSize-28, 0); err != nil {
		return nil, err
	}

	var indexOffset int64
	var bloomOffset int64
	var bloomLen uint32
	var numEntries uint32
	var magic uint32

	if err := binary.Read(file, binary.LittleEndian, &indexOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &bloomOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &bloomLen); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &numEntries); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}

	if magic != sstableMagicNumber {
		return nil, fmt.Errorf("invalid SSTable magic number")
	}

	// Read index
	if _, err := file.Seek(indexOffset, 0); err != nil {
		return nil, err
	}

	index := make([]IndexEntry, numEntries)
	for i := uint32(0); i < numEntries; i++ {
		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(file, key); err != nil {
			return nil, err
		}

		var offset int64
		if err := binary.Read(file, binary.LittleEndian, &offset); err != nil {
			return nil, err
		}

		index[i] = IndexEntry{
			Key:    key,
			Offset: offset,
		}
	}

	// Read bloom filter
	var bloomFilter *BloomFilter
	if bloomLen > 0 {
		if _, err := file.Seek(bloomOffset, 0); err != nil {
			return nil, err
		}

		bloomData := make([]byte, bloomLen)
		if _, err := io.ReadFull(file, bloomData); err != nil {
			return nil, err
		}

		bloomFilter = DeserializeBloomFilter(bloomData)
	}

	return &SSTable{
		filePath:    filePath,
		index:       index,
		bloomFilter: bloomFilter,
	}, nil
}

// Get retrieves a value by key from the SSTable
func (s *SSTable) Get(key []byte) ([]byte, bool, error) {
	// NEW: Check bloom filter first - if it says "definitely not present", skip disk read
	if s.bloomFilter != nil && !s.bloomFilter.MayContain(key) {
		return nil, false, nil // Definitely not in this SSTable
	}

	// Bloom filter says "might be present" or we don't have a bloom filter
	// Proceed with binary search in index
	idx := sort.Search(len(s.index), func(i int) bool {
		return string(s.index[i].Key) >= string(key)
	})

	if idx >= len(s.index) || string(s.index[idx].Key) != string(key) {
		return nil, false, nil // Key not found (bloom filter false positive)
	}

	// Read from data block
	file, err := os.Open(s.filePath)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	if _, err := file.Seek(s.index[idx].Offset, 0); err != nil {
		return nil, false, err
	}

	reader := bufio.NewReader(file)

	// Read key length
	var keyLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
		return nil, false, err
	}

	// Skip key (we already know it matches)
	if _, err := reader.Discard(int(keyLen)); err != nil {
		return nil, false, err
	}

	// Read value length
	var valueLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
		return nil, false, err
	}

	// Read value
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, false, err
	}

	return value, true, nil
}

// FilePath returns the file path
func (s *SSTable) FilePath() string {
	return s.filePath
}

// HasBloomFilter returns true if this SSTable has a bloom filter
func (s *SSTable) HasBloomFilter() bool {
	return s.bloomFilter != nil
}

// BloomFilterStats returns bloom filter statistics
func (s *SSTable) BloomFilterStats() map[string]interface{} {
	if s.bloomFilter == nil {
		return map[string]interface{}{
			"exists": false,
		}
	}
	stats := s.bloomFilter.Stats()
	stats["exists"] = true
	return stats
}
