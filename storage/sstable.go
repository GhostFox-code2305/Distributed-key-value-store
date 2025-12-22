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
// [Footer: index offset + magic number]

const (
	sstableMagicNumber = 0xDEADBEEF
	indexEntrySize     = 256 // Max key size in index
)

type SSTable struct {
	filePath string
	index    []IndexEntry
}

type IndexEntry struct {
	Key    []byte
	Offset int64
}

// SSTableWriter writes MemTable data to disk
type SSTableWriter struct {
	file       *os.File
	writer     *bufio.Writer
	filePath   string
	index      []IndexEntry
	dataOffset int64
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
	}, nil
}

// Write writes a sorted entry to the SSTable
func (w *SSTableWriter) Write(key, value []byte) error {
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

// Finalize writes the index and footer, then closes the file
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

	// Write footer: [index_offset(8)][num_entries(4)][magic(4)]
	if err := binary.Write(w.writer, binary.LittleEndian, indexOffset); err != nil {
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

	// Footer is last 16 bytes: [index_offset(8)][num_entries(4)][magic(4)]
	if fileSize < 16 {
		return nil, fmt.Errorf("invalid SSTable file: too small")
	}

	if _, err := file.Seek(fileSize-16, 0); err != nil {
		return nil, err
	}

	var indexOffset int64
	var numEntries uint32
	var magic uint32

	if err := binary.Read(file, binary.LittleEndian, &indexOffset); err != nil {
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

	return &SSTable{
		filePath: filePath,
		index:    index,
	}, nil
}

// Get retrieves a value by key from the SSTable
func (s *SSTable) Get(key []byte) ([]byte, bool, error) {
	// Binary search in index
	idx := sort.Search(len(s.index), func(i int) bool {
		return string(s.index[i].Key) >= string(key)
	})

	if idx >= len(s.index) || string(s.index[idx].Key) != string(key) {
		return nil, false, nil // Key not found
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
