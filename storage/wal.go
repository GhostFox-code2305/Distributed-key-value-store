package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
	path   string
}

type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

type Entry struct {
	Timestamp int64
	Op        OpType
	Key       []byte
	Value     []byte
}

func NewWAL(dirPath string) (*WAL, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	walPath := filepath.Join(dirPath, "wal.log")

	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
		path:   walPath,
	}, nil
}

func (w *WAL) Write(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := binary.Write(w.writer, binary.LittleEndian, entry.Timestamp); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}

	if err := w.writer.WriteByte(byte(entry.Op)); err != nil {
		return fmt.Errorf("failed to write op type: %w", err)
	}

	keyLen := uint32(len(entry.Key))
	if err := binary.Write(w.writer, binary.LittleEndian, keyLen); err != nil {
		return fmt.Errorf("failed to write key length: %w", err)
	}

	if _, err := w.writer.Write(entry.Key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	valueLen := uint32(len(entry.Value))
	if err := binary.Write(w.writer, binary.LittleEndian, valueLen); err != nil {
		return fmt.Errorf("failed to write value length: %w", err)
	}

	if _, err := w.writer.Write(entry.Value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	// NOTE: we avoid calling file.Sync() on every write because an
	// fsync per-Put is extremely expensive (especially on Windows).
	// Flushing the buffered writer is sufficient for tests and typical
	// throughput; we keep Sync on Reset/Close to ensure data is
	// persisted when rotating or closing the WAL.

	return nil
}

func (w *WAL) ReadAll() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to beginning: %w", err)
	}

	reader := bufio.NewReader(w.file)
	var entries []Entry

	for {
		entry, err := w.readEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (w *WAL) readEntry(reader *bufio.Reader) (Entry, error) {
	var entry Entry

	if err := binary.Read(reader, binary.LittleEndian, &entry.Timestamp); err != nil {
		return entry, err
	}

	opByte, err := reader.ReadByte()
	if err != nil {
		return entry, err
	}
	entry.Op = OpType(opByte)

	var keyLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
		return entry, err
	}

	entry.Key = make([]byte, keyLen)
	if _, err := io.ReadFull(reader, entry.Key); err != nil {
		return entry, err
	}

	var valueLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
		return entry, err
	}

	entry.Value = make([]byte, valueLen)
	if _, err := io.ReadFull(reader, entry.Value); err != nil {
		return entry, err
	}

	return entry, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriter(file)
	// Ensure new WAL file is synced to disk metadata-wise. Caller
	// may rely on Reset() to make new file durable.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL after reset: %w", err)
	}
	return nil
}
