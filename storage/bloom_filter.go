package storage

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure
// Used to test whether an element is a member of a set
// False positives are possible, but false negatives are not
type BloomFilter struct {
	bits      []byte // Bit array
	size      uint32 // Size in bits
	numHashes uint32 // Number of hash functions to use
}

// NewBloomFilter creates a new bloom filter
// numElements: expected number of elements
// falsePositiveRate: desired false positive rate (e.g., 0.01 for 1%)
func NewBloomFilter(numElements int, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal size and number of hash functions
	// m = -n*ln(p) / (ln(2)^2)  where m=bits, n=elements, p=false positive rate
	size := optimalBloomFilterSize(numElements, falsePositiveRate)
	numHashes := optimalHashFunctions(size, numElements)

	// Convert size to bytes (round up)
	numBytes := (size + 7) / 8

	return &BloomFilter{
		bits:      make([]byte, numBytes),
		size:      uint32(size),
		numHashes: uint32(numHashes),
	}
}

// Add adds a key to the bloom filter
func (bf *BloomFilter) Add(key []byte) {
	hashes := bf.getHashes(key)
	for _, hash := range hashes {
		bitPos := hash % bf.size
		bytePos := bitPos / 8
		bitOffset := bitPos % 8
		bf.bits[bytePos] |= (1 << bitOffset)
	}
}

// MayContain checks if a key might be in the set
// Returns true if key MIGHT be present (or false positive)
// Returns false if key is DEFINITELY not present
func (bf *BloomFilter) MayContain(key []byte) bool {
	hashes := bf.getHashes(key)
	for _, hash := range hashes {
		bitPos := hash % bf.size
		bytePos := bitPos / 8
		bitOffset := bitPos % 8

		// If any bit is not set, key is definitely not present
		if (bf.bits[bytePos] & (1 << bitOffset)) == 0 {
			return false
		}
	}
	// All bits are set, key might be present
	return true
}

// getHashes generates k hash values for a key using double hashing
// h(i) = hash1(x) + i*hash2(x)
func (bf *BloomFilter) getHashes(key []byte) []uint32 {
	hash1 := hashFNV32(key)
	hash2 := hashFNV32a(key)

	hashes := make([]uint32, bf.numHashes)
	for i := uint32(0); i < bf.numHashes; i++ {
		hashes[i] = hash1 + i*hash2
	}
	return hashes
}

// hashFNV32 computes FNV-1 32-bit hash
func hashFNV32(data []byte) uint32 {
	h := fnv.New32()
	h.Write(data)
	return h.Sum32()
}

// hashFNV32a computes FNV-1a 32-bit hash (different from FNV-1)
func hashFNV32a(data []byte) uint32 {
	h := fnv.New32a()
	h.Write(data)
	return h.Sum32()
}

// Serialize converts bloom filter to bytes for storage
func (bf *BloomFilter) Serialize() []byte {
	// Format: [size:4][numHashes:4][bits:variable]
	result := make([]byte, 8+len(bf.bits))
	binary.LittleEndian.PutUint32(result[0:4], bf.size)
	binary.LittleEndian.PutUint32(result[4:8], bf.numHashes)
	copy(result[8:], bf.bits)
	return result
}

// DeserializeBloomFilter recreates a bloom filter from bytes
func DeserializeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 8 {
		return nil
	}

	size := binary.LittleEndian.Uint32(data[0:4])
	numHashes := binary.LittleEndian.Uint32(data[4:8])
	bits := make([]byte, len(data)-8)
	copy(bits, data[8:])

	return &BloomFilter{
		bits:      bits,
		size:      size,
		numHashes: numHashes,
	}
}

// optimalBloomFilterSize calculates optimal bit array size
func optimalBloomFilterSize(n int, p float64) int {
	// m = -n*ln(p) / (ln(2)^2)
	m := -float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)
	return int(math.Ceil(m))
}

// optimalHashFunctions calculates optimal number of hash functions
func optimalHashFunctions(m, n int) int {
	// k = (m/n) * ln(2)
	k := float64(m) / float64(n) * math.Ln2
	return int(math.Ceil(k))
}

// Stats returns bloom filter statistics
func (bf *BloomFilter) Stats() map[string]interface{} {
	setBits := 0
	for _, b := range bf.bits {
		for i := 0; i < 8; i++ {
			if (b & (1 << i)) != 0 {
				setBits++
			}
		}
	}

	fillRatio := float64(setBits) / float64(bf.size)

	return map[string]interface{}{
		"size_bits":    bf.size,
		"size_bytes":   len(bf.bits),
		"num_hashes":   bf.numHashes,
		"bits_set":     setBits,
		"fill_ratio":   fillRatio,
		"expected_fpr": math.Pow(fillRatio, float64(bf.numHashes)), // Approximate false positive rate
	}
}
