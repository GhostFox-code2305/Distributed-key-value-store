package storage

import (
	"fmt"
	"testing"
)

func TestBloomFilter_Basic(t *testing.T) {
	// Create bloom filter for 1000 elements with 1% false positive rate
	bf := NewBloomFilter(1000, 0.01)

	// Add some keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range keys {
		bf.Add([]byte(key))
	}

	// Test that added keys are found
	for _, key := range keys {
		if !bf.MayContain([]byte(key)) {
			t.Errorf("Key '%s' should be in bloom filter (false negative!)", key)
		}
	}

	// Test that non-existent keys are mostly not found
	nonExistent := []string{"fig", "grape", "kiwi", "lemon", "mango"}
	falsePositives := 0
	for _, key := range nonExistent {
		if bf.MayContain([]byte(key)) {
			falsePositives++
		}
	}

	t.Logf("False positives: %d/%d (%.1f%%)", falsePositives, len(nonExistent),
		float64(falsePositives)/float64(len(nonExistent))*100)

	// We expect some false positives, but not too many
	if falsePositives > len(nonExistent)/2 {
		t.Errorf("Too many false positives: %d/%d", falsePositives, len(nonExistent))
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	numElements := 10000
	targetFPR := 0.01 // 1%
	bf := NewBloomFilter(numElements, targetFPR)

	// Add elements
	for i := 0; i < numElements; i++ {
		key := fmt.Sprintf("key_%d", i)
		bf.Add([]byte(key))
	}

	// Test with keys not in the filter
	testSize := 10000
	falsePositives := 0
	for i := numElements; i < numElements+testSize; i++ {
		key := fmt.Sprintf("key_%d", i)
		if bf.MayContain([]byte(key)) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(testSize)
	t.Logf("Target FPR: %.2f%%, Actual FPR: %.2f%%", targetFPR*100, actualFPR*100)

	// Allow some margin of error (actual should be within 3x of target)
	if actualFPR > targetFPR*3 {
		t.Errorf("False positive rate too high: %.2f%% (target: %.2f%%)",
			actualFPR*100, targetFPR*100)
	}
}

func TestBloomFilter_Serialization(t *testing.T) {
	// Create and populate bloom filter
	bf1 := NewBloomFilter(1000, 0.01)
	keys := []string{"test1", "test2", "test3", "test4", "test5"}
	for _, key := range keys {
		bf1.Add([]byte(key))
	}

	// Serialize
	data := bf1.Serialize()
	t.Logf("Serialized size: %d bytes", len(data))

	// Deserialize
	bf2 := DeserializeBloomFilter(data)

	// Verify all keys still found
	for _, key := range keys {
		if !bf2.MayContain([]byte(key)) {
			t.Errorf("Key '%s' not found after deserialization", key)
		}
	}

	// Verify properties match
	if bf2.size != bf1.size {
		t.Errorf("Size mismatch: %d != %d", bf2.size, bf1.size)
	}
	if bf2.numHashes != bf1.numHashes {
		t.Errorf("NumHashes mismatch: %d != %d", bf2.numHashes, bf1.numHashes)
	}
}

func TestBloomFilter_EmptyFilter(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Empty filter should return false for any key
	if bf.MayContain([]byte("anything")) {
		t.Error("Empty bloom filter should return false for any key")
	}
}

func TestBloomFilter_Stats(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Add some keys
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key_%d", i)
		bf.Add([]byte(key))
	}

	stats := bf.Stats()
	t.Logf("Bloom Filter Stats:")
	t.Logf("  Size (bits): %v", stats["size_bits"])
	t.Logf("  Size (bytes): %v", stats["size_bytes"])
	t.Logf("  Num hashes: %v", stats["num_hashes"])
	t.Logf("  Bits set: %v", stats["bits_set"])
	t.Logf("  Fill ratio: %.2f%%", stats["fill_ratio"].(float64)*100)
	t.Logf("  Expected FPR: %.2f%%", stats["expected_fpr"].(float64)*100)
}

func BenchmarkBloomFilter_Add(b *testing.B) {
	bf := NewBloomFilter(1000000, 0.01)
	key := []byte("benchmark_key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(key)
	}
}

func BenchmarkBloomFilter_MayContain(b *testing.B) {
	bf := NewBloomFilter(1000000, 0.01)

	// Add some keys
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		bf.Add([]byte(key))
	}

	testKey := []byte("key_5000")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MayContain(testKey)
	}
}
