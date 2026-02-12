package hashsort

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"
)

func TestSemiSortAndChunkSort(t *testing.T) {
	tmpDir := t.TempDir()

	// Generate 10000 pseudo-random hashes.
	input := make(chan string, 100)
	go func() {
		for i := 0; i < 10000; i++ {
			h := make([]byte, 20)
			h[0] = byte(i >> 8)
			h[1] = byte(i)
			h[2] = byte(i >> 16)
			for j := 3; j < 20; j++ {
				h[j] = byte(i*7 + j*13)
			}
			hx := ""
			for _, b := range h {
				hx += hexChar(b>>4) + hexChar(b&0x0f)
			}
			input <- hx
		}
		close(input)
	}()

	// Semi-sort with a small heap to force evictions, streamed into chunk sort.
	semiCh := make(chan string, 256)
	cfg := SemiSortConfig{
		MaxHeapBytes: 50 * 1024,
		HashSize:     40,
	}
	go SemiSort(cfg, input, semiCh, nil)

	// Chunk sort with small chunks to force multiple chunks.
	chunkCfg := ChunkSortConfig{ChunkBytes: 10 * 1024}
	chunkDir := filepath.Join(tmpDir, "chunks")
	chunkFiles, err := ChunkSort(semiCh, chunkDir, chunkCfg, nil)
	if err != nil {
		t.Fatalf("ChunkSort failed: %v", err)
	}

	if len(chunkFiles) < 2 {
		t.Logf("Expected multiple chunk files, got %d", len(chunkFiles))
	}

	// Verify each individual chunk is sorted and deduplicated.
	for _, cf := range chunkFiles {
		f, err := os.Open(cf)
		if err != nil {
			t.Fatalf("opening chunk %s: %v", cf, err)
		}
		scanner := bufio.NewScanner(f)
		var prev string
		for scanner.Scan() {
			line := scanner.Text()
			if line <= prev && prev != "" {
				t.Errorf("chunk %s not sorted at %q after %q", cf, line, prev)
				break
			}
			prev = line
		}
		f.Close()
	}

	t.Logf("Produced %d chunk files", len(chunkFiles))
}

func TestChunkMergeReader(t *testing.T) {
	tmpDir := t.TempDir()

	// Generate hashes, semi-sort, concat, chunk-sort.
	input := make(chan string, 100)
	go func() {
		for i := 0; i < 5000; i++ {
			h := make([]byte, 20)
			h[0] = byte(i >> 8)
			h[1] = byte(i)
			h[2] = byte(i >> 16)
			for j := 3; j < 20; j++ {
				h[j] = byte(i*7 + j*13)
			}
			hx := ""
			for _, b := range h {
				hx += hexChar(b>>4) + hexChar(b&0x0f)
			}
			input <- hx
		}
		close(input)
	}()

	semiCh := make(chan string, 256)
	cfg := SemiSortConfig{
		MaxHeapBytes: 30 * 1024,
		HashSize:     40,
	}
	go SemiSort(cfg, input, semiCh, nil)

	chunkCfg := ChunkSortConfig{ChunkBytes: 8 * 1024}
	chunkDir := filepath.Join(tmpDir, "chunks")
	chunkFiles, err := ChunkSort(semiCh, chunkDir, chunkCfg, nil)
	if err != nil {
		t.Fatalf("ChunkSort failed: %v", err)
	}

	// Use ChunkMergeReader to stream the fully-sorted sequence.
	reader, err := NewChunkMergeReader(chunkFiles)
	if err != nil {
		t.Fatalf("NewChunkMergeReader failed: %v", err)
	}
	defer reader.Close()

	var prev string
	count := 0
	for reader.Next() {
		val := reader.Value()
		if val <= prev && prev != "" {
			t.Errorf("merge stream not sorted at line %d: %q <= %q", count, val, prev)
			break
		}
		prev = val
		count++
	}

	if count == 0 {
		t.Error("merge reader produced no output")
	}

	t.Logf("ChunkMergeReader streamed %d unique hashes from %d chunks", count, len(chunkFiles))
}

func hexChar(b byte) string {
	const chars = "0123456789abcdef"
	return string(chars[b])
}
