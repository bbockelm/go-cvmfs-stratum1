// Package hashsort implements external sorting for content hashes.
//
// The pipeline has two concurrent stages connected by a channel:
//
//  1. Semi-sort: Incoming hashes are buffered in a min-heap. Once the
//     heap reaches a configurable memory budget, each new insertion evicts
//     the minimum element to an output channel. This produces a roughly
//     sorted stream with strong locality.
//
//  2. Chunk sort: The semi-sorted stream is accumulated into fixed-size
//     chunks (default 100 MB) and each chunk is sorted independently in
//     memory, deduplicated, and written to disk.
//
// Because the two stages run in separate goroutines, chunk sorting overlaps
// with catalog traversal and heap processing — no intermediate file is
// written between them.
//
// A ChunkMergeReader streams the fully-sorted sequence by doing a k-way
// merge across the sorted chunks on-the-fly.
package hashsort

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// Semi-sort
// ---------------------------------------------------------------------------

// SemiSortConfig controls the semi-sort phase.
type SemiSortConfig struct {
	// MaxHeapBytes is the approximate memory budget for the in-memory heap.
	MaxHeapBytes int64
	// HashSize is the number of characters per hash string (hex-encoded).
	// For SHA-1 this is 40, plus 1 for the suffix = 41 bytes per entry.
	HashSize int
}

// SemiSort reads hashes from the input channel, buffers them in a min-heap,
// and sends the semi-sorted stream to the output channel. The output channel
// is closed when all input has been consumed and the heap is drained.
//
// If hashesConsumed is non-nil, it is atomically incremented for each hash
// read from the input channel.
func SemiSort(cfg SemiSortConfig, input <-chan string, output chan<- string, hashesConsumed *int64) {
	if cfg.MaxHeapBytes <= 0 {
		cfg.MaxHeapBytes = 100 * 1024 * 1024
	}
	if cfg.HashSize <= 0 {
		cfg.HashSize = 41
	}

	// +16 for Go string overhead
	maxEntries := int(cfg.MaxHeapBytes / int64(cfg.HashSize+16))
	if maxEntries < 1024 {
		maxEntries = 1024
	}

	h := &stringHeap{}
	heap.Init(h)

	for hashStr := range input {
		if hashesConsumed != nil {
			atomic.AddInt64(hashesConsumed, 1)
		}
		heap.Push(h, hashStr)

		if h.Len() >= maxEntries {
			// Evict the minimum
			output <- heap.Pop(h).(string)
		}
	}

	// Flush remaining heap contents sorted.
	remaining := make([]string, 0, h.Len())
	for h.Len() > 0 {
		remaining = append(remaining, heap.Pop(h).(string))
	}
	sort.Strings(remaining)

	for _, s := range remaining {
		output <- s
	}

	close(output)
}

// ---------------------------------------------------------------------------
// Chunk sort
// ---------------------------------------------------------------------------

// ChunkSortConfig controls chunk-sorting of a semi-sorted file.
type ChunkSortConfig struct {
	// ChunkBytes is the target size of each chunk in bytes.
	// Default: 100 MB.
	ChunkBytes int64
}

// DefaultChunkSortConfig returns a config with 100 MB chunks.
func DefaultChunkSortConfig() ChunkSortConfig {
	return ChunkSortConfig{
		ChunkBytes: 100 * 1024 * 1024,
	}
}

// ChunkSort reads semi-sorted hashes from the input channel, accumulates
// them into fixed-size chunks, sorts and deduplicates each chunk in memory,
// and writes the result to disk. Returns the list of chunk file paths.
//
// If chunksProduced is non-nil, it is atomically incremented for each chunk
// written.
func ChunkSort(input <-chan string, outputDir string, cfg ChunkSortConfig, chunksProduced *int64) ([]string, error) {
	if cfg.ChunkBytes <= 0 {
		cfg.ChunkBytes = 100 * 1024 * 1024
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("creating chunk dir: %w", err)
	}

	var chunkFiles []string
	chunkIdx := 0

	var lines []string
	var chunkSize int64

	flushChunk := func() error {
		if len(lines) == 0 {
			return nil
		}

		// Sort this chunk in memory.
		sort.Strings(lines)

		// Deduplicate.
		deduped := lines[:0]
		for i, l := range lines {
			if i == 0 || l != lines[i-1] {
				deduped = append(deduped, l)
			}
		}

		// Write sorted chunk to disk.
		chunkPath := filepath.Join(outputDir, fmt.Sprintf("chunk-%06d.txt", chunkIdx))
		cf, err := os.Create(chunkPath)
		if err != nil {
			return fmt.Errorf("creating chunk file: %w", err)
		}
		w := bufio.NewWriterSize(cf, 1024*1024)
		for _, l := range deduped {
			fmt.Fprintln(w, l)
		}
		if err := w.Flush(); err != nil {
			cf.Close()
			return err
		}
		cf.Close()

		chunkFiles = append(chunkFiles, chunkPath)
		chunkIdx++
		if chunksProduced != nil {
			atomic.AddInt64(chunksProduced, 1)
		}

		// Reset for next chunk.
		lines = lines[:0]
		chunkSize = 0
		return nil
	}

	for line := range input {
		lineBytes := int64(len(line) + 1) // +1 for newline
		lines = append(lines, line)
		chunkSize += lineBytes
		if chunkSize >= cfg.ChunkBytes {
			if err := flushChunk(); err != nil {
				return chunkFiles, err
			}
		}
	}

	// Flush any remaining lines.
	if err := flushChunk(); err != nil {
		return chunkFiles, err
	}

	return chunkFiles, nil
}

// ---------------------------------------------------------------------------
// Streaming k-way chunk merge reader
// ---------------------------------------------------------------------------

// ChunkMergeReader streams the fully-sorted, deduplicated sequence of
// hashes by doing a k-way merge across sorted chunk files.
type ChunkMergeReader struct {
	readers []*bufio.Scanner
	files   []*os.File
	mh      *mergeHeap
	prev    string
	started bool
}

// NewChunkMergeReader opens all chunk files and prepares the k-way merge.
// Call Next() to advance and Value() to read the current hash.
// Must call Close() when done.
func NewChunkMergeReader(chunkFiles []string) (*ChunkMergeReader, error) {
	r := &ChunkMergeReader{}

	r.readers = make([]*bufio.Scanner, len(chunkFiles))
	r.files = make([]*os.File, len(chunkFiles))

	mh := &mergeHeap{}
	heap.Init(mh)

	for i, path := range chunkFiles {
		f, err := os.Open(path)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("opening chunk %s: %w", path, err)
		}
		r.files[i] = f
		r.readers[i] = bufio.NewScanner(f)
		r.readers[i].Buffer(make([]byte, 256), 1024*1024)

		if r.readers[i].Scan() {
			heap.Push(mh, mergeEntry{value: r.readers[i].Text(), readerIdx: i})
		}
	}

	r.mh = mh
	return r, nil
}

// Next advances to the next unique hash in sorted order.
// Returns false when exhausted.
func (r *ChunkMergeReader) Next() bool {
	for r.mh.Len() > 0 {
		entry := heap.Pop(r.mh).(mergeEntry)

		// Refill from the same reader.
		if r.readers[entry.readerIdx].Scan() {
			heap.Push(r.mh, mergeEntry{
				value:     r.readers[entry.readerIdx].Text(),
				readerIdx: entry.readerIdx,
			})
		}

		// Deduplicate.
		if r.started && entry.value == r.prev {
			continue
		}

		r.prev = entry.value
		r.started = true
		return true
	}
	return false
}

// Value returns the current hash string. Only valid after Next() returns true.
func (r *ChunkMergeReader) Value() string {
	return r.prev
}

// Close releases all file handles.
func (r *ChunkMergeReader) Close() {
	for _, f := range r.files {
		if f != nil {
			f.Close()
		}
	}
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

// CountLines counts the number of lines in a file.
func CountLines(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var count int64
	buf := make([]byte, 32*1024)
	for {
		n, err := f.Read(buf)
		for i := 0; i < n; i++ {
			if buf[i] == '\n' {
				count++
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, err
		}
	}
	return count, nil
}

// CountLinesMulti counts the total number of lines across multiple files.
func CountLinesMulti(paths []string) (int64, error) {
	var total int64
	for _, p := range paths {
		n, err := CountLines(p)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// --- Heap implementations ---

// stringHeap is a min-heap of strings.
type stringHeap []string

func (h stringHeap) Len() int            { return len(h) }
func (h stringHeap) Less(i, j int) bool  { return h[i] < h[j] }
func (h stringHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *stringHeap) Push(x interface{}) { *h = append(*h, x.(string)) }

func (h *stringHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// mergeEntry pairs a value with the index of its source reader.
type mergeEntry struct {
	value     string
	readerIdx int
}

// mergeHeap is a min-heap of mergeEntry, ordered by value.
type mergeHeap []mergeEntry

func (h mergeHeap) Len() int            { return len(h) }
func (h mergeHeap) Less(i, j int) bool  { return h[i].value < h[j].value }
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(mergeEntry)) }

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
