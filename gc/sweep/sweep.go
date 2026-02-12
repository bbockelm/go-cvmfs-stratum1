// Package sweep implements the GC sweep phase for CVMFS.
//
// It walks the data directory (2-hex-char prefix scheme) and removes any
// file not present in the set of reachable hashes.
//
// Algorithm:
//
//  1. Open a ChunkMergeReader over the sorted chunk files. This streams
//     the globally-sorted, deduplicated sequence of reachable hashes.
//  2. Iterate prefixes "00".."ff" in lexicographic order. A pool of 3
//     goroutines reads directories ahead so that ReadDir I/O overlaps
//     with merge-join processing. Results are delivered in order via a
//     sliding-window of per-prefix channels.  For each prefix:
//     a. The entries from os.ReadDir are already sorted by name and share
//        the same 2-char prefix, so they are in global sorted order.
//     b. Merge-join the sorted directory entries against the merge reader.
//        Both are in the same global sorted order, so this is a simple
//        two-pointer advance. Any directory entry not matched is deleted.
//
// Because both the directory entries (sorted in-memory per prefix) and the
// merge reader are in ascending order, the entire sweep is a single linear
// pass over both streams. Memory usage is O(entries-in-one-directory).
package sweep

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/bbockelm/cvmfs-gc-optim/gc/hashsort"
)

// Config holds configuration for the sweep phase.
type Config struct {
	// DataDir is the CVMFS data directory containing 00..ff subdirs.
	DataDir string
	// ChunkFiles is the list of sorted chunk file paths from ChunkSort.
	ChunkFiles []string
	// DryRun logs deletions without actually removing files.
	DryRun bool
	// OutputWriter, if non-nil, receives one hash per line for every
	// unreachable object found during dry-run or deletion. The caller
	// is responsible for flushing and closing the underlying file.
	OutputWriter *bufio.Writer
	// ReadAhead controls how many directory listings to issue in
	// parallel. Defaults to 3 if <= 0.
	ReadAhead int
}

// Stats holds statistics about the sweep operation.
// Fields are updated atomically and can be read concurrently for progress
// reporting while the sweep is running.
type Stats struct {
	FilesChecked  int64
	FilesDeleted  int64
	FilesRetained int64
	BytesFreed    int64
	Errors        int64
	PrefixesDone  int64 // 0..256: how many of the 00..ff prefixes are complete

	// Per-suffix deletion counters. Updated atomically.
	// Suffixes: 0=none, 'C'=catalog, 'P'=partial, 'L'=micro-catalog,
	// 'H'=history, 'X'=certificate, 'M'=metainfo.
	DeletedBySuffix [256]int64
}

// suffixFromName extracts the CVMFS hash suffix from a filename. CVMFS
// filenames are the hash hex (minus the 2-char prefix directory), optionally
// followed by a single ASCII suffix byte (C, P, L, H, X, M).
func suffixFromName(name string) byte {
	if len(name) == 0 {
		return 0
	}
	last := name[len(name)-1]
	// Valid suffixes are upper-case ASCII letters.
	if last >= 'A' && last <= 'Z' {
		return last
	}
	return 0
}

// Run performs the sweep. If stats is non-nil it is used to track progress
// (fields are updated atomically); otherwise a new Stats is allocated.
// It processes prefixes 00-ff sequentially (they must be in order to align
// with the merge reader stream), but directory listing I/O for the next
// several prefixes runs in parallel (controlled by cfg.ReadAhead, default 3).
func Run(cfg Config, stats *Stats) (*Stats, error) {
	if stats == nil {
		stats = &Stats{}
	}

	readAhead := cfg.ReadAhead
	if readAhead <= 0 {
		readAhead = 3
	}

	if len(cfg.ChunkFiles) == 0 {
		// No reachable hashes at all — delete everything.
		for i := 0; i < 256; i++ {
			prefix := fmt.Sprintf("%02x", i)
			deleteAllInPrefix(cfg, prefix, stats)
			atomic.AddInt64(&stats.PrefixesDone, 1)
		}
		return stats, nil
	}

	reader, err := hashsort.NewChunkMergeReader(cfg.ChunkFiles)
	if err != nil {
		return nil, fmt.Errorf("opening chunk merge reader: %w", err)
	}
	defer reader.Close()

	// ---- Parallel directory read-ahead with in-order delivery ----
	//
	// We launch `readAhead` goroutines that pull prefix indices from a
	// shared counter, perform os.ReadDir, and write the result into a
	// per-slot channel. A collector goroutine reads slots 0..255 in
	// order and forwards results to `dirCh`. This ensures the consumer
	// always receives prefixes in sorted order while up to `readAhead`
	// ReadDir calls overlap with processing.
	type dirResult struct {
		prefix  string
		dirPath string
		entries []os.DirEntry
		missing bool
		err     error
	}

	// One buffered-channel "slot" per prefix.
	slots := make([]chan dirResult, 256)
	for i := range slots {
		slots[i] = make(chan dirResult, 1)
	}

	// Shared index counter for the worker pool.
	var nextIdx int64

	var wg sync.WaitGroup
	for w := 0; w < readAhead; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := int(atomic.AddInt64(&nextIdx, 1) - 1)
				if idx >= 256 {
					return
				}
				prefix := fmt.Sprintf("%02x", idx)
				dirPath := filepath.Join(cfg.DataDir, prefix)
				entries, err := os.ReadDir(dirPath)
				if os.IsNotExist(err) {
					slots[idx] <- dirResult{prefix: prefix, dirPath: dirPath, missing: true}
					continue
				}
				slots[idx] <- dirResult{prefix: prefix, dirPath: dirPath, entries: entries, err: err}
			}
		}()
	}

	// Collector: read slots in order and forward to dirCh.
	dirCh := make(chan dirResult, readAhead)
	go func() {
		for i := 0; i < 256; i++ {
			dirCh <- <-slots[i]
		}
		wg.Wait()
		close(dirCh)
	}()

	// Prime the reader with the first hash.
	readerValid := reader.Next()

	for dr := range dirCh {
		if dr.missing {
			// Skip prefixes that have no directory.
			// But we still need to advance the reader past any hashes
			// with this prefix so it stays aligned.
			for readerValid && reader.Value()[:2] == dr.prefix {
				readerValid = reader.Next()
			}
			atomic.AddInt64(&stats.PrefixesDone, 1)
			continue
		}
		if dr.err != nil {
			log.Printf("WARNING: reading directory %s: %v", dr.dirPath, dr.err)
			atomic.AddInt64(&stats.Errors, 1)
			atomic.AddInt64(&stats.PrefixesDone, 1)
			continue
		}

		// Merge-join: entries from os.ReadDir are already sorted by
		// name, and all share the same prefix, so they are in global
		// sorted order. Walk them in lockstep with the merge reader.
		hadFiles := false
		for _, e := range dr.entries {
			if e.IsDir() {
				continue
			}
			hadFiles = true
			name := e.Name()
			fullHash := dr.prefix + name

			atomic.AddInt64(&stats.FilesChecked, 1)

			// Advance merge reader past entries that come before
			// this file's hash.
			for readerValid && reader.Value() < fullHash {
				readerValid = reader.Next()
			}

			if readerValid && reader.Value() == fullHash {
				// Reachable — keep it. Advance reader past this match.
				atomic.AddInt64(&stats.FilesRetained, 1)
				readerValid = reader.Next()
				continue
			}

			// Not reachable — delete (or record).
			suf := suffixFromName(name)
			atomic.AddInt64(&stats.DeletedBySuffix[suf], 1)

			filePath := filepath.Join(dr.dirPath, name)
			if cfg.OutputWriter != nil {
				fmt.Fprintln(cfg.OutputWriter, fullHash)
			}
			if cfg.DryRun {
				atomic.AddInt64(&stats.FilesDeleted, 1)
				continue
			}

			if err := os.Remove(filePath); err != nil {
				log.Printf("WARNING: failed to delete %s: %v", filePath, err)
				atomic.AddInt64(&stats.Errors, 1)
			} else {
				atomic.AddInt64(&stats.FilesDeleted, 1)
				if info, err := e.Info(); err == nil {
					atomic.AddInt64(&stats.BytesFreed, info.Size())
				}
			}
		}

		if !hadFiles {
			// Advance reader past this prefix.
			for readerValid && reader.Value()[:2] == dr.prefix {
				readerValid = reader.Next()
			}
		}
		atomic.AddInt64(&stats.PrefixesDone, 1)
	}

	return stats, nil
}

// deleteAllInPrefix deletes every file in a prefix directory (used when
// there are no reachable hashes at all).
func deleteAllInPrefix(cfg Config, prefix string, stats *Stats) {
	dirPath := filepath.Join(cfg.DataDir, prefix)
	entries, err := os.ReadDir(dirPath)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Printf("WARNING: reading directory %s: %v", dirPath, err)
		atomic.AddInt64(&stats.Errors, 1)
		return
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		atomic.AddInt64(&stats.FilesChecked, 1)

		name := e.Name()
		filePath := filepath.Join(dirPath, name)
		fullHash := prefix + name

		suf := suffixFromName(name)
		atomic.AddInt64(&stats.DeletedBySuffix[suf], 1)

		if cfg.OutputWriter != nil {
			fmt.Fprintln(cfg.OutputWriter, fullHash)
		}
		if cfg.DryRun {
			atomic.AddInt64(&stats.FilesDeleted, 1)
			continue
		}

		info, _ := e.Info()
		if err := os.Remove(filePath); err != nil {
			log.Printf("WARNING: failed to delete %s: %v", filePath, err)
			atomic.AddInt64(&stats.Errors, 1)
		} else {
			atomic.AddInt64(&stats.FilesDeleted, 1)
			if info != nil {
				atomic.AddInt64(&stats.BytesFreed, info.Size())
			}
		}
	}
}
