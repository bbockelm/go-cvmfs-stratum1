// Package sweep implements the GC sweep phase for CVMFS.
//
// The sweep uses a two-pass algorithm designed to minimise the time the
// repository lock is held:
//
// Pass 1 — Candidate Collection (no lock required):
//
//  1. Open a ChunkMergeReader over the sorted chunk files produced by
//     Phase 1 (catalog traversal → semi-sort → chunk-sort).
//  2. Iterate prefixes "00".."ff" in lexicographic order with parallel
//     read-ahead.  For each prefix, merge-join the sorted directory
//     entries against the merge reader.  Any entry not matched is added
//     to the candidate set.
//
// Pass 2 — Locked Deletion:
//
//  4. Acquire the repository lock.
//  5. Re-walk all catalogs from the (possibly updated) manifest to
//     discover hashes that became reachable while we were scanning.
//  6. Remove those hashes from the candidate set.
//  7. Delete the remaining candidates.
//  8. Release the lock.
//
// The existing Run() function preserves the original single-pass API for
// backward compatibility and testing.
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
	// unreachable object found during dry-run or deletion.  The caller
	// is responsible for flushing and closing the underlying file.
	OutputWriter *bufio.Writer
	// ReadAhead controls how many directory listings to issue in
	// parallel.  Defaults to 3 if <= 0.
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

	// Per-suffix deletion counters.  Updated atomically.
	// Suffixes: 0=none, 'C'=catalog, 'P'=partial, 'L'=micro-catalog,
	// 'H'=history, 'X'=certificate, 'M'=metainfo.
	DeletedBySuffix [256]int64

	// Two-pass specific counters.
	CandidatesFound    int64 // after pass 1 merge-join
	CandidatesProtected int64 // removed by pass 2 delta
}

// Candidate represents a file identified as potentially unreachable.
type Candidate struct {
	// FullHash is prefix + filename (e.g. "aa" + "bcdef...").
	FullHash string
	// FilePath is the absolute path on disk.
	FilePath string
}

// suffixFromName extracts the CVMFS hash suffix from a filename.  CVMFS
// filenames are the hash hex (minus the 2-char prefix directory), optionally
// followed by a single ASCII suffix byte (C, P, L, H, X, M).
func suffixFromName(name string) byte {
	if len(name) == 0 {
		return 0
	}
	last := name[len(name)-1]
	if last >= 'A' && last <= 'Z' {
		return last
	}
	return 0
}

// ===================================================================
// Pass 1: Candidate Collection
// ===================================================================

// CollectCandidates performs the merge-join sweep and returns a map of
// unreachable hash → Candidate.  No files are deleted.
//
// The stats argument is updated atomically for progress reporting
// (FilesChecked, FilesRetained, PrefixesDone, CandidatesFound).
func CollectCandidates(cfg Config, stats *Stats) (map[string]Candidate, error) {
	if stats == nil {
		stats = &Stats{}
	}

	readAhead := cfg.ReadAhead
	if readAhead <= 0 {
		readAhead = 3
	}

	candidates := make(map[string]Candidate)

	if len(cfg.ChunkFiles) == 0 {
		// No reachable hashes at all — everything is a candidate.
		for i := 0; i < 256; i++ {
			prefix := fmt.Sprintf("%02x", i)
			collectAllInPrefix(cfg.DataDir, prefix, candidates, stats)
			atomic.AddInt64(&stats.PrefixesDone, 1)
		}
		return candidates, nil
	}

	reader, err := hashsort.NewChunkMergeReader(cfg.ChunkFiles)
	if err != nil {
		return nil, fmt.Errorf("opening chunk merge reader: %w", err)
	}
	defer reader.Close()

	// ---- Parallel directory read-ahead with in-order delivery ----
	dirCh := startReadAhead(cfg.DataDir, readAhead)

	// Prime the reader with the first hash.
	readerValid := reader.Next()

	for dr := range dirCh {
		if dr.missing {
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

		hadFiles := false
		for _, e := range dr.entries {
			if e.IsDir() {
				continue
			}
			hadFiles = true
			name := e.Name()
			fullHash := dr.prefix + name

			atomic.AddInt64(&stats.FilesChecked, 1)

			// Advance merge reader past entries before this file.
			for readerValid && reader.Value() < fullHash {
				readerValid = reader.Next()
			}

			if readerValid && reader.Value() == fullHash {
				atomic.AddInt64(&stats.FilesRetained, 1)
				readerValid = reader.Next()
				continue
			}

			// Unreachable — record as candidate.
			candidates[fullHash] = Candidate{
				FullHash: fullHash,
				FilePath: filepath.Join(dr.dirPath, name),
			}
			atomic.AddInt64(&stats.CandidatesFound, 1)
		}

		if !hadFiles {
			for readerValid && reader.Value()[:2] == dr.prefix {
				readerValid = reader.Next()
			}
		}
		atomic.AddInt64(&stats.PrefixesDone, 1)
	}

	return candidates, nil
}



// ===================================================================
// Pass 2: Delta Subtraction
// ===================================================================

// SubtractReachable removes any hash present in the reachable set from the
// candidates map.  The reachable set is typically built from a catalog
// re-walk performed under the repository lock.
func SubtractReachable(candidates map[string]Candidate, reachable map[string]struct{}, stats *Stats) {
	var removed int64
	for hash := range reachable {
		if _, ok := candidates[hash]; ok {
			delete(candidates, hash)
			removed++
		}
	}
	if stats != nil {
		atomic.AddInt64(&stats.CandidatesProtected, removed)
	}
}

// ===================================================================
// Deletion
// ===================================================================

// DeleteCandidates removes (or records in dry-run mode) the remaining
// candidates.  It updates stats.FilesDeleted, BytesFreed, DeletedBySuffix,
// and Errors.
func DeleteCandidates(candidates map[string]Candidate, cfg Config, stats *Stats) {
	if stats == nil {
		stats = &Stats{}
	}

	for _, c := range candidates {
		name := c.FullHash
		suf := suffixFromName(name)
		atomic.AddInt64(&stats.DeletedBySuffix[suf], 1)

		if cfg.OutputWriter != nil {
			fmt.Fprintln(cfg.OutputWriter, c.FullHash)
		}

		if cfg.DryRun {
			atomic.AddInt64(&stats.FilesDeleted, 1)
			continue
		}

		// Stat the file just before removal to get accurate size.
		var sz int64
		if fi, err := os.Lstat(c.FilePath); err == nil {
			sz = fi.Size()
		}

		if err := os.Remove(c.FilePath); err != nil {
			if !os.IsNotExist(err) {
				log.Printf("WARNING: failed to delete %s: %v", c.FilePath, err)
				atomic.AddInt64(&stats.Errors, 1)
			}
		} else {
			atomic.AddInt64(&stats.FilesDeleted, 1)
			atomic.AddInt64(&stats.BytesFreed, sz)
		}
	}
}

// ===================================================================
// Single-pass Run (backward compatible)
// ===================================================================

// Run performs the sweep in a single pass (collect + delete without locking
// or delta).  This preserves the original API for simple usage and testing.
func Run(cfg Config, stats *Stats) (*Stats, error) {
	if stats == nil {
		stats = &Stats{}
	}

	candidates, err := CollectCandidates(cfg, stats)
	if err != nil {
		return stats, err
	}

	DeleteCandidates(candidates, cfg, stats)
	return stats, nil
}

// ===================================================================
// Read-ahead helpers
// ===================================================================

type dirResult struct {
	prefix  string
	dirPath string
	entries []os.DirEntry
	missing bool
	err     error
}

// startReadAhead launches goroutines that read directories 00..ff in
// parallel and delivers results in order via a channel.
func startReadAhead(dataDir string, readAhead int) <-chan dirResult {
	slots := make([]chan dirResult, 256)
	for i := range slots {
		slots[i] = make(chan dirResult, 1)
	}

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
				dirPath := filepath.Join(dataDir, prefix)
				entries, err := os.ReadDir(dirPath)
				if os.IsNotExist(err) {
					slots[idx] <- dirResult{prefix: prefix, dirPath: dirPath, missing: true}
					continue
				}
				slots[idx] <- dirResult{prefix: prefix, dirPath: dirPath, entries: entries, err: err}
			}
		}()
	}

	dirCh := make(chan dirResult, readAhead)
	go func() {
		for i := 0; i < 256; i++ {
			dirCh <- <-slots[i]
		}
		wg.Wait()
		close(dirCh)
	}()

	return dirCh
}

// collectAllInPrefix adds every file in a prefix directory to the
// candidates map (used when there are no reachable hashes at all).
func collectAllInPrefix(dataDir, prefix string, candidates map[string]Candidate, stats *Stats) {
	dirPath := filepath.Join(dataDir, prefix)
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
		fullHash := prefix + name

		candidates[fullHash] = Candidate{
			FullHash: fullHash,
			FilePath: filepath.Join(dirPath, name),
		}
		atomic.AddInt64(&stats.CandidatesFound, 1)
	}
}
