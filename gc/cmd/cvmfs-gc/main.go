// cvmfs-gc is an optimized garbage collector for CVMFS Stratum-1 servers.
//
// It replaces the traditional mark-and-sweep approach with a two-phase
// algorithm designed for repositories with hundreds of millions of objects:
//
//  1. Catalog traversal + streaming sort: parallel catalog tree walk feeds
//     a semi-sort heap, which streams into a concurrent chunk-sort goroutine
//     that writes ~100 MB sorted chunks to disk.  No intermediate files are
//     written between the semi-sort and chunk-sort stages.
//  2. Sequential directory sweep: for each prefix, list + sort dir entries,
//     merge-join against the k-way streaming merge of sorted chunks.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bbockelm/cvmfs-gc-optim/gc/catalog"
	"github.com/bbockelm/cvmfs-gc-optim/gc/hashsort"
	"github.com/bbockelm/cvmfs-gc-optim/gc/mirror"
	"github.com/bbockelm/cvmfs-gc-optim/gc/repolock"
	"github.com/bbockelm/cvmfs-gc-optim/gc/sweep"
)

// cleanupRegistry tracks directories that should be removed on exit,
// including when the process is interrupted by SIGINT or SIGTERM.
// All methods are safe for concurrent use.
var cleanupRegistry struct {
	mu   sync.Mutex
	dirs []string
}

// repoLocks holds the CVMFS GC + update locks. Released on exit or signal.
var repoLocks repolock.Set

func registerCleanup(dir string) {
	cleanupRegistry.mu.Lock()
	cleanupRegistry.dirs = append(cleanupRegistry.dirs, dir)
	cleanupRegistry.mu.Unlock()
}

func runCleanup() {
	cleanupRegistry.mu.Lock()
	dirs := cleanupRegistry.dirs
	cleanupRegistry.dirs = nil
	cleanupRegistry.mu.Unlock()
	for _, d := range dirs {
		os.RemoveAll(d)
	}
}

func init() {
	// Catch SIGINT/SIGTERM so we can clean up temp files.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Caught %v, cleaning up...", sig)
		repoLocks.Release()
		runCleanup()
		os.Exit(1)
	}()
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		repoDir      = flag.String("repo", "", "Path to the CVMFS repository (local)")
		rootHash     = flag.String("root-hash", "", "Hex hash of root catalog")
		parallelism  = flag.Int("parallelism", 8, "Catalog traversal workers")
		heapMB       = flag.Int("heap-mb", 100, "Heap memory budget in MB for semi-sort")
		chunkMB      = flag.Int("chunk-mb", 100, "Chunk size in MB for chunk sort")
		doDelete     = flag.Bool("delete", false, "Actually delete unreachable files (default is list only)")
		outputFile   = flag.String("output", "", "Write unreachable hashes to this file (default: <repo>-gc-unreachable.txt)")
		tempDir      = flag.String("temp-dir", "", "Temp directory")
		manifestFile = flag.String("manifest", "", "Path to .cvmfspublished")
		spoolDir     = flag.String("spool-dir", "", "Spool directory for lock files (default: /var/spool/cvmfs/<repo>)")
		noLock       = flag.Bool("no-lock", false, "Skip repository locking (not recommended)")

		// Mirror mode
		doMirror    = flag.Bool("mirror", false, "Mirror a repo from a Stratum-1 instead of running GC")
		stratum1URL = flag.String("stratum1-url", "", "Stratum-1 base URL for mirroring")
		mirrorJobs  = flag.Int("mirror-jobs", 8, "Parallel downloads for mirroring")
	)
	flag.Parse()

	// Mirror mode: download a repo snapshot for testing.
	if *doMirror {
		if *stratum1URL == "" {
			log.Fatal("ERROR: -stratum1-url is required for -mirror")
		}
		if *repoDir == "" {
			log.Fatal("ERROR: -repo is required (local destination directory)")
		}
		mirrorCfg := mirror.Config{
			BaseURL:     *stratum1URL,
			LocalDir:    *repoDir,
			Parallelism: *mirrorJobs,
		}
		if _, err := mirror.Run(mirrorCfg); err != nil {
			log.Fatalf("ERROR: mirror failed: %v", err)
		}
		return
	}

	// GC mode: requires existing local repo.

	if *repoDir == "" {
		log.Fatal("ERROR: -repo is required")
	}

	dataDir := filepath.Join(*repoDir, "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		log.Fatalf("ERROR: data directory not found: %s", dataDir)
	}

	// ----------------------------------------------------------------
	// Repository locking (CVMFS-compatible flock)
	// ----------------------------------------------------------------
	if !*noLock {
		// Determine spool directory.
		if *spoolDir == "" {
			// Default: /var/spool/cvmfs/<basename_of_repo>
			*spoolDir = filepath.Join("/var/spool/cvmfs", filepath.Base(*repoDir))
		}
		log.Printf("Acquiring repository locks in %s ...", *spoolDir)
		if err := repoLocks.Acquire(*spoolDir); err != nil {
			log.Fatalf("ERROR: failed to lock repository: %v", err)
		}
		defer repoLocks.Release()
		log.Printf("Repository locked (GC + update locks acquired)")
	}

	// Determine root hash
	if *rootHash == "" && *manifestFile == "" {
		mfPath := filepath.Join(*repoDir, ".cvmfspublished")
		if _, err := os.Stat(mfPath); err == nil {
			*manifestFile = mfPath
		} else {
			log.Fatal("ERROR: -root-hash or -manifest required")
		}
	}

	var manifestHashes []catalog.Hash
	if *rootHash == "" && *manifestFile != "" {
		var err error
		*rootHash, manifestHashes, err = parseManifest(*manifestFile)
		if err != nil {
			log.Fatalf("ERROR: parsing manifest: %v", err)
		}
		log.Printf("Root catalog hash from manifest: %s", *rootHash)
		if len(manifestHashes) > 0 {
			log.Printf("Manifest-referenced objects: %d (history, certificate, metainfo)", len(manifestHashes))
		}
	}

	// Setup temp directory
	if *tempDir == "" {
		td, err := os.MkdirTemp("", "cvmfs-gc-*")
		if err != nil {
			log.Fatalf("ERROR: creating temp dir: %v", err)
		}
		*tempDir = td
		registerCleanup(*tempDir)
		defer runCleanup()
	} else {
		os.MkdirAll(*tempDir, 0755)
	}

	startTime := time.Now()

	// ----------------------------------------------------------------
	// Phase 1: Catalog traversal → semi-sort → chunk-sort (streamed)
	// ----------------------------------------------------------------
	log.Println("=== Phase 1: Traversing catalogs, semi-sorting, and chunk-sorting ===")
	phaseStart := time.Now()

	hashCh := make(chan catalog.Hash, 8192)

	cfg := catalog.TraverseConfig{
		DataDir:     dataDir,
		Parallelism: *parallelism,
		TempDir:     *tempDir,
	}

	var catProg catalog.Progress

	traverseErr := make(chan error, 1)
	go func() {
		traverseErr <- catalog.TraverseFromRootHash(cfg, *rootHash, hashCh, &catProg)
	}()

	// Convert catalog.Hash channel to string channel for semi-sorter.
	// Also inject manifest-referenced hashes (history, certificate, metainfo)
	// that aren't discovered through catalog traversal.
	stringCh := make(chan string, 8192)
	go func() {
		for _, mh := range manifestHashes {
			stringCh <- mh.String()
		}
		for h := range hashCh {
			stringCh <- h.String()
		}
		close(stringCh)
	}()

	// Semi-sorted stream: SemiSort reads stringCh and pushes the
	// semi-sorted output to semiCh. ChunkSort reads semiCh.
	semiCh := make(chan string, 8192)

	semiSortCfg := hashsort.SemiSortConfig{
		MaxHeapBytes: int64(*heapMB) * 1024 * 1024,
		HashSize:     41,
	}

	var hashesConsumed int64
	go func() {
		hashsort.SemiSort(semiSortCfg, stringCh, semiCh, &hashesConsumed)
	}()

	// ChunkSort runs in its own goroutine, reading the semi-sorted stream
	// and writing sorted+deduped chunk files to disk.
	chunkCfg := hashsort.ChunkSortConfig{
		ChunkBytes: int64(*chunkMB) * 1024 * 1024,
	}
	chunkDir := filepath.Join(*tempDir, "chunks")

	type chunkResult struct {
		files []string
		err   error
	}

	var chunksProduced int64
	chunkDone := make(chan chunkResult, 1)
	go func() {
		files, err := hashsort.ChunkSort(semiCh, chunkDir, chunkCfg, &chunksProduced)
		chunkDone <- chunkResult{files, err}
	}()

	// Progress ticker.
	p1Ticker := time.NewTicker(5 * time.Second)
	p1Done := make(chan struct{})
	go func() {
		for {
			select {
			case <-p1Ticker.C:
				log.Printf("  [Phase 1 progress] catalogs=%d  hashes_discovered=%d  semi_sorted=%d  chunks_written=%d  elapsed=%s",
					atomic.LoadInt64(&catProg.CatalogsProcessed),
					atomic.LoadInt64(&catProg.HashesEmitted),
					atomic.LoadInt64(&hashesConsumed),
					atomic.LoadInt64(&chunksProduced),
					time.Since(phaseStart).Truncate(time.Second))
			case <-p1Done:
				return
			}
		}
	}()

	// Wait for ChunkSort to finish (which implies SemiSort is done too,
	// since ChunkSort reads from the channel SemiSort writes to).
	cr := <-chunkDone
	chunkFiles := cr.files
	if cr.err != nil {
		log.Fatalf("ERROR: chunk sort failed: %v", cr.err)
	}

	if err := <-traverseErr; err != nil {
		log.Fatalf("ERROR: catalog traversal failed: %v", err)
	}

	p1Ticker.Stop()
	close(p1Done)

	hashCount, _ := hashsort.CountLinesMulti(chunkFiles)
	phase1Elapsed := time.Since(phaseStart)
	log.Printf("Phase 1 complete: %d catalogs, %d hashes, %d chunks, elapsed %s",
		atomic.LoadInt64(&catProg.CatalogsProcessed),
		atomic.LoadInt64(&hashesConsumed),
		len(chunkFiles), phase1Elapsed.Truncate(time.Millisecond))

	// ----------------------------------------------------------------
	// Phase 2: Sequential directory sweep with streaming merge
	// ----------------------------------------------------------------
	if !*doDelete {
		log.Println("=== Phase 2: Listing unreachable objects (use -delete to remove) ===")
	} else {
		log.Println("=== Phase 2: Sweeping unreachable objects ===")
	}
	phase2Start := time.Now()

	// Determine output file path for unreachable hashes.
	if *outputFile == "" {
		base := filepath.Base(*repoDir)
		*outputFile = base + "-gc-unreachable.txt"
	}
	outF, err := os.Create(*outputFile)
	if err != nil {
		log.Fatalf("ERROR: creating output file %s: %v", *outputFile, err)
	}
	outBuf := bufio.NewWriterSize(outF, 1024*1024)

	sweepCfg := sweep.Config{
		DataDir:      dataDir,
		ChunkFiles:   chunkFiles,
		DryRun:       !*doDelete,
		OutputWriter: outBuf,
	}

	// Pre-allocate Stats so the ticker can read atomic counters while
	// the sweep is running.
	var sweepStats sweep.Stats
	sweepDone := make(chan error, 1)
	go func() {
		_, e := sweep.Run(sweepCfg, &sweepStats)
		sweepDone <- e
	}()

	// Progress ticker for Phase 2.
	p2Ticker := time.NewTicker(5 * time.Second)
	func() {
		for {
			select {
			case <-p2Ticker.C:
				done := atomic.LoadInt64(&sweepStats.PrefixesDone)
				elapsed := time.Since(phase2Start)
				eta := "--"
				if done > 0 {
					remaining := time.Duration(float64(elapsed) * float64(256-done) / float64(done))
					eta = remaining.Truncate(time.Second).String()
				}
				log.Printf("  [Phase 2 progress] prefixes=%d/256  files_checked=%d  to_delete=%d  retained=%d  elapsed=%s  eta=%s",
					done,
					atomic.LoadInt64(&sweepStats.FilesChecked),
					atomic.LoadInt64(&sweepStats.FilesDeleted),
					atomic.LoadInt64(&sweepStats.FilesRetained),
					elapsed.Truncate(time.Second), eta)
			case err := <-sweepDone:
				if err != nil {
					log.Fatalf("ERROR: sweep failed: %v", err)
				}
				return
			}
		}
	}()
	p2Ticker.Stop()
	stats := &sweepStats

	// Flush and close the output file.
	if err := outBuf.Flush(); err != nil {
		log.Printf("WARNING: flushing output file: %v", err)
	}
	outF.Close()

	phase2Elapsed := time.Since(phase2Start)

	// Cleanup temp chunk files.
	for _, f := range chunkFiles {
		os.Remove(f)
	}
	os.Remove(chunkDir)

	// ----------------------------------------------------------------
	// Summary
	// ----------------------------------------------------------------
	totalElapsed := time.Since(startTime)
	log.Println("")
	log.Println("============================================")
	log.Println("  Garbage Collection Summary")
	log.Println("============================================")
	log.Printf("  Phase 1 (traverse + sort):   %s", phase1Elapsed.Truncate(time.Millisecond))
	log.Printf("    Sorted chunks:             %d", len(chunkFiles))
	log.Printf("    Unique reachable hashes:   %d", hashCount)
	log.Printf("  Phase 2 (sweep):             %s", phase2Elapsed.Truncate(time.Millisecond))
	log.Printf("    Files checked:             %d", stats.FilesChecked)
	log.Printf("    Files retained (reachable): %d", stats.FilesRetained)
	log.Printf("    Files to delete:           %d", stats.FilesDeleted)
	if stats.FilesChecked > 0 {
		pct := 100.0 * float64(stats.FilesDeleted) / float64(stats.FilesChecked)
		log.Printf("    Delete percentage:         %.1f%%", pct)
	}
	// Per-type breakdown of deletions.
	type suffixLabel struct {
		suffix byte
		label  string
	}
	suffixes := []suffixLabel{
		{0, "data (no suffix)"},
		{catalog.SuffixPartial, "partial chunks (P)"},
		{catalog.SuffixCatalog, "catalogs (C)"},
		{catalog.SuffixMicroCatalog, "micro-catalogs (L)"},
		{catalog.SuffixHistory, "history (H)"},
		{catalog.SuffixCertificate, "certificates (X)"},
		{catalog.SuffixMetainfo, "metainfo (M)"},
	}
	for _, sl := range suffixes {
		cnt := atomic.LoadInt64(&stats.DeletedBySuffix[sl.suffix])
		if cnt > 0 {
			typePct := 100.0 * float64(cnt) / float64(stats.FilesChecked)
			log.Printf("      %-24s %d (%.1f%% of total)", sl.label, cnt, typePct)
		}
	}
	log.Printf("    Unreachable hashes file:   %s", *outputFile)
	log.Printf("    Errors:                    %d", stats.Errors)
	if *doDelete && stats.BytesFreed > 0 {
		log.Printf("    Space freed:               %s", humanBytes(stats.BytesFreed))
	}
	log.Println("  ──────────────────────────────────────────")
	log.Printf("  Total elapsed:               %s", totalElapsed.Truncate(time.Millisecond))
	if !*doDelete {
		log.Println("")
		log.Println("  ** No files were deleted (pass -delete to actually remove) **")
	}
	log.Println("============================================")
}

// parseManifest reads a .cvmfspublished file and extracts:
//   - the root catalog hash (C line)
//   - any additional reachable object hashes: history (H), certificate (X),
//     metainfo (M)
//
// Manifest line format:
//
//	C<hash>  - root catalog content hash
//	B<size>  - catalog size
//	R<md5>   - root path hash
//	H<hash>  - history database hash
//	X<hash>  - certificate hash
//	M<hash>  - metainfo hash
//	S<rev>   - revision number
//	G<bool>  - garbage-collectable flag
//	N<name>  - repository name
func parseManifest(path string) (string, []catalog.Hash, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", nil, err
	}

	var rootHash string
	var extra []catalog.Hash

	for _, line := range splitLines(string(data)) {
		if len(line) < 2 {
			continue
		}
		hash := line[1:]
		switch line[0] {
		case 'C':
			rootHash = hash
		case 'H':
			extra = append(extra, catalog.Hash{Hex: hash, Suffix: catalog.SuffixHistory})
		case 'X':
			extra = append(extra, catalog.Hash{Hex: hash, Suffix: catalog.SuffixCertificate})
		case 'M':
			extra = append(extra, catalog.Hash{Hex: hash, Suffix: catalog.SuffixMetainfo})
		}
	}

	if rootHash == "" {
		return "", nil, fmt.Errorf("root catalog hash (C line) not found in manifest")
	}

	return rootHash, extra, nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			lines = append(lines, line)
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
