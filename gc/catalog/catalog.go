// Package catalog implements parallel traversal of CVMFS SQLite catalog trees.
package catalog

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite"
)

// Progress holds atomic counters that callers can read to monitor traversal.
type Progress struct {
	CatalogsProcessed int64 // catalogs fully processed
	HashesEmitted     int64 // content hashes sent to output channel
}

// Hash represents a content-addressable hash with its suffix character.
type Hash struct {
	Hex    string // full hex-encoded hash
	Suffix byte   // hash suffix character (0 for none)
}

// String returns the hash with its suffix appended (if any).
func (h Hash) String() string {
	if h.Suffix == 0 {
		return h.Hex
	}
	return h.Hex + string(h.Suffix)
}

// ObjectPath returns the relative path under the data/ directory for this hash.
// CVMFS uses 1 directory level with 2 hex digits: ab/cdef01234...
func (h Hash) ObjectPath() string {
	if len(h.Hex) < 3 {
		return h.Hex
	}
	name := h.Hex[2:]
	if h.Suffix != 0 {
		name += string(h.Suffix)
	}
	return h.Hex[:2] + "/" + name
}

const (
	flagFile         = 4
	flagDir          = 1
	flagFileExternal = 128
)

// Hash suffix constants used by CVMFS content-addressable storage.
// Exported so that manifest parsing can construct Hash values.
const (
	SuffixNone         byte = 0
	SuffixCatalog      byte = 'C'
	SuffixPartial      byte = 'P'
	SuffixMicroCatalog byte = 'L'
	SuffixHistory      byte = 'H'
	SuffixCertificate  byte = 'X'
	SuffixMetainfo     byte = 'M'
)

// TraverseConfig holds configuration for a parallel catalog traversal.
type TraverseConfig struct {
	// DataDir is the path to the repository data directory.
	DataDir string
	// Parallelism controls how many catalogs are processed concurrently.
	Parallelism int
	// TempDir is where catalog SQLite files are temporarily extracted.
	TempDir string
}

// TraverseFromRootHash starts a parallel traversal from the given root
// catalog hash. It sends every discovered reachable content hash to the
// output channel. The channel is closed when traversal is complete.
//
// If prog is non-nil, atomic counters are updated as work proceeds so
// that callers can report progress from a ticker goroutine.
//
// Internally, a dispatcher goroutine owns an unbounded work queue so that
// worker goroutines never block when enqueuing newly-discovered nested
// catalogs. Without this, all workers can deadlock trying to send on a
// full catalogCh while none is free to receive.
func TraverseFromRootHash(cfg TraverseConfig, rootHash string, out chan<- Hash, prog *Progress) error {
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 8
	}

	// The root catalog itself is a reachable object.
	out <- Hash{Hex: rootHash, Suffix: SuffixCatalog}

	// foundCh: workers send newly-discovered nested catalog hashes here.
	// catalogCh: the dispatcher sends catalogs to workers for processing.
	foundCh := make(chan []Hash, cfg.Parallelism)
	catalogCh := make(chan Hash, cfg.Parallelism)
	errCh := make(chan error, 1)

	// doneCh is closed by the dispatcher when all work is finished.
	doneCh := make(chan struct{})

	// ---------- dispatcher ----------
	// Holds an unbounded slice-based queue. Receives discovered nested
	// catalogs from workers (foundCh) and feeds them to catalogCh.
	// Tracks in-flight catalogs; when none remain and the queue is empty,
	// closes catalogCh so workers exit.
	go func() {
		var queue []Hash
		queue = append(queue, Hash{Hex: rootHash, Suffix: SuffixCatalog})
		inFlight := 0

		for {
			// Determine what we can send next.
			var sendCh chan Hash
			var sendVal Hash
			if len(queue) > 0 {
				sendCh = catalogCh
				sendVal = queue[0]
			}

			// If nothing in queue and nothing in flight, we're done.
			if len(queue) == 0 && inFlight == 0 {
				close(catalogCh)
				close(doneCh)
				return
			}

			select {
			case sendCh <- sendVal:
				queue = queue[1:]
				inFlight++
			case nested, ok := <-foundCh:
				if !ok {
					// Should not happen, but be safe.
					continue
				}
				// nil slice signals a worker finished one catalog.
				if nested == nil {
					inFlight--
				} else {
					queue = append(queue, nested...)
				}
			}
		}
	}()

	// ---------- workers ----------
	var workerWg sync.WaitGroup
	for i := 0; i < cfg.Parallelism; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for catHash := range catalogCh {
				nested, nHashes, err := processCatalog(cfg, catHash, out)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("catalog %s: %w", catHash.Hex, err):
					default:
					}
				}
				if prog != nil {
					atomic.AddInt64(&prog.CatalogsProcessed, 1)
					atomic.AddInt64(&prog.HashesEmitted, nHashes)
				}
				if len(nested) > 0 {
					foundCh <- nested
				}
				// Signal this catalog is done (nil = completion marker).
				foundCh <- nil
			}
		}()
	}

	// Wait for all workers to finish, then close out.
	go func() {
		<-doneCh
		workerWg.Wait()
		close(out)
	}()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// processCatalog opens a single catalog, extracts all content hashes
// (sent to out), and returns any nested catalog hashes for the dispatcher.
// The second return value is the number of hashes sent to out.
func processCatalog(cfg TraverseConfig, catHash Hash, out chan<- Hash) ([]Hash, int64, error) {
	catPath := filepath.Join(cfg.DataDir, catHash.ObjectPath())

	dbPath, cleanup, err := prepareCatalogDB(cfg, catPath)
	if err != nil {
		return nil, 0, fmt.Errorf("preparing catalog %s: %w", catHash.Hex, err)
	}
	defer cleanup()

	db, err := sql.Open("sqlite", dbPath+"?mode=ro&_journal_mode=OFF&_synchronous=OFF")
	if err != nil {
		return nil, 0, fmt.Errorf("opening catalog db %s: %w", dbPath, err)
	}
	defer db.Close()

	// Extract schema version to adapt queries
	schemaVersion := 2.5
	row := db.QueryRow("SELECT value FROM properties WHERE key='schema'")
	if row != nil {
		_ = row.Scan(&schemaVersion)
	}

	var nHashes int64
	if err := extractContentHashes(db, schemaVersion, out, &nHashes); err != nil {
		return nil, nHashes, err
	}

	nested, err := collectNestedCatalogs(db, out, &nHashes)
	if err != nil {
		return nil, nHashes, err
	}

	return nested, nHashes, nil
}

// extractContentHashes queries a catalog database for all content hashes
// (regular files and file chunks) and sends them to the output channel.
func extractContentHashes(db *sql.DB, schemaVersion float64, out chan<- Hash, count *int64) error {
	rows, err := db.Query(
		"SELECT hash, flags FROM catalog " +
			"WHERE length(hash) > 0 AND (flags & 128) = 0")
	if err != nil {
		return fmt.Errorf("querying catalog hashes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var hashBlob []byte
		var flags int
		if err := rows.Scan(&hashBlob, &flags); err != nil {
			return fmt.Errorf("scanning catalog row: %w", err)
		}
		if len(hashBlob) == 0 {
			continue
		}

		hexStr := hex.EncodeToString(hashBlob)

		var suffix byte = SuffixNone
		if flags&flagDir != 0 && flags&flagFile == 0 {
			suffix = SuffixMicroCatalog
		}

		out <- Hash{Hex: hexStr, Suffix: suffix}
		*count++
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Chunk hashes (for chunked files)
	if schemaVersion >= 2.4 {
		chunkRows, err := db.Query(
			"SELECT chunks.hash, catalog.flags FROM chunks " +
				"JOIN catalog ON chunks.md5path_1 = catalog.md5path_1 " +
				"AND chunks.md5path_2 = catalog.md5path_2 " +
				"WHERE (catalog.flags & 128) = 0")
		if err != nil {
			return fmt.Errorf("querying chunk hashes: %w", err)
		}
		defer chunkRows.Close()

		for chunkRows.Next() {
			var hashBlob []byte
			var flags int
			if err := chunkRows.Scan(&hashBlob, &flags); err != nil {
				return fmt.Errorf("scanning chunk row: %w", err)
			}
			if len(hashBlob) == 0 {
				continue
			}
			out <- Hash{Hex: hex.EncodeToString(hashBlob), Suffix: SuffixPartial}
			*count++
		}
		if err := chunkRows.Err(); err != nil {
			return err
		}
	}

	return nil
}

// collectNestedCatalogs reads the nested_catalogs table, sends each
// nested catalog hash to the output channel (marking it as reachable),
// and returns the list so the dispatcher can schedule processing.
func collectNestedCatalogs(db *sql.DB, out chan<- Hash, count *int64) ([]Hash, error) {
	rows, err := db.Query("SELECT sha1 FROM nested_catalogs")
	if err != nil {
		return nil, fmt.Errorf("querying nested catalogs: %w", err)
	}
	defer rows.Close()

	var nested []Hash
	for rows.Next() {
		var hashStr string
		if err := rows.Scan(&hashStr); err != nil {
			return nil, fmt.Errorf("scanning nested catalog: %w", err)
		}
		if hashStr == "" {
			continue
		}

		h := Hash{Hex: hashStr, Suffix: SuffixCatalog}
		out <- h
		*count++
		nested = append(nested, h)
	}
	return nested, rows.Err()
}

// prepareCatalogDB makes a catalog SQLite file available for reading.
// Checks if it is a valid SQLite file; if not, tries zlib decompression.
func prepareCatalogDB(cfg TraverseConfig, catPath string) (string, func(), error) {
	noop := func() {}

	if _, err := os.Stat(catPath); os.IsNotExist(err) {
		return "", noop, fmt.Errorf("catalog file not found: %s", catPath)
	}

	f, err := os.Open(catPath)
	if err != nil {
		return "", noop, err
	}

	magic := make([]byte, 16)
	n, err := f.Read(magic)
	f.Close()
	if err != nil {
		return "", noop, err
	}

	// SQLite files start with "SQLite format 3\000"
	if n >= 16 && string(magic[:16]) == "SQLite format 3\000" {
		return catPath, noop, nil
	}

	// Not a plain SQLite file -- try zlib decompression
	tmpDir := cfg.TempDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}

	tmpFile, err := os.CreateTemp(tmpDir, "cvmfs-catalog-*.sqlite")
	if err != nil {
		return "", noop, fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	cleanup := func() {
		os.Remove(tmpPath)
	}

	if err := decompressZlib(catPath, tmpFile); err != nil {
		tmpFile.Close()
		cleanup()
		return "", noop, fmt.Errorf("decompressing catalog: %w", err)
	}
	tmpFile.Close()

	return tmpPath, cleanup, nil
}

// decompressZlib decompresses a zlib-compressed file to the given writer.
func decompressZlib(src string, dst *os.File) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	return decompressZlibStream(srcFile, dst)
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
