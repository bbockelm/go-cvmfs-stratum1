// Package mirror downloads a CVMFS repository snapshot from a Stratum-1
// server.  It fetches the manifest, recursively downloads all catalogs,
// extracts every content hash, and downloads all referenced data objects.
package mirror

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bbockelm/cvmfs-gc-optim/gc/catalog"
	_ "modernc.org/sqlite"
)

// Config controls a mirror operation.
type Config struct {
	BaseURL     string // Stratum-1 base URL (e.g. http://host:8000/cvmfs/repo)
	LocalDir    string // local destination directory
	Parallelism int    // parallel downloads for content objects
}

// Stats reports what was downloaded.
type Stats struct {
	Catalogs      int
	ContentHashes int
	Downloaded    int64
	Skipped       int64
	Failed        int64
	TotalBytes    int64
}

// Run mirrors a CVMFS repository into cfg.LocalDir.
func Run(cfg Config) (*Stats, error) {
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 8
	}
	dataDir := filepath.Join(cfg.LocalDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}

	st := &Stats{}

	// ------------------------------------------------------------------
	// Step 1: fetch manifest
	// ------------------------------------------------------------------
	log.Println("=== Mirror: fetching manifest ===")
	manifestData, err := httpGet(cfg.BaseURL + "/.cvmfspublished")
	if err != nil {
		return nil, fmt.Errorf("fetching manifest: %w", err)
	}
	mfPath := filepath.Join(cfg.LocalDir, ".cvmfspublished")
	if err := os.WriteFile(mfPath, manifestData, 0644); err != nil {
		return nil, fmt.Errorf("writing manifest: %w", err)
	}

	fields := parseManifestFields(manifestData)
	rootHash := fields['C']
	if rootHash == "" {
		return nil, fmt.Errorf("no root catalog hash (C) in manifest")
	}
	log.Printf("  Root catalog: %s", rootHash)
	log.Printf("  Repo:         %s", fields['N'])
	log.Printf("  Revision:     %s", fields['S'])

	// ------------------------------------------------------------------
	// Step 2: BFS catalog traversal — download + parse each catalog
	// ------------------------------------------------------------------
	log.Println("=== Mirror: downloading catalogs ===")

	type catalogWork struct {
		hash string
	}
	queue := []catalogWork{{hash: rootHash}}
	visited := map[string]bool{rootHash: true}
	allContentHashes := make(map[string]struct{})

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		st.Catalogs++
		log.Printf("  Catalog %d: %s...", st.Catalogs, cur.hash[:min(16, len(cur.hash))])

		if err := downloadObject(cfg.BaseURL, dataDir, cur.hash, string(catalog.SuffixCatalog)); err != nil {
			log.Printf("    WARNING: %v", err)
			continue
		}

		contentHashes, nestedHashes, err := parseCatalogFile(dataDir, cur.hash)
		if err != nil {
			log.Printf("    WARNING: parsing catalog: %v", err)
			continue
		}
		log.Printf("    %d content hashes, %d nested catalogs", len(contentHashes), len(nestedHashes))

		for _, h := range contentHashes {
			allContentHashes[h] = struct{}{}
		}
		for _, nh := range nestedHashes {
			if !visited[nh] {
				visited[nh] = true
				queue = append(queue, catalogWork{hash: nh})
			}
		}
	}

	st.ContentHashes = len(allContentHashes)
	log.Printf("  Total: %d catalogs, %d unique content hashes", st.Catalogs, st.ContentHashes)

	// ------------------------------------------------------------------
	// Step 3: download manifest-referenced objects (H, X, M)
	// ------------------------------------------------------------------
	type suffixHash struct {
		hash   string
		suffix string
	}
	var extraObjects []suffixHash
	for _, pair := range []struct {
		key    byte
		suffix byte
	}{
		{'H', catalog.SuffixHistory},
		{'X', catalog.SuffixCertificate},
		{'M', catalog.SuffixMetainfo},
	} {
		if h := fields[pair.key]; h != "" {
			extraObjects = append(extraObjects, suffixHash{hash: h, suffix: string(pair.suffix)})
		}
	}
	for _, obj := range extraObjects {
		if err := downloadObject(cfg.BaseURL, dataDir, obj.hash, obj.suffix); err != nil {
			log.Printf("  WARNING: manifest object %s%s: %v", obj.hash[:16], obj.suffix, err)
		}
	}

	// ------------------------------------------------------------------
	// Step 4: parallel download of all content objects
	// ------------------------------------------------------------------
	log.Println("=== Mirror: downloading content objects ===")

	// Build a slice so we can feed a channel.
	hashList := make([]string, 0, len(allContentHashes))
	for h := range allContentHashes {
		hashList = append(hashList, h)
	}

	var downloaded, failed atomic.Int64

	work := make(chan string, cfg.Parallelism*2)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range work {
				if err := downloadObject(cfg.BaseURL, dataDir, h, ""); err != nil {
					failed.Add(1)
				} else {
					// Check if it was a skip or a real download —
					// we count all as "downloaded" for simplicity.
					downloaded.Add(1)
				}
			}
		}()
	}
	startT := time.Now()
	for i, h := range hashList {
		if (i+1)%500 == 0 {
			log.Printf("  Progress: %d/%d (%.0f/s)", i+1, len(hashList),
				float64(i+1)/time.Since(startT).Seconds())
		}
		work <- h
	}
	close(work)
	wg.Wait()

	st.Downloaded = downloaded.Load()
	st.Failed = failed.Load()

	// ------------------------------------------------------------------
	// Step 5: summary
	// ------------------------------------------------------------------
	var totalFiles int64
	var totalSize int64
	entries, _ := os.ReadDir(dataDir)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sub, _ := os.ReadDir(filepath.Join(dataDir, e.Name()))
		totalFiles += int64(len(sub))
		for _, s := range sub {
			if info, err := s.Info(); err == nil {
				totalSize += info.Size()
			}
		}
	}
	st.TotalBytes = totalSize

	log.Println("=== Mirror complete ===")
	log.Printf("  Local path:     %s", cfg.LocalDir)
	log.Printf("  Data files:     %d", totalFiles)
	log.Printf("  Total size:     %.1f MB", float64(totalSize)/1024/1024)
	log.Printf("  Catalogs:       %d", st.Catalogs)
	log.Printf("  Content hashes: %d", st.ContentHashes)
	log.Printf("  Downloaded:     %d, Failed: %d", st.Downloaded, st.Failed)
	log.Println()
	log.Printf("To test GC:")
	log.Printf("  go run ./cmd/cvmfs-gc -repo %s -dry-run", cfg.LocalDir)

	return st, nil
}

// --------------- helpers ------------------------------------------------

func httpGet(url string) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d for %s", resp.StatusCode, url)
	}
	return io.ReadAll(resp.Body)
}

// downloadObject fetches data/<prefix>/<rest><suffix> from the Stratum-1
// into the local data directory.  It is a no-op if the file already exists.
func downloadObject(baseURL, dataDir, hexHash, suffix string) error {
	if len(hexHash) < 3 {
		return fmt.Errorf("hash too short: %s", hexHash)
	}
	rel := hexHash[:2] + "/" + hexHash[2:] + suffix
	localPath := filepath.Join(dataDir, hexHash[:2], hexHash[2:]+suffix)

	if _, err := os.Stat(localPath); err == nil {
		return nil // already have it
	}

	url := baseURL + "/data/" + rel
	data, err := httpGet(url)
	if err != nil {
		return fmt.Errorf("downloading %s: %w", url, err)
	}

	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return os.WriteFile(localPath, data, 0644)
}

// parseCatalogFile decompresses a downloaded catalog and extracts its
// content hashes and nested catalog hashes from the SQLite database.
func parseCatalogFile(dataDir, hexHash string) (content []string, nested []string, err error) {
	catPath := filepath.Join(dataDir, hexHash[:2], hexHash[2:]+string(catalog.SuffixCatalog))
	compressed, err := os.ReadFile(catPath)
	if err != nil {
		return nil, nil, err
	}

	dbBytes, err := decompressMaybe(compressed)
	if err != nil {
		return nil, nil, fmt.Errorf("decompress: %w", err)
	}

	// Write to a temp file so SQLite can open it.
	tmp, err := os.CreateTemp("", "cvmfs-cat-*.sqlite")
	if err != nil {
		return nil, nil, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(dbBytes); err != nil {
		tmp.Close()
		return nil, nil, err
	}
	tmp.Close()

	db, err := sql.Open("sqlite", tmpPath)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()

	// Content hashes from the catalog table.
	rows, err := db.Query("SELECT hex(hash), flags FROM catalog WHERE hash IS NOT NULL")
	if err != nil {
		return nil, nil, fmt.Errorf("query catalog: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var hex string
		var flags int
		if err := rows.Scan(&hex, &flags); err != nil {
			continue
		}
		hex = strings.ToLower(hex)
		content = append(content, hex)
	}

	// Chunk hashes (if present).
	chunkRows, err := db.Query("SELECT hex(hash) FROM chunks WHERE hash IS NOT NULL")
	if err == nil {
		defer chunkRows.Close()
		for chunkRows.Next() {
			var hex string
			if err := chunkRows.Scan(&hex); err != nil {
				continue
			}
			content = append(content, strings.ToLower(hex))
		}
	}

	// Nested catalogs.
	nestedRows, err := db.Query("SELECT hex(sha1) FROM nested_catalogs WHERE sha1 IS NOT NULL")
	if err == nil {
		defer nestedRows.Close()
		for nestedRows.Next() {
			var hex string
			if err := nestedRows.Scan(&hex); err != nil {
				continue
			}
			nested = append(nested, strings.ToLower(hex))
		}
	}

	return content, nested, nil
}

// decompressMaybe tries zlib decompression; falls back to raw data.
func decompressMaybe(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		// Not zlib — check if it looks like raw SQLite.
		if len(data) >= 16 && string(data[:6]) == "SQLite" {
			return data, nil
		}
		return nil, fmt.Errorf("not zlib and not SQLite: %w", err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

// parseManifestFields parses .cvmfspublished into a key→value map.
// Each line is key-char + value, separated from the signature by "--".
func parseManifestFields(data []byte) map[byte]string {
	fields := make(map[byte]string)
	text := string(data)
	if idx := strings.Index(text, "--"); idx >= 0 {
		text = text[:idx]
	}
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		line = strings.TrimRight(line, "\r")
		if len(line) >= 2 {
			fields[line[0]] = line[1:]
		}
	}
	return fields
}
