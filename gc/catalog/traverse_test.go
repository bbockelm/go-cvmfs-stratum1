package catalog

import (
	"compress/zlib"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// writeCatalogDB creates a zlib-compressed SQLite catalog file at
// dataDir/<prefix>/<rest>C with the given content and nested catalog
// references. Returns the hex hash used.
func writeCatalogDB(t *testing.T, dataDir string, hexHash string, contentHashes []string, nestedHexHashes []string) {
	t.Helper()

	// Create an uncompressed SQLite file in a temp location.
	tmpDB, err := os.CreateTemp(t.TempDir(), "cat-*.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	tmpPath := tmpDB.Name()
	tmpDB.Close()
	defer os.Remove(tmpPath)

	db, err := sql.Open("sqlite", tmpPath)
	if err != nil {
		t.Fatal(err)
	}

	schema := `
		CREATE TABLE properties (key TEXT, value TEXT);
		INSERT INTO properties VALUES ('schema', '2.5');
		CREATE TABLE catalog (
			md5path_1 INTEGER, md5path_2 INTEGER,
			parent_1 INTEGER, parent_2 INTEGER,
			hardlinks INTEGER, hash BLOB, size INTEGER,
			mode INTEGER, mtime INTEGER, flags INTEGER,
			name TEXT, symlink TEXT, uid INTEGER, gid INTEGER, xattr BLOB
		);
		CREATE TABLE chunks (
			md5path_1 INTEGER, md5path_2 INTEGER,
			offset INTEGER, size INTEGER, hash BLOB
		);
		CREATE TABLE nested_catalogs (path TEXT, sha1 TEXT, size INTEGER);
	`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("schema: %v", err)
	}

	for i, ch := range contentHashes {
		blob, _ := hex.DecodeString(ch)
		_, err := db.Exec(
			"INSERT INTO catalog VALUES (?, ?, 0, 0, 1, ?, 100, 33188, 1000, 4, ?, '', 0, 0, NULL)",
			i+1, i+1, blob, fmt.Sprintf("file%d.txt", i),
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, nh := range nestedHexHashes {
		_, err := db.Exec(
			"INSERT INTO nested_catalogs VALUES (?, ?, 0)",
			fmt.Sprintf("/nested/%d", i), nh,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	db.Close()

	// Read the raw SQLite file and zlib-compress it into the data dir.
	raw, err := os.ReadFile(tmpPath)
	if err != nil {
		t.Fatal(err)
	}

	objPath := filepath.Join(dataDir, hexHash[:2], hexHash[2:]+"C")
	if err := os.MkdirAll(filepath.Dir(objPath), 0755); err != nil {
		t.Fatal(err)
	}

	outFile, err := os.Create(objPath)
	if err != nil {
		t.Fatal(err)
	}
	w := zlib.NewWriter(outFile)
	if _, err := w.Write(raw); err != nil {
		t.Fatal(err)
	}
	w.Close()
	outFile.Close()
}

// TestTraverseNoDeadlock creates a catalog tree where the root catalog
// has more nested catalogs than the worker pool size. With the old
// WaitGroup-based approach, all workers would block trying to enqueue
// nested catalogs onto a full channel, causing a deadlock. The
// dispatcher-based approach uses an unbounded queue, preventing this.
func TestTraverseNoDeadlock(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	tempDir := t.TempDir()

	// Use parallelism=2 with 10 nested catalogs.
	// Old code: catalogCh buffer = 2*2 = 4, so after the 2 workers
	// each enqueue 2 catalogs, the channel is full and both block.
	const numNested = 10
	const parallelism = 2

	rootHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	// Generate nested catalog hashes and leaf content hashes.
	nestedHashes := make([]string, numNested)
	for i := range nestedHashes {
		nestedHashes[i] = fmt.Sprintf("%040x", i+1)
	}
	leafContent := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	// Write root catalog: no content, many nested.
	writeCatalogDB(t, dataDir, rootHash, nil, nestedHashes)

	// Write each nested catalog: one content hash, no further nesting.
	for _, nh := range nestedHashes {
		writeCatalogDB(t, dataDir, nh, []string{leafContent}, nil)
	}

	cfg := TraverseConfig{
		DataDir:     dataDir,
		Parallelism: parallelism,
		TempDir:     tempDir,
	}

	out := make(chan Hash, 1024)

	done := make(chan error, 1)
	go func() {
		done <- TraverseFromRootHash(cfg, rootHash, out, nil)
	}()

	// Drain with a timeout to detect deadlock.
	var hashes []Hash
	timeout := time.After(10 * time.Second)
	draining := true
	for draining {
		select {
		case h, ok := <-out:
			if !ok {
				draining = false
			} else {
				hashes = append(hashes, h)
			}
		case <-timeout:
			t.Fatal("DEADLOCK: TraverseFromRootHash did not complete within 10 seconds")
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("traversal error: %v", err)
	}

	// Expect:
	//   1 root catalog hash (from the initial out <- in TraverseFromRootHash)
	//   10 nested catalog hashes (from collectNestedCatalogs)
	//   10 content hashes (leafContent, one per nested catalog)
	// = 21 total
	expected := 1 + numNested + numNested
	if len(hashes) != expected {
		t.Errorf("got %d hashes, want %d", len(hashes), expected)
	}

	// Verify all nested catalog hashes appear.
	seen := make(map[string]bool)
	for _, h := range hashes {
		seen[h.Hex] = true
	}
	for _, nh := range nestedHashes {
		if !seen[nh] {
			t.Errorf("missing nested catalog hash: %s", nh)
		}
	}

	t.Logf("Traversal complete: %d hashes from root + %d nested catalogs (parallelism=%d)",
		len(hashes), numNested, parallelism)
}

// TestTraverseDeepTree tests a deep catalog tree (depth=5, branching=3)
// to verify the dispatcher handles multi-level nesting correctly.
func TestTraverseDeepTree(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	tempDir := t.TempDir()

	const depth = 4
	const branching = 3
	const parallelism = 2

	rootHash := "cccccccccccccccccccccccccccccccccccccccc"

	// Build tree level by level. hashCounter generates unique hashes.
	hashCounter := 0x100
	nextHash := func() string {
		hashCounter++
		return fmt.Sprintf("%040x", hashCounter)
	}

	type node struct {
		hash     string
		children []string
	}

	// BFS to build the tree.
	currentLevel := []node{{hash: rootHash}}
	var allNodes []node

	for d := 0; d < depth; d++ {
		var nextLevel []node
		for i := range currentLevel {
			var children []string
			if d < depth-1 {
				for b := 0; b < branching; b++ {
					ch := nextHash()
					children = append(children, ch)
					nextLevel = append(nextLevel, node{hash: ch})
				}
			}
			currentLevel[i].children = children
		}
		allNodes = append(allNodes, currentLevel...)
		currentLevel = nextLevel
	}
	// Add leaf nodes (last level has no children).
	allNodes = append(allNodes, currentLevel...)

	contentHash := "dddddddddddddddddddddddddddddddddddddddd"[:40]

	for _, n := range allNodes {
		writeCatalogDB(t, dataDir, n.hash, []string{contentHash}, n.children)
	}

	cfg := TraverseConfig{
		DataDir:     dataDir,
		Parallelism: parallelism,
		TempDir:     tempDir,
	}

	out := make(chan Hash, 4096)
	done := make(chan error, 1)
	go func() {
		done <- TraverseFromRootHash(cfg, rootHash, out, nil)
	}()

	var hashes []Hash
	timeout := time.After(30 * time.Second)
	draining := true
	for draining {
		select {
		case h, ok := <-out:
			if !ok {
				draining = false
			} else {
				hashes = append(hashes, h)
			}
		case <-timeout:
			t.Fatal("DEADLOCK: deep tree traversal did not complete within 30 seconds")
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("traversal error: %v", err)
	}

	// Total catalogs = 1 + 3 + 9 + 27 = 40  (depth=4, branching=3)
	// Each catalog has 1 content hash → 40 content hashes.
	// Each catalog hash appears twice in out: once from TraverseFromRootHash/
	// collectNestedCatalogs, so 40 catalog hashes.
	// Total = 40 + 40 = 80.
	totalNodes := len(allNodes)
	expectedHashes := totalNodes + totalNodes // catalog hashes + content hashes
	if len(hashes) != expectedHashes {
		t.Errorf("got %d hashes, want %d (nodes=%d)", len(hashes), expectedHashes, totalNodes)
	}

	t.Logf("Deep tree traversal complete: %d hashes, %d catalog nodes (parallelism=%d)",
		len(hashes), totalNodes, parallelism)
}
