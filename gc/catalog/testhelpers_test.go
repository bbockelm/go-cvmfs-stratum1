package catalog

import (
	"database/sql"
	"encoding/hex"
	"testing"

	_ "modernc.org/sqlite"
)

// createTestDB creates an in-memory SQLite database with the CVMFS catalog
// schema and some test data.
func createTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening in-memory db: %v", err)
	}

	schema := `
		CREATE TABLE properties (key TEXT, value TEXT);
		INSERT INTO properties VALUES ('schema', '2.5');

		CREATE TABLE catalog (
			md5path_1 INTEGER,
			md5path_2 INTEGER,
			parent_1 INTEGER,
			parent_2 INTEGER,
			hardlinks INTEGER,
			hash BLOB,
			size INTEGER,
			mode INTEGER,
			mtime INTEGER,
			flags INTEGER,
			name TEXT,
			symlink TEXT,
			uid INTEGER,
			gid INTEGER,
			xattr BLOB
		);

		CREATE TABLE chunks (
			md5path_1 INTEGER,
			md5path_2 INTEGER,
			offset INTEGER,
			size INTEGER,
			hash BLOB
		);

		CREATE TABLE nested_catalogs (
			path TEXT,
			sha1 TEXT,
			size INTEGER
		);
	`

	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("creating schema: %v", err)
	}

	// Insert test data
	// Regular file (flags=4 = file)
	file1Hash, _ := hex.DecodeString("aabbccdd00112233aabbccdd00112233aabbccdd")
	_, err = db.Exec(
		"INSERT INTO catalog VALUES (1, 2, 0, 0, 1, ?, 100, 33188, 1000, 4, 'file1.txt', '', 0, 0, NULL)",
		file1Hash,
	)
	if err != nil {
		t.Fatalf("inserting file1: %v", err)
	}

	// External file (flags = 4 | 128 = 132) -- should be excluded
	extHash, _ := hex.DecodeString("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	_, err = db.Exec(
		"INSERT INTO catalog VALUES (3, 4, 0, 0, 1, ?, 200, 33188, 1000, 132, 'external.dat', '', 0, 0, NULL)",
		extHash,
	)
	if err != nil {
		t.Fatalf("inserting external file: %v", err)
	}

	// Directory (flags=1)
	_, err = db.Exec(
		"INSERT INTO catalog VALUES (5, 6, 0, 0, 1, NULL, 0, 16384, 1000, 1, 'subdir', '', 0, 0, NULL)",
	)
	if err != nil {
		t.Fatalf("inserting directory: %v", err)
	}

	// Directory with hash (flags=1) — should get SuffixMicroCatalog
	dirHash, _ := hex.DecodeString("99aabb00112233445566778899aabb0011223344")
	_, err = db.Exec(
		"INSERT INTO catalog VALUES (9, 10, 0, 0, 1, ?, 0, 16384, 1000, 1, 'nested', '', 0, 0, NULL)",
		dirHash,
	)
	if err != nil {
		t.Fatalf("inserting directory with hash: %v", err)
	}

	// Chunked file (flags = 4 | 64 = 68)
	chunkedFileHash, _ := hex.DecodeString("5566778899001122334455667788990011223344")
	_, err = db.Exec(
		"INSERT INTO catalog VALUES (7, 8, 0, 0, 1, ?, 5000, 33188, 1000, 68, 'big.dat', '', 0, 0, NULL)",
		chunkedFileHash,
	)
	if err != nil {
		t.Fatalf("inserting chunked file: %v", err)
	}

	// Chunk for the chunked file
	chunkHash, _ := hex.DecodeString("11223344556677889900aabbccddeeff11223344")
	_, err = db.Exec(
		"INSERT INTO chunks VALUES (7, 8, 0, 2500, ?)",
		chunkHash,
	)
	if err != nil {
		t.Fatalf("inserting chunk: %v", err)
	}

	chunk2Hash, _ := hex.DecodeString("ffeeddccbbaa99887766ffeeddccbbaa99887766")
	_, err = db.Exec(
		"INSERT INTO chunks VALUES (7, 8, 2500, 2500, ?)",
		chunk2Hash,
	)
	if err != nil {
		t.Fatalf("inserting chunk2: %v", err)
	}

	return db
}

