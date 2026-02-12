package catalog

import (
	"testing"
)

func TestHashString(t *testing.T) {
	cases := []struct {
		hex    string
		suffix byte
		want   string
	}{
		{"abcdef0123456789abcdef0123456789abcdef01", 0, "abcdef0123456789abcdef0123456789abcdef01"},
		{"abcdef0123456789abcdef0123456789abcdef01", 'C', "abcdef0123456789abcdef0123456789abcdef01C"},
		{"abcdef0123456789abcdef0123456789abcdef01", 'P', "abcdef0123456789abcdef0123456789abcdef01P"},
	}
	for _, tc := range cases {
		h := Hash{Hex: tc.hex, Suffix: tc.suffix}
		got := h.String()
		if got != tc.want {
			t.Errorf("Hash{%q, %c}.String() = %q, want %q", tc.hex, tc.suffix, got, tc.want)
		}
	}
}

func TestHashObjectPath(t *testing.T) {
	cases := []struct {
		hex    string
		suffix byte
		want   string
	}{
		{"abcdef0123456789abcdef0123456789abcdef01", 0, "ab/cdef0123456789abcdef0123456789abcdef01"},
		{"abcdef0123456789abcdef0123456789abcdef01", 'C', "ab/cdef0123456789abcdef0123456789abcdef01C"},
		{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4", 'P', "e3/b0c44298fc1c149afbf4c8996fb92427ae41e4P"},
	}
	for _, tc := range cases {
		h := Hash{Hex: tc.hex, Suffix: tc.suffix}
		got := h.ObjectPath()
		if got != tc.want {
			t.Errorf("Hash{%q, %c}.ObjectPath() = %q, want %q", tc.hex, tc.suffix, got, tc.want)
		}
	}
}

func TestHashObjectPathShort(t *testing.T) {
	h := Hash{Hex: "ab", Suffix: 0}
	got := h.ObjectPath()
	if got != "ab" {
		t.Errorf("short hash ObjectPath = %q, want %q", got, "ab")
	}
}

func TestExtractContentHashes(t *testing.T) {
	// This test creates a real SQLite database with the CVMFS catalog schema
	// and verifies that extractContentHashes returns the correct hashes
	// with the correct suffixes, including partial chunk handling.

	db := createTestDB(t)
	defer db.Close()

	out := make(chan Hash, 100)
	go func() {
		if err := extractContentHashes(db, 2.5, out, new(int64)); err != nil {
			t.Errorf("extractContentHashes: %v", err)
		}
		close(out)
	}()

	var hashes []Hash
	for h := range out {
		hashes = append(hashes, h)
	}

	// We expect hashes from catalog table + chunks table.
	// Test data (see createTestDB):
	//   - file1: regular file, hash=aabbccdd..., flags=4       → SuffixNone
	//   - ext:   external file, hash=eeeeeeee..., flags=132    → excluded
	//   - dir:   directory, hash=NULL,            flags=1       → skipped (NULL)
	//   - big:   chunked file, hash=55667788...,  flags=68(4|64)→ SuffixNone (bulk hash)
	//   - chunk1 of big: hash=11223344...,                       → SuffixPartial
	//   - chunk2 of big: hash=ffeeddcc...,                       → SuffixPartial
	//   - dirWithHash: directory, hash=99aabb..., flags=1       → SuffixMicroCatalog

	t.Logf("Extracted %d hashes", len(hashes))

	// Build lookup: hash string (with suffix) -> found
	// This is the exact form used for sweep comparison.
	foundFull := make(map[string]bool)
	for _, h := range hashes {
		foundFull[h.String()] = true
	}

	// 1. Regular file → emitted with no suffix.
	if !foundFull["aabbccdd00112233aabbccdd00112233aabbccdd"] {
		t.Error("missing regular file hash (SuffixNone)")
	}

	// 2. Chunked file bulk hash → emitted with no suffix.
	if !foundFull["5566778899001122334455667788990011223344"] {
		t.Error("missing chunked file bulk hash (SuffixNone)")
	}

	// 3. Chunk hashes → emitted with SuffixPartial ('P').
	if !foundFull["11223344556677889900aabbccddeeff11223344P"] {
		t.Error("missing chunk1 hash with SuffixPartial")
	}
	if !foundFull["ffeeddccbbaa99887766ffeeddccbbaa99887766P"] {
		t.Error("missing chunk2 hash with SuffixPartial")
	}

	// Verify chunk hashes are NOT present without suffix (that would be wrong).
	if foundFull["11223344556677889900aabbccddeeff11223344"] {
		t.Error("chunk1 hash should have SuffixPartial, not SuffixNone")
	}
	if foundFull["ffeeddccbbaa99887766ffeeddccbbaa99887766"] {
		t.Error("chunk2 hash should have SuffixPartial, not SuffixNone")
	}

	// 4. Directory with hash → emitted with SuffixMicroCatalog ('L').
	if !foundFull["99aabb00112233445566778899aabb0011223344L"] {
		t.Error("missing directory hash with SuffixMicroCatalog")
	}

	// 5. External file → excluded entirely.
	if foundFull["eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"] {
		t.Error("external file hash should have been excluded")
	}
	if foundFull["eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeP"] {
		t.Error("external file hash should not appear with any suffix")
	}

	// 6. Verify total count: file1 + bulk + chunk1 + chunk2 + dirWithHash = 5
	expected := 5
	if len(hashes) != expected {
		t.Errorf("expected %d hashes, got %d", expected, len(hashes))
		for _, h := range hashes {
			t.Logf("  hash: %s (suffix=%c)", h.Hex, h.Suffix)
		}
	}
}

