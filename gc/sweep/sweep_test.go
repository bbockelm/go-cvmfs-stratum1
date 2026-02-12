package sweep

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestSweepDeletesUnreachable(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	// Create some fake object files under data/XX/...
	// Reachable: aa/1111... and bb/2222...
	// Unreachable: aa/9999... and cc/3333...
	reachableHashes := []string{
		"aa" + pad("1111", 38),
		"bb" + pad("2222", 38),
	}
	unreachableHashes := []string{
		"aa" + pad("9999", 38),
		"cc" + pad("3333", 38),
	}

	allHashes := append(reachableHashes, unreachableHashes...)
	for _, h := range allHashes {
		prefix := h[:2]
		rest := h[2:]
		dir := filepath.Join(dataDir, prefix)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(filepath.Join(dir, rest))
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString("dummy content")
		f.Close()
	}

	// Write sorted hash file as a single "chunk" (just the reachable ones).
	sort.Strings(reachableHashes)
	chunkDir := filepath.Join(tmpDir, "chunks")
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		t.Fatal(err)
	}
	chunkPath := filepath.Join(chunkDir, "chunk-000000.txt")
	hf, err := os.Create(chunkPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range reachableHashes {
		hf.WriteString(h + "\n")
	}
	hf.Close()

	cfg := Config{
		DataDir:    dataDir,
		ChunkFiles: []string{chunkPath},
		DryRun:     false,
	}

	stats, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Check that reachable files still exist
	for _, h := range reachableHashes {
		p := filepath.Join(dataDir, h[:2], h[2:])
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Errorf("reachable file was deleted: %s", h)
		}
	}

	// Check that unreachable files were deleted
	for _, h := range unreachableHashes {
		p := filepath.Join(dataDir, h[:2], h[2:])
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("unreachable file was not deleted: %s", h)
		}
	}

	t.Logf("Stats: checked=%d deleted=%d retained=%d",
		stats.FilesChecked, stats.FilesDeleted, stats.FilesRetained)
}

func TestSweepDryRun(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	// Create one unreachable file
	hash := "ab" + pad("face", 36)
	dir := filepath.Join(dataDir, "ab")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Create(filepath.Join(dir, hash[2:]))
	f.WriteString("data")
	f.Close()

	// No chunk files -- everything is unreachable
	cfg := Config{
		DataDir:    dataDir,
		ChunkFiles: nil,
		DryRun:     true,
	}

	stats, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// File should still exist because dry run
	p := filepath.Join(dir, hash[2:])
	if _, err := os.Stat(p); os.IsNotExist(err) {
		t.Error("dry run deleted a file")
	}

	if stats.FilesDeleted != 1 {
		t.Errorf("expected 1 file marked for deletion, got %d", stats.FilesDeleted)
	}
}

func TestSweepMultipleChunks(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	// Create files across several prefixes.
	reachable := []string{
		"11" + pad("aaaa", 38),
		"22" + pad("bbbb", 38),
		"33" + pad("cccc", 38),
		"ff" + pad("dddd", 38),
	}
	unreachable := []string{
		"11" + pad("zzzz", 38),
		"22" + pad("zzzz", 38),
		"44" + pad("eeee", 38),
	}

	for _, h := range append(reachable, unreachable...) {
		prefix := h[:2]
		rest := h[2:]
		dir := filepath.Join(dataDir, prefix)
		os.MkdirAll(dir, 0755)
		f, _ := os.Create(filepath.Join(dir, rest))
		f.WriteString("content")
		f.Close()
	}

	// Split reachable hashes across two chunk files.
	sort.Strings(reachable)
	chunkDir := filepath.Join(tmpDir, "chunks")
	os.MkdirAll(chunkDir, 0755)

	// Chunk 1: first two hashes
	chunk1 := filepath.Join(chunkDir, "chunk-000000.txt")
	f1, _ := os.Create(chunk1)
	for _, h := range reachable[:2] {
		f1.WriteString(h + "\n")
	}
	f1.Close()

	// Chunk 2: last two hashes
	chunk2 := filepath.Join(chunkDir, "chunk-000001.txt")
	f2, _ := os.Create(chunk2)
	for _, h := range reachable[2:] {
		f2.WriteString(h + "\n")
	}
	f2.Close()

	cfg := Config{
		DataDir:    dataDir,
		ChunkFiles: []string{chunk1, chunk2},
		DryRun:     false,
	}

	stats, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	for _, h := range reachable {
		p := filepath.Join(dataDir, h[:2], h[2:])
		if _, err := os.Stat(p); os.IsNotExist(err) {
			t.Errorf("reachable file was deleted: %s", h)
		}
	}

	for _, h := range unreachable {
		p := filepath.Join(dataDir, h[:2], h[2:])
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("unreachable file was not deleted: %s", h)
		}
	}

	if stats.FilesDeleted != int64(len(unreachable)) {
		t.Errorf("expected %d deletions, got %d", len(unreachable), stats.FilesDeleted)
	}

	t.Logf("Stats: checked=%d deleted=%d retained=%d",
		stats.FilesChecked, stats.FilesDeleted, stats.FilesRetained)
}

// pad returns s repeated/truncated to exactly n characters.
func pad(s string, n int) string {
	result := ""
	for len(result) < n {
		result += s
	}
	return result[:n]
}

func TestSweepDeletedBySuffix(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")

	// Create files with various suffixes — all unreachable.
	// CVMFS filenames: prefix dir (2 hex chars) + rest-of-hash + optional suffix.
	unreachable := []string{
		"aa/" + pad("1111", 38),       // no suffix (data object)
		"aa/" + pad("2222", 38) + "P", // partial chunk
		"bb/" + pad("3333", 38) + "C", // catalog
		"bb/" + pad("4444", 38) + "P", // partial chunk
		"cc/" + pad("5555", 38) + "L", // micro-catalog
	}
	for _, h := range unreachable {
		dir := filepath.Join(dataDir, h[:2])
		os.MkdirAll(dir, 0755)
		f, _ := os.Create(filepath.Join(dataDir, h))
		f.WriteString("data")
		f.Close()
	}

	// No reachable hashes — everything gets deleted.
	cfg := Config{
		DataDir:    dataDir,
		ChunkFiles: nil,
		DryRun:     true,
	}

	stats, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if stats.FilesDeleted != int64(len(unreachable)) {
		t.Errorf("expected %d deletions, got %d", len(unreachable), stats.FilesDeleted)
	}

	// Check per-suffix counts.
	if got := stats.DeletedBySuffix[0]; got != 1 {
		t.Errorf("DeletedBySuffix[none]: want 1, got %d", got)
	}
	if got := stats.DeletedBySuffix['P']; got != 2 {
		t.Errorf("DeletedBySuffix['P']: want 2, got %d", got)
	}
	if got := stats.DeletedBySuffix['C']; got != 1 {
		t.Errorf("DeletedBySuffix['C']: want 1, got %d", got)
	}
	if got := stats.DeletedBySuffix['L']; got != 1 {
		t.Errorf("DeletedBySuffix['L']: want 1, got %d", got)
	}

	t.Logf("Suffix breakdown: none=%d P=%d C=%d L=%d",
		stats.DeletedBySuffix[0], stats.DeletedBySuffix['P'],
		stats.DeletedBySuffix['C'], stats.DeletedBySuffix['L'])
}
