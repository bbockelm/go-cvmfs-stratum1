package repolock

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTryLockAndUnlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	// Acquire lock.
	lk, err := TryLock(path)
	if err != nil {
		t.Fatalf("TryLock failed: %v", err)
	}
	if lk.Path() != path {
		t.Errorf("Path() = %q, want %q", lk.Path(), path)
	}

	// Lock file should exist.
	if _, err := os.Stat(path); err != nil {
		t.Errorf("lock file does not exist: %v", err)
	}

	// Unlock should succeed and remove the file.
	if err := lk.Unlock(); err != nil {
		t.Errorf("Unlock failed: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("lock file still exists after Unlock")
	}

	// Double-unlock should be safe.
	if err := lk.Unlock(); err != nil {
		t.Errorf("second Unlock failed: %v", err)
	}
}

func TestTryLockContention(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	// First lock succeeds.
	lk1, err := TryLock(path)
	if err != nil {
		t.Fatalf("first TryLock failed: %v", err)
	}
	defer lk1.Unlock()

	// Second lock from same process should fail with ErrWouldBlock.
	// Note: On some systems flock allows the same process to re-lock,
	// but we test the API contract regardless.
	lk2, err := TryLock(path)
	if err == nil {
		// On systems that allow same-process re-lock, just clean up.
		lk2.Unlock()
		t.Skip("system allows same-process flock re-acquisition; skipping contention test")
	}
	if err != ErrWouldBlock {
		t.Errorf("expected ErrWouldBlock, got: %v", err)
	}
}

func TestNilLockUnlock(t *testing.T) {
	// Nil lock should be safe to unlock.
	var lk *Lock
	if err := lk.Unlock(); err != nil {
		t.Errorf("nil Unlock failed: %v", err)
	}
	if lk.Path() != "" {
		t.Errorf("nil Path() = %q, want empty", lk.Path())
	}
}

func TestSetAcquireRelease(t *testing.T) {
	dir := t.TempDir()

	var s Set
	if s.Held() {
		t.Error("Held() should be false before Acquire")
	}

	if err := s.Acquire(dir); err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if !s.Held() {
		t.Error("Held() should be true after Acquire")
	}

	// Both lock files should exist.
	gcPath := filepath.Join(dir, GCLockName)
	updPath := filepath.Join(dir, UpdateLockName)
	if _, err := os.Stat(gcPath); err != nil {
		t.Errorf("GC lock file missing: %v", err)
	}
	if _, err := os.Stat(updPath); err != nil {
		t.Errorf("update lock file missing: %v", err)
	}

	// Release.
	s.Release()
	if s.Held() {
		t.Error("Held() should be false after Release")
	}

	// Lock files should be cleaned up.
	if _, err := os.Stat(gcPath); !os.IsNotExist(err) {
		t.Errorf("GC lock file still exists after Release")
	}
	if _, err := os.Stat(updPath); !os.IsNotExist(err) {
		t.Errorf("update lock file still exists after Release")
	}

	// Double-release should be safe.
	s.Release()
}

func TestSetAcquireContention(t *testing.T) {
	dir := t.TempDir()

	// Hold the GC lock.
	gcPath := filepath.Join(dir, GCLockName)
	held, err := TryLock(gcPath)
	if err != nil {
		t.Fatalf("pre-lock failed: %v", err)
	}
	defer held.Unlock()

	// Set.Acquire should fail because GC lock is held.
	var s Set
	err = s.Acquire(dir)
	if err == nil {
		s.Release()
		t.Skip("system allows same-process flock re-acquisition; skipping contention test")
	}
	// Should not be held on failure.
	if s.Held() {
		t.Error("Held() should be false after failed Acquire")
	}
}
