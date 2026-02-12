// Package repolock implements CVMFS-compatible repository locking using
// POSIX flock(2).
//
// CVMFS uses two lock files in the spool directory (/var/spool/cvmfs/<repo>/)
// to serialize operations:
//
//   - is_collecting.lock - held during garbage collection
//   - is_updating.lock   - held during snapshot/update operations
//
// The GC process acquires both locks: the GC lock to prevent concurrent GC
// runs, and the update lock to prevent concurrent snapshots from modifying
// the repository while GC is in progress.
//
// Locks are implemented via flock(2) with LOCK_EX (exclusive). The lock is
// automatically released by the kernel when the file descriptor is closed
// or the process exits. The lock file is explicitly removed on normal
// cleanup; if the process is killed, the lock file may remain on disk but
// the kernel-level lock is released, so a stale file does not block future
// operations (flock is on the fd, not the filename).
package repolock

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const (
	// GCLockName is the lock file name for garbage collection, matching
	// the CVMFS convention in cvmfs_server_common.sh:
	//   acquire_gc_lock -> acquire_lock ${CVMFS_SPOOL_DIR}/is_collecting
	// acquire_lock appends ".lock" to the path.
	GCLockName = "is_collecting.lock"

	// UpdateLockName is the lock file name for update/snapshot operations.
	// Acquired by GC on Stratum-1 to prevent concurrent snapshot.
	UpdateLockName = "is_updating.lock"
)

// ErrWouldBlock is returned by TryLock when another process holds the lock.
var ErrWouldBlock = fmt.Errorf("lock is held by another process")

// Lock represents a single POSIX flock on a file.
type Lock struct {
	path string
	fd   int
}

// TryLock attempts to acquire an exclusive flock without blocking.
// Returns ErrWouldBlock if the lock is already held, or another error on
// failure. This mirrors CVMFS TryLockFile (posix.cc:936-956):
//
//	open(path, O_RDONLY|O_CREAT, 0600)
//	flock(fd, LOCK_EX|LOCK_NB)
func TryLock(path string) (*Lock, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CREAT, 0600)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", path, err)
	}

	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		syscall.Close(fd)
		if err == syscall.EWOULDBLOCK {
			return nil, ErrWouldBlock
		}
		return nil, fmt.Errorf("flock %s: %w", path, err)
	}

	return &Lock{path: path, fd: fd}, nil
}

// Unlock releases the lock and removes the lock file.
// Safe to call multiple times.
func (l *Lock) Unlock() error {
	if l == nil || l.fd < 0 {
		return nil
	}
	// Close the fd releases the flock automatically.
	err := syscall.Close(l.fd)
	l.fd = -1
	// Remove the lock file (best-effort, matching CVMFS release_lock).
	os.Remove(l.path)
	return err
}

// Path returns the lock file path.
func (l *Lock) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// Set manages a pair of CVMFS GC locks (gc + update) and integrates
// with the process signal handler for cleanup.
type Set struct {
	mu      sync.Mutex
	gcLock  *Lock
	updLock *Lock
}

// Acquire acquires both the GC lock and the update lock in the given
// spool directory. Returns ErrWouldBlock if either lock is already held.
// The locks should be released by calling Release.
func (s *Set) Acquire(spoolDir string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	gcPath := filepath.Join(spoolDir, GCLockName)
	updPath := filepath.Join(spoolDir, UpdateLockName)

	gcLock, err := TryLock(gcPath)
	if err != nil {
		return fmt.Errorf("acquiring GC lock: %w", err)
	}

	updLock, err := TryLock(updPath)
	if err != nil {
		gcLock.Unlock()
		return fmt.Errorf("acquiring update lock: %w", err)
	}

	s.gcLock = gcLock
	s.updLock = updLock
	return nil
}

// Release releases both locks. Safe to call multiple times.
func (s *Set) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.updLock != nil {
		s.updLock.Unlock()
		s.updLock = nil
	}
	if s.gcLock != nil {
		s.gcLock.Unlock()
		s.gcLock = nil
	}
}

// Held returns true if the lock set currently holds locks.
func (s *Set) Held() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gcLock != nil
}
