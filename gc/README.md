# cvmfs-gc — Optimized CVMFS Garbage Collector

A high-performance garbage collector for
[CVMFS](https://cernvm.cern.ch/fs/) Stratum-1 servers, designed to handle
repositories with hundreds of millions of content-addressable objects.

## How it works

The built-in CVMFS GC holds all reachable hashes in memory and then
walks every object in the data directory.  That approach falls over on
very large repositories.  This tool replaces it with a three-phase
external-sort pipeline that keeps memory usage bounded:

1. **Catalog traversal + semi-sort** — A parallel worker pool walks the
   catalog tree (nested SQLite databases).  Discovered content hashes are
   streamed into a heap-based semi-sorter that flushes sorted runs to
   disk when memory pressure is reached.

2. **Chunk sort** — The semi-sorted runs are concatenated and split into
   ~100 MB chunks.  Each chunk is sorted and deduplicated in memory, then
   written to disk.

3. **Sweep** — The 256 hash-prefix directories (`00`–`ff`) are scanned
   sequentially.  Directory entries are merge-joined against a streaming
   k-way merge of the sorted chunks.  Any object not present in the
   merge stream is unreachable and deleted.

ReadDir I/O is overlapped with the merge-join via a one-ahead goroutine,
and `os.DirEntry.Info()` is deferred until after a successful delete to
avoid unnecessary stat syscalls.

## Building

```bash
# Native build
make

# Cross-compile for Linux x86-64
make linux

# Run tests
make test
```

### Cross-compilation

SQLite is provided by [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite),
a pure-Go translation of SQLite with no cgo dependency.  This means
cross-compilation requires nothing beyond the Go toolchain:

```bash
make linux
scp cvmfs-gc-linux-amd64 stratum1:/usr/local/bin/cvmfs-gc
```

The resulting binary is statically linked and can be copied directly to
a Stratum-1 server with no runtime dependencies.

## Usage

### Garbage collection

```bash
# List unreachable objects (default — nothing is deleted)
cvmfs-gc -repo /srv/cvmfs/myrepo.example.org

# Actually delete unreachable objects
cvmfs-gc -repo /srv/cvmfs/myrepo.example.org -delete
```

The tool automatically reads `.cvmfspublished` from the repo root to
discover the root catalog hash plus any history, certificate, and
metainfo objects.  You can also supply these explicitly:

```bash
cvmfs-gc -repo /srv/cvmfs/myrepo.example.org \
         -manifest /path/to/.cvmfspublished \
         -root-hash abc123...
```

### Mirror (for testing)

Download a small repository from a Stratum-1 to create a local test
fixture:

```bash
cvmfs-gc -mirror \
         -stratum1-url http://stratum1.example.org:8000/cvmfs/repo.example.org \
         -repo /tmp/test-mirror
```

Then run GC against the mirror:

```bash
cvmfs-gc -repo /tmp/test-mirror
```

### All flags

| Flag | Default | Description |
|------|---------|-------------|
| `-repo` | | Path to the local CVMFS repository |
| `-root-hash` | | Root catalog hash (auto-detected from manifest) |
| `-manifest` | | Path to `.cvmfspublished` (defaults to `<repo>/.cvmfspublished`) |
| `-parallelism` | 8 | Number of parallel catalog-traversal workers |
| `-heap-mb` | 100 | Memory budget (MB) for the semi-sort heap |
| `-chunk-mb` | 100 | Chunk size (MB) for the chunk-sort phase |
| `-delete` | false | Actually delete unreachable files (default is list only) |
| `-temp-dir` | | Temp directory for intermediate files (auto-created if omitted) |
| `-mirror` | false | Mirror mode: download a repo from a Stratum-1 |
| `-stratum1-url` | | Base URL of the Stratum-1 (mirror mode only) |
| `-mirror-jobs` | 8 | Parallel downloads for mirroring |

## Project layout

```
cmd/cvmfs-gc/   CLI entry point, manifest parsing, three-phase pipeline
catalog/        Parallel CVMFS catalog-tree traversal, hash types, zlib decompression
hashsort/       Semi-sort, chunk sort, k-way streaming merge reader
sweep/          Directory sweep with merge-join deletion
mirror/         Download a CVMFS repo snapshot from a Stratum-1
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
