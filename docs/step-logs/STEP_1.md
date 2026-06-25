# Step 1 — Core Foundations

## What was built

| File | Description |
|------|-------------|
| `go.mod` | Module declaration (`github.com/sumanthd032/vaultfs`, Go 1.22) |
| `Makefile` | Targets: `test`, `lint`, `build`, `proto`, `certs`, `dev`, `docker-build`, `clean` |
| `.golangci.yml` | Linter config enabling errcheck, govet, staticcheck, gosec |
| `.gitignore` | Standard Go ignores + `deploy/certs/`, `docs/step-logs/`, `CLAUDE.md` |
| `LICENSE` | MIT license, copyright Sumanth D |
| `cmd/master/main.go` | Master node entry point stub |
| `cmd/chunkserver/main.go` | Chunk server entry point stub |
| `cmd/vaultfs/main.go` | CLI entry point stub |
| `internal/wal/entry.go` | `Entry` type + `Encode`/`Decode` with CRC32 integrity |
| `internal/wal/segment.go` | Rolling segment file management (`*segment`, create, open, write, sync, close) |
| `internal/wal/wal.go` | `WAL` struct: `Open`, `Append`, `ReadAll`, `LastIndex`, `Close`, `rotate` |
| `internal/wal/recovery.go` | `replayFile` + `recover`: safe crash recovery, tail truncation |
| `internal/wal/wal_test.go` | 11 top-level tests, 16 subtests covering all WAL behaviors |
| `internal/clock/lamport.go` | `LamportClock` backed by `atomic.Uint64` |
| `internal/clock/vector.go` | `VectorClock` (immutable map): `Increment`, `Merge`, `HappensBefore`, `Concurrent`, `Equal` |
| `internal/clock/clock_test.go` | 13 top-level tests, 26 subtests covering all clock behaviors |

---

## Why each piece was built this way

### WAL on-disk format: length + CRC32 + payload

- **What we chose:** `[8:payloadLen][4:CRC32][8:index][N:data]` per record.
- **Alternatives considered:** RocksDB-style format (type + length + data + trailing CRC); LevelDB block format (block-level CRC).
- **Why this choice:** Prefix-length framing lets the decoder allocate exactly the right buffer and detect truncation before attempting a read. Leading CRC means we verify integrity immediately after reading, before returning data to callers. Separating the index into the payload (rather than the header) means the CRC covers both index and data — corruption in either is caught.

### WAL segment rotation

- **What we chose:** Rolling 64 MiB segments named `%016d.wal` (zero-padded for lexicographic sort).
- **Alternatives considered:** Single unbounded file; fixed-count rotation.
- **Why this choice:** Bounded segment size = bounded recovery time. Recovery only needs to scan segments since the last snapshot; old segments can be deleted once the Raft snapshot covers them (Step 2). Zero-padded naming means `sort.Strings` on file names gives the correct temporal order without parsing.

### Crash recovery: truncate to last valid entry

- **What we chose:** `replayFile` replays entries until the first `io.ErrUnexpectedEOF` or `ErrChecksumMismatch`, then returns the byte offset of the last valid entry. The active segment is `Truncate`-d to this offset before resuming appends.
- **Alternatives considered:** Keep the corrupt tail and mark it invalid; require explicit `fsync` before crash recovery is allowed.
- **Why this choice:** Truncating the file is the correct GFS/Raft behavior. An entry is only "committed" if it was fsynced and acknowledged. Any partial record at the tail was in-flight during the crash and was never acked — discarding it is safe. This matches the WAL semantics used by etcd, RocksDB, and Kafka.

### CRC32 vs SHA-256 for WAL integrity

- **What we chose:** CRC32 (IEEE polynomial) per WAL entry.
- **Alternatives considered:** SHA-256 (used for chunk content addressing in Step 3); Adler-32.
- **Why this choice:** CRC32 is a non-cryptographic error-detection code. It is fast (hardware-accelerated on x86), detects all single-bit errors, and is sufficient for detecting accidental disk corruption in a WAL. SHA-256 is reserved for content-addressed chunk IDs where the property needed is collision resistance, not just error detection.

### LamportClock: CAS loop for concurrency

- **What we chose:** `atomic.Uint64` with a compare-and-swap loop in `Update`.
- **Alternatives considered:** `sync.Mutex` around the counter; lock-free monotonic counter without CAS.
- **Why this choice:** `Tick()` is a single atomic `Add` — no CAS needed. `Update` needs read-then-write atomicity (`max(local, received)+1`) which requires CAS. A mutex would work but adds latency under high concurrency; CAS is wait-free in the uncontended case (which is the common case for clock updates).

### VectorClock: immutable value semantics

- **What we chose:** `VectorClock = map[NodeID]uint64`, all operations return new copies.
- **Alternatives considered:** Mutable struct with a mutex; persistent/functional map for true immutability.
- **Why this choice:** VectorClocks are passed in messages and compared at receive time — they are conceptually values, not shared mutable state. Returning copies makes the API impossible to misuse (no caller can accidentally mutate a clock that is already in a message). The copy cost is negligible: in practice a VectorClock has at most 3–5 entries (one per master node).

### No external dependencies in Step 1

- **What we chose:** Pure stdlib (`encoding/binary`, `hash/crc32`, `sync/atomic`, `path/filepath`, etc.).
- **Why this choice:** The WAL and clock are the lowest-level primitives — they must be dependency-free to be trustworthy. External libraries would introduce supply-chain risk and complicate auditing of the core correctness logic. Dependencies enter in Step 2 (BadgerDB) and Step 4 (gRPC).

---

## How this connects to the final goal

| What was built | What depends on it |
|----------------|--------------------|
| `internal/wal` | Step 3 chunk server (each chunk write is WAL-logged before being applied) |
| `internal/wal` | Step 2 Raft node (WAL stores Raft log entries for crash safety) |
| `internal/clock/LamportClock` | Step 2 Raft (term tracking, log entry ordering) |
| `internal/clock/VectorClock` | Step 2 metadata (chunk version tracking per node, conflict detection) |
| `go.mod` / `Makefile` | Every subsequent step: tests, lint, and Docker builds all use these |

---

## Why this matters

**WAL correctness:** The WAL is the single most critical component for durability. Every node in every distributed storage system bottoms out in a write-ahead log. The implementation here demonstrates knowledge of binary framing, CRC integrity, crash recovery semantics (truncate-not-discard), and fsync discipline — all details that distinguish production systems from toy ones.

**Distributed clocks:** Implementing both Lamport and vector clocks from scratch demonstrates understanding of the foundational papers (Lamport 1978, Fidge/Mattern 1988). A VectorClock that correctly identifies concurrent events (vs. causally ordered ones) is a prerequisite for the chunk version tracking in Step 3 and the conflict detection in metadata operations.

**Atomic counter in LamportClock:** Using `atomic.Uint64` with a CAS loop rather than a mutex shows awareness of lock-free programming and cache-line effects — the kind of detail that distinguishes a distributed systems engineer from someone who just cargo-cults `sync.Mutex`.

---

## Tests added

| Test | What it covers |
|------|---------------|
| `TestWALEmpty` | Fresh WAL has 0 entries and LastIndex 0 |
| `TestWALSingleEntry` (4 subtests) | Append and readback: empty, small, binary, large data |
| `TestWALMultiEntry` (3 subtests) | 2, 10, and 100 entries all preserved in order |
| `TestWALCloseAndReopen` | Entries survive a close/reopen cycle |
| `TestWALCrashRecovery_TruncatedEntry` | Partial header at file tail is silently discarded |
| `TestWALCRCCorruption` | Bit-flipped CRC causes the entry and all subsequent ones to be dropped |
| `TestWALConcurrentAppends` | 8 goroutines × 20 appends; no races, all entries present |
| `TestWALSegmentRotation` | Segment rotates when limit is hit; all entries remain readable |
| `TestWALSegmentRotationSurvivesReopen` | Multi-segment WAL survives close/reopen |
| `TestWALLastIndex` | LastIndex tracks the highest appended index |
| `TestWALLastIndexRestoredOnReopen` | LastIndex restored correctly from disk |
| `TestLamportClock_Tick` (2 subtests) | Single and multi-tick return values |
| `TestLamportClock_Monotonic` | 1000 ticks all strictly increasing |
| `TestLamportClock_Update_HigherReceived` (3 subtests) | max(local, received)+1 semantics |
| `TestLamportClock_Update_LowerReceived` | Local clock still advances |
| `TestLamportClock_Now` | Now does not advance the clock |
| `TestLamportClock_Concurrent` | 200 concurrent goroutines, no races |
| `TestVectorClock_Increment` (3 subtests) | Counter increments, other nodes unchanged |
| `TestVectorClock_IncrementImmutability` | Original unchanged after Increment |
| `TestVectorClock_Merge` (3 subtests) | Component-wise maximum |
| `TestVectorClock_HappensBefore` (6 subtests) | All combinations of causal ordering |
| `TestVectorClock_Concurrent` (3 subtests) | True concurrent, causally ordered, equal |
| `TestVectorClock_Equal` (5 subtests) | Equality including implicit zero keys |
| `TestVectorClock_HappensBefore_Transitivity` | a→b, b→c implies a→c |

**Total: 56 test cases (24 top-level, 32 subtests)**

---

## How to verify this step manually

```bash
# Run all tests with race detector
go test -race -v ./...

# Run lint (zero errors expected)
golangci-lint run ./...

# Build all binaries (stub entry points)
make build

# WAL package only, verbose
go test -race -v ./internal/wal/...

# Clock package only, verbose
go test -race -v ./internal/clock/...
```
