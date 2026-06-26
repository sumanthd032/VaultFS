# Step 3 — Chunk System

## What was built

| File | Description |
|------|-------------|
| `internal/chunk/hasher.go` | SHA-256 content addressing: `Hash`, `Verify`, `ChunkID.Valid` |
| `internal/chunk/store.go` | Disk-backed, content-addressed store with atomic writes and read-time integrity verification |
| `internal/chunk/replication.go` | GFS-style pipeline replication along a linear secondary chain |
| `internal/chunk/gc.go` | Garbage-collection goroutine for orphaned chunks with a grace period |
| `internal/chunk/heartbeat.go` | Heartbeat sender: reports stored-chunk inventory to the master every 5s |
| `internal/chunk/chunk_test.go` | 30 tests: roundtrip, corruption detection, pipeline replication, GC, heartbeat |
| `internal/metadata/lease.go` | LeaseManager: grant/renew/revoke/check with 60s automatic expiry |
| `internal/metadata/heartbeat.go` | Master-side Monitor: dead-node detection + re-replication tasks |
| `internal/metadata/chunkmap.go` | (extended) `UnderReplicated` to surface chunks below the replication factor |
| `internal/metadata/lease_test.go` | 8 tests: grant, contention, expiry, renew, revoke, sweep, concurrency |
| `internal/metadata/heartbeat_test.go` | 5 tests: liveness, dead-node sweep, eviction, re-replication tasks, defaults |

## Why each piece was built this way

### Content addressing (chunk ID = SHA-256 of bytes)

**What we chose:** A chunk's identity *is* the hash of its contents.

**Alternatives considered:** Master-assigned opaque chunk handles (classic GFS 64-bit IDs); UUIDs.

**Why this choice:** Content addressing makes writes idempotent (the same bytes always map to the same file, so retries and duplicate replication are free no-ops) and makes corruption *self-detecting* — any reader re-hashes the bytes it read and compares to the ID it asked for, needing no external checksum store. This is the same property that makes Git's object store and IPFS robust.

### Atomic durable writes (temp + fsync + rename)

**What we chose:** Write to a temp file in the destination directory, `fsync`, then `rename` into place.

**Alternatives considered:** Writing directly to the final path (a crash mid-write leaves a truncated, corrupt chunk); `O_TMPFILE` + linkat (less portable).

**Why this choice:** `rename` within a filesystem is atomic, so a reader never sees a partially written chunk — the file either exists complete or not at all. `fsync` before the rename guarantees the bytes are durable, not just the directory entry. A serialise-by-ID guard lets different chunks write in parallel while collapsing concurrent writes of *identical* bytes.

### Fanout directories (first 2 hex chars)

**What we chose:** Store chunks under `<root>/<ab>/<full-id>`, sharding across 256 subdirectories.

**Why this choice:** A single flat directory with millions of entries degrades lookup and listing on many filesystems (ext4 htree, older XFS). Two-character fanout caps per-directory size and mirrors Git's object layout — a well-understood, proven pattern.

### Pipeline replication (linear chain, not fan-out)

**What we chose:** The primary stores locally then forwards to `secondary-1`, which forwards to `secondary-2`. Data moves in one direction along a chain.

**Alternatives considered:** Star fan-out where the primary pushes to every secondary itself.

**Why this choice:** This is the GFS data-flow design. Fan-out makes the primary's uplink the bottleneck (it sends N copies); a linear pipeline lets every node spend its outbound bandwidth exactly once and overlaps transfers, maximising aggregate throughput. The `ChunkSender` interface is defined consumer-side so the gRPC transport (Step 4) and test fakes are drop-in.

### GC grace period

**What we chose:** An unreferenced chunk is deleted only after it has been *observed orphaned* for a grace period, tracked by an in-memory `orphanedAt` timer with an injectable clock.

**Alternatives considered:** Immediate deletion of anything unreferenced.

**Why this choice:** A chunk can be written to a chunk server *before* the master commits the namespace entry that references it (the normal write ordering). Immediate GC would race that window and delete live data. The grace period closes the race — the same reason GFS defers chunk deletion. The injectable clock makes the grace logic deterministically testable without real sleeps.

### Lease manager (lazy expiry + contention guard)

**What we chose:** Leases expire lazily — every access checks the clock — with an optional `SweepExpired` for eager cleanup. Granting a held chunk to a different node returns `ErrLeaseHeld`.

**Alternatives considered:** A background goroutine per lease (one timer per chunk — does not scale); eager-only expiry.

**Why this choice:** Lazy expiry means correctness never depends on a sweeper running; a single mutex-guarded map handles any number of leases. This is exactly how a master serialises writes to a chunk: the lease holder is the primary that orders mutations, and it must be unique.

### Heartbeat: sender on the chunk server, monitor on the master

**What we chose:** Split responsibility — `chunk.HeartbeatSender` reports inventory outward; `metadata.Monitor` consumes heartbeats, detects death, and computes re-replication work.

**Why this choice:** It mirrors the real topology. The chunk server owns its inventory and pushes it; the master owns the global view (which chunks are under-replicated after a failure) and decides remediation. Re-replication is *failure-driven*: a sweep only emits tasks when a node actually dies, avoiding churn from transient steady-state under-replication.

## How this connects to the final goal

- **Step 4 (Client surface)** implements `ChunkSender` and `HeartbeatReporter` over gRPC, and wires `LeaseManager` into the master's `OpenForWrite`/`GetLease` RPCs. The chunk `Store` becomes the chunk server's `WriteChunk`/`ReadChunk` RPC backend.
- **Step 6 (Observability)** instruments these paths: GC deletions, heartbeat-missed counts (from `Monitor`), and active lease count (from `LeaseManager`) become Prometheus metrics.
- The `Monitor`'s `ReplicationTask` output is the trigger for the master's re-replication controller, keeping every chunk at replication factor 3.

## Why this matters (technically and for the portfolio)

This step builds the **data plane** and demonstrates:
- **Content-addressed storage** with self-verifying integrity — the property behind Git, IPFS, and Dropbox's block store.
- **Crash-safe durability** via the temp+fsync+rename idiom, the canonical way to write files that survive power loss.
- **GFS pipeline replication** — showing the candidate has read and understood the original GFS paper, not just used a library.
- **Race-aware GC** with a grace period — awareness that distributed deletion is subtle and ordering-dependent.
- **Lease-based write coordination** — the mechanism that gives a weakly-consistent replicated store a single serialisation point per chunk.

## Tests added

| Test | What it covers |
|------|---------------|
| `TestHashDeterministic`, `TestVerify`, `TestChunkIDValid` | SHA-256 addressing and ID validation |
| `TestStoreWriteReadRoundtrip` | Write/read across small, empty, binary, and 1 MiB payloads |
| `TestStoreWriteIdempotent` | Re-writing identical bytes is a no-op |
| `TestStoreReadNotFound` | Missing chunk returns `ErrChunkNotFound` |
| `TestStoreReadCorruptionDetected` | Bit-flip on disk → `ErrChecksumFailed` on read |
| `TestStoreDelete`, `TestStoreListChunks`, `TestStoreFanoutLayout` | Delete, listing, on-disk layout |
| `TestStoreConcurrentWrites` | 50 concurrent writers (shared + unique chunks) under `-race` |
| `TestPipelineReplicationToThreeNodes` | Chunk reaches all 3 replicas with verifiable integrity |
| `TestReplicationTailReturnsWithoutForwarding` | Chain tail stores without calling the sender |
| `TestReplicationChainFailurePropagates` | Unreachable downstream node surfaces an error |
| `TestGCDeletesOrphansKeepsReferenced` | Orphans deleted, referenced chunks preserved |
| `TestGCRespectsGracePeriod` | No deletion before grace elapses; deletion after |
| `TestGCResetsTimerWhenReReferenced` | Re-referenced chunk is not deleted |
| `TestHeartbeatSenderReportsInventory` | Sender reports the correct chunk inventory |
| `TestLeaseGrant`, `TestLeaseGrantContended` | Grant and same/different-holder contention |
| `TestLeaseExpiry`, `TestLeaseRenew`, `TestLeaseRevoke`, `TestLeaseSweepExpired` | Time-based expiry, renewal, revocation, sweep |
| `TestLeaseConcurrentRequests` | 64-goroutine contention yields a single consistent holder |
| `TestMonitorRecordAndLive`, `TestMonitorSweepDeclaresDeadAndEvicts` | Liveness tracking, dead-node eviction, re-replication tasks |
| `TestMonitorSweepNoDeadNoTasks`, `TestMonitorDefaults` | No false positives; default config |

## How to verify this step manually

```bash
go test -race -v ./internal/chunk/...
# Expected: 30 tests pass, no race conditions

go test -race -v ./internal/metadata/...
# Expected: lease + monitor tests pass alongside Step 2's metadata tests

make test   # full suite, all packages
make lint   # 0 issues
```
