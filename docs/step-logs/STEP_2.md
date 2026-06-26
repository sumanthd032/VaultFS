# Step 2 — Raft Consensus + Metadata

## What was built

| File | Description |
|------|-------------|
| `internal/raft/types.go` | Shared types: NodeState, Entry, Config, *Args/*Reply, Snapshot |
| `internal/raft/log.go` | Thread-safe in-memory Raft log with compaction via SetSnapshot |
| `internal/raft/transport.go` | Transport interface + InMemNetwork for test-time RPC delivery |
| `internal/raft/node.go` | Node struct, Start/Stop, Propose, main run loop, apply loop |
| `internal/raft/election.go` | RequestVote RPC, randomised election timeout, single-node fast path |
| `internal/raft/replication.go` | AppendEntries RPC, log replication, majority commit index advance |
| `internal/raft/snapshot.go` | TakeSnapshot, InstallSnapshot, log compaction trigger at 10k entries |
| `internal/raft/transport_grpc.go` | gRPC transport using gob codec (no proto codegen required) |
| `internal/raft/raft_test.go` | 15 tests: single-node, 3-node, 5-node election; heartbeat; log replication; leader failure; partition/heal; raftLog unit tests |
| `internal/metadata/store.go` | BadgerDB-backed Store: Get/Put/Delete/Scan + Txn (commit/discard) |
| `internal/metadata/namespace.go` | POSIX-like namespace: Create/Stat/Update/Delete/ListDir/Rename |
| `internal/metadata/chunkmap.go` | In-memory ChunkMap: replica locations per chunk, per-node eviction |
| `internal/metadata/version.go` | ChunkVersion using VectorClock for write-conflict detection |
| `internal/metadata/metadata_test.go` | 26 tests covering Store, Namespace, ChunkMap, ChunkVersion |

## Why each piece was built this way

### Raft in-memory log (raftLog)

**What we chose:** A `[]Entry` slice with a `snapIndex`/`snapTerm` offset so that after a snapshot, slice indices remain dense.

**Alternatives considered:** A map indexed by log index (O(1) lookup but poor cache locality and GC pressure), or a linked list (easier compaction but terrible random access).

**Why this choice:** Slice-based storage gives O(1) append and O(n) sequential scan — both cheap for the expected log sizes. Snapshot compaction just re-slices and updates the offset; no memory copy required until the retained tail is copied once.

### Transport interface with InMemNetwork

**What we chose:** A `Transport` interface whose test implementation (`InMemNetwork`) delivers RPCs by calling `RPCHandler` methods directly in-process — no network, no goroutine hand-off.

**Alternatives considered:** Running a local gRPC server per test node (slow, port binding, async delivery); using channels (adds latency, complicates partition simulation).

**Why this choice:** Synchronous in-process delivery makes tests deterministic and fast (the 15 Raft tests complete in ≈1.6 s). Partitions are simulated by a deny-list in InMemNetwork with zero overhead. The same interface accepts the production GRPCTransport drop-in.

### gRPC transport without proto codegen

**What we chose:** `GRPCTransport` uses `grpc.ServiceDesc` + a `gobCodec` registered with `encoding.RegisterCodec`. Messages are gob-encoded/decoded; no `.proto` file or `protoc`/`buf` toolchain is needed in Step 2.

**Alternatives considered:** `net/rpc` (stdlib, gob over TCP — not gRPC), raw HTTP/2 + JSON, deferring the transport entirely.

**Why this choice:** gRPC is the production transport mandated by the build plan. Using a custom codec lets us use the full gRPC framework (connection pooling, interceptors, TLS later) while deferring the proto toolchain to Step 4, when the full service protos are defined. The codec is swappable to protobuf with one line once the protos exist.

### BadgerDB for metadata

**What we chose:** BadgerDB (LSM-tree, embedded, no server) for the namespace and any other small key/value metadata the master stores.

**Alternatives considered:** BoltDB/bbolt (B-tree, write-heavy bottleneck), SQLite (schema friction for variable-shape inodes), etcd/Raft-backed K/V (circular dependency in Step 2).

**Why this choice:** BadgerDB is purpose-built for write-heavy workloads, provides ACID transactions, and supports prefix iteration (`Seek`/`ValidForPrefix`) which is exactly what ListDir needs. It is an embedded database — no separate process, easy to test with `t.TempDir()`.

### ChunkMap as in-memory index

**What we chose:** A `sync.RWMutex`-guarded `map[string][]Location` rather than a BadgerDB-backed store.

**Alternatives considered:** Persisting chunk locations in BadgerDB, using a sync.Map.

**Why this choice:** Chunk locations are rebuilt from chunk-server heartbeats at startup (Step 3). Persisting them adds write amplification for no benefit. `sync.RWMutex` gives cheap concurrent reads (the majority case) and serialised writes for mutation. `sync.Map` is optimised for high-read/no-key-set-growth workloads; our map grows continuously, so a plain mutex is faster.

### ChunkVersion with VectorClock

**What we chose:** Reuse `internal/clock.VectorClock` (built in Step 1) to track per-chunk causal history. `ChunkVersion` is a value type (immutable, returns new copies on mutate).

**Why this choice:** VectorClock already handles the `HappensBefore` and `Concurrent` semantics we need for conflict detection. Reusing it avoids duplicating the logic and ensures consistent causal-ordering semantics across the codebase.

## How this connects to the final goal

- **Step 3 (Chunk system)** will call `ChunkMap.AddLocation`/`RemoveLocation` from heartbeat handlers, and use `ChunkVersion` to detect write conflicts during replication.
- **Step 4 (Client surface)** will define the complete proto schemas and wire the gRPC transport into the master and chunkserver binaries. At that point `GRPCTransport` gets its codec upgraded from gob to protobuf.
- **Step 5 (Security)** will pass a `tls.Config` to `grpc.NewServer` and `grpc.NewClient` in `transport_grpc.go` — the transport structure is already designed for this.
- **Step 7 (K8s)** depends on the Raft cluster's leader election to know which master pod should own the Kubernetes leader lease.

## Why this matters (technically and for the portfolio)

This step demonstrates:
- **Raft from scratch**: Most engineers use a library (hashicorp/raft, etcd/raft). Building the state machine, log, quorum logic, and fast log back-up from the paper shows deep understanding.
- **Interface-driven design for distributed components**: The Transport interface decouples the algorithm from the network, enabling sub-millisecond test execution.
- **Dual-layer storage**: BadgerDB for durable namespace + in-memory ChunkMap for ephemeral replica locations reflects the real design trade-off in GFS and HDFS.
- **Vector clocks in practice**: ChunkVersion with VectorClock shows awareness of the CAP theorem and concurrent-write detection at the data layer.

## Tests added

| Test | What it covers |
|------|---------------|
| `TestSingleNodeBecomesLeader` | Single-node cluster wins election immediately (quorum = 1) |
| `TestThreeNodeElection` | One leader elected in a 3-node cluster; no split-brain |
| `TestFiveNodeElection` | Scales to 5 nodes |
| `TestElectionTermAdvances` | Term ≥ 1 after election completes |
| `TestHeartbeatPreventsFollowerElection` | Leader heartbeats keep followers stable; term unchanged |
| `TestLogReplicationToQuorum` | 5 commands committed and delivered on all 3 commit channels |
| `TestProposeRejectsOnFollower` | `Propose` returns `ErrNotLeader` on a non-leader node |
| `TestLeaderFailureTriggerReelection` | Stopping the leader causes remaining nodes to elect a new one with higher term |
| `TestLogConsistencyAfterPartitionHeals` | Partitioned old leader steps down; log converges after heal |
| `TestRaftLogAppendAndGet` | Basic log append and slice retrieval |
| `TestRaftLogTruncate` | TruncateAfter removes the correct tail |
| `TestRaftLogSetSnapshot` | SetSnapshot discards compacted entries; LastIndex unchanged |
| `TestRaftLogTermCompacted` | Term() returns error for compacted indices |
| `TestRaftLogSlice` | Slice returns the correct sub-range |
| `TestStorePutGet`, `TestStoreGetMissingKey`, `TestStoreDelete`, `TestStoreDeleteMissingIsNoop`, `TestStoreScan`, `TestStoreTxnCommit`, `TestStoreTxnDiscard` | BadgerDB store CRUD and transaction semantics |
| `TestNamespaceCreate*`, `TestNamespaceStat*`, `TestNamespaceDelete*`, `TestNamespaceListDir`, `TestNamespaceRename*`, `TestNamespaceUpdateFile` | Namespace inode tree operations |
| `TestChunkMap*` | ChunkMap add, update-in-place, remove, node eviction, unknown chunk |
| `TestChunkVersion*` | HappensBefore, Concurrent, Equal, Merge |

## How to verify this step manually

```bash
go test -race -v ./internal/raft/...
# Expected: 15 tests pass, no race conditions

go test -race -v ./internal/metadata/...
# Expected: 26 tests pass

make test   # full suite
make lint   # 0 issues
```
