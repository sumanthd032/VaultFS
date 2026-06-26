# Tech Stack — VaultFS

Every technology choice is deliberate. This document records what we use and exactly why.

---

## Language: Go 1.26+

**Why Go:**
- First-class goroutines and channels make distributed systems code natural to write
- `net` and `sync` stdlib are production-quality
- gRPC, Prometheus, BadgerDB, and Cobra all have excellent Go-native libraries
- Fast compilation means rapid iteration
- Single static binary per component simplifies deployment and Docker images
- `go test -race` built-in race detector catches concurrency bugs during development

**Why not Rust:** Steeper learning curve for distributed systems patterns; ecosystem less mature for this stack.
**Why not C++:** Memory safety issues; no standard dependency management.

Go version pinned in `go.mod` via the `go` directive (`go 1.26.0`), which sets the
minimum required toolchain. The directive uses the full `major.minor.patch` form,
the modern convention since Go 1.21.

---

## RPC: gRPC + Protocol Buffers

**Why gRPC:**
- Strongly typed contracts between all nodes
- Bidirectional streaming for push-based chunk data transfer
- HTTP/2 multiplexing means one connection per peer, not one per RPC
- Auto-generated client stubs in Go
- First-class mTLS support via `credentials.NewTLS`

**Why Protobuf:**
- Compact binary encoding (critical for bulk chunk metadata in the master)
- Schema evolution with field numbering (add fields without breaking old nodes)
- `protoc-gen-go-grpc` generates all the boilerplate

**Proto structure:**
```
proto/
  master.proto       — MasterService (namespace + lease operations)
  chunk.proto        — ChunkService (read/write/delete chunks)
  admin.proto        — AdminService (cluster health, GC)
  types.proto        — shared types (ChunkID, NodeID, FileInfo, etc.)
```

---

## Consensus: Custom Raft

**Why custom Raft (not etcd/raft, not hashicorp/raft):**
- This IS the project. Using a library hides the most important part.
- The implementation is documented and tested to serve as a learning reference.
- Demonstrates actual understanding of leader election, log replication, and safety.
- Shows up strongly in interviews: "walk me through your Raft implementation."

**What we implement:**
- Leader election with randomized election timeouts (150–300ms)
- Heartbeat-based lease renewal (50ms interval)
- Log replication with majority quorum
- Log compaction via snapshotting (once log exceeds 10,000 entries)
- Cluster membership changes (add/remove nodes) — joint consensus

**What we do NOT implement:**
- Multi-Raft (one Raft group per shard) — out of scope
- Pipelining of log entries — clean implementation first, optimize later

---

## Metadata Store: BadgerDB

**Why BadgerDB:**
- Embedded — no external process to run, no ops overhead
- Go-native library (DGraph)
- LSM-tree based — fast writes, good for the master's workload
- ACID transactions
- Supports prefix iteration (needed for directory listing: scan all keys with prefix `/data/logs/`)
- Survives restarts — metadata is durable

**Why not BoltDB:** BadgerDB has better write throughput; BoltDB is B-tree (better for reads). Master has more writes than reads at scale.
**Why not SQLite:** No complex relational queries needed; key-value is the right abstraction.
**Why not Redis:** Requires external process, no disk durability by default, wrong abstraction.

**Key layout in BadgerDB:**
```
inode:{path}        → InodeProto (file metadata)
chunk:{chunkID}     → ChunkMetaProto (version, size, deleted flag)
gc:{chunkID}        → GCEntryProto (scheduled for deletion at time T)
lease:{chunkID}     → LeaseProto (primary node, expiry)
```

---

## Chunk Addressing: SHA-256

**Why SHA-256:**
- Cryptographic hash → collision probability negligible (2^-128 for SHA-256 truncated to 128 bits)
- Any bit flip in chunk data changes the hash → silent corruption detected on every read
- Content-addressability: identical files share chunks automatically (deduplication)
- Standard library: `crypto/sha256` in Go stdlib, zero external dependency

**Chunk size: 64 MB**
Following the GFS paper. Large chunks reduce master load (fewer metadata entries per file).
The trade-off is wasted space for small files — acceptable for our use case.

---

## Clocks: Lamport Clocks + Vector Clocks

**Why both:**
- **Lamport clocks** — total ordering of events across nodes. Used to order log entries within the WAL and across master replicas. Cheap: one uint64 per node.
- **Vector clocks** — causal ordering. Used to detect write conflicts when two clients write the same chunk concurrently without going through the master. Each chunk's version is a vector clock.

These are implemented in `internal/clock/` and used by both the WAL and the chunk replication logic.

---

## Security: mTLS

**Why mTLS (mutual TLS):**
- Both sides of every connection authenticate each other
- A rogue process cannot join the cluster without a certificate signed by the cluster CA
- Encrypts all data in transit (chunk data, metadata, Raft log entries)
- Standard in production distributed systems (Kubernetes itself uses mTLS internally)

**Implementation:**
- `internal/security/` handles certificate loading and `tls.Config` construction
- Each node loads: its own cert+key, and the CA cert to verify peers
- `make certs` generates a self-signed CA + per-component certs for local dev
- In K8s: cert-manager issues and rotates certificates automatically

---

## Write-Ahead Log: Custom

**Why a custom WAL:**
- Core distributed systems primitive — must understand deeply
- BadgerDB handles master metadata durability; chunk servers need their own WAL for chunk data
- The WAL is the durability guarantee for chunk servers: even if the process crashes mid-write, recovery replays the WAL to reconstruct committed state

**WAL design:**
- Segmented: rolling files of up to 64MB each, older segments deleted after snapshot
- Entry format: `[length:4][crc32:4][data:N]` — self-describing, crash-recoverable
- `fsync` on every append (configurable: can use `fdatasync` for performance)
- Recovery: scan all segments, replay valid entries, discard entries after last valid CRC

---

## Observability: Prometheus + Grafana

**Why Prometheus:**
- Pull-based scraping fits the K8s deployment model (Prometheus scrapes `/metrics` from pods)
- `prometheus/client_golang` is the standard Go instrumentation library
- All VaultFS-specific metrics are defined in `internal/metrics/` with clear naming

**Why Grafana:**
- The dashboard JSON is committed to the repo (`deploy/k8s/monitoring/grafana-dashboard.json`)
- Anyone cloning the repo can import it immediately — one-command observability
- Shows operational maturity: a project with a Grafana dashboard is a serious project

**Six core metrics:**

| Metric name | Type | What it measures |
|-------------|------|-----------------|
| `vaultfs_ops_total` | Counter | Read/write operations, labelled by type |
| `vaultfs_wal_write_seconds` | Histogram | WAL append latency (durability cost) |
| `vaultfs_raft_elections_total` | Counter | Leader elections (cluster stability) |
| `vaultfs_replication_lag_seconds` | Gauge | Lag between primary and secondary chunk sync |
| `vaultfs_heartbeat_missed_total` | Counter | Missed heartbeats per chunk server node |
| `vaultfs_active_leases` | Gauge | Number of currently outstanding write leases |

---

## Local Dev: Docker Compose

**Why Docker Compose:**
- `make dev` starts 3 masters + 3 chunk servers + Prometheus + Grafana in one command
- No need to manually manage 8 processes during development
- Mimics the K8s topology locally (same network names, same env vars)
- Every developer gets an identical environment

**Compose topology:**
```yaml
services:
  master-0, master-1, master-2       — Master nodes
  chunkserver-0, chunkserver-1, chunkserver-2  — Chunk servers
  prometheus                         — Scrapes all /metrics endpoints
  grafana                            — Dashboards (auto-provisioned)
```

---

## Production: Kubernetes

**Why K8s:**
- StatefulSets give chunk servers stable network identity
- Deployment + rolling update for masters (zero-downtime deploys)
- VolumeClaimTemplates give each chunk server its own persistent disk
- cert-manager handles mTLS certificate rotation
- Horizontal scaling: add chunk servers by scaling the StatefulSet

**Why StatefulSet for chunk servers (not Deployment):**
Deployments give pods random names and ephemeral identity. Chunk servers need stable
identities because the master's chunk location map uses node IDs derived from hostnames.
`chunkserver-0.vaultfs` always refers to the same logical node, even after pod restarts.

---

## CI/CD: GitHub Actions

**Three jobs, minimal but complete:**

```yaml
test:
  - go test -race -coverprofile=coverage.out ./...
  - Upload coverage to Codecov

lint:
  - golangci-lint run
  - go vet ./...

docker:
  - Build images for master, chunkserver, vaultfs CLI
  - Push to ghcr.io/sumanthd032/vaultfs on main branch
```

**Why golangci-lint:** Runs 30+ linters in one tool. Configuration in `.golangci.yml`.
Key linters enabled: `errcheck`, `govet`, `staticcheck`, `gosec`, `exhaustruct`.

---

## CLI: Cobra

**Why Cobra:**
- Standard Go CLI library (kubectl, hugo, and most major Go CLIs use it)
- Auto-generates help text, completion scripts, man pages
- Subcommand structure fits the `vaultfs put/get/ls/status` model naturally

**Commands:**
```
vaultfs put <local-path> <remote-path>   — upload file
vaultfs get <remote-path> <local-path>   — download file
vaultfs ls  <remote-path>                — list directory
vaultfs rm  <remote-path>                — delete file
vaultfs status                           — cluster health (leader, nodes, replication)
```
