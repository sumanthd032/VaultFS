# Build Plan — VaultFS (7 Steps)

Complete one step at a time. Do not begin the next step until told.
After each step: run tests, lint, write the step log, commit.

---

## Step 1 — Core Foundations

**Goal:** Establish the project scaffold and the two lowest-level building blocks:
the Write-Ahead Log and distributed clocks. Everything else depends on these.

**Deliverables:**

### Project scaffold
- `go.mod` — module `github.com/sumanthd032/vaultfs`, Go 1.26
- `Makefile` — targets: `test`, `lint`, `build`, `proto`, `certs`, `dev`, `docker-build`
- `.golangci.yml` — linter configuration
- `.gitignore` — Go standard + `deploy/certs/` + binaries
- `CLAUDE.md`, `README.md`, `docs/` skeleton (all docs already written)
- `cmd/master/main.go`, `cmd/chunkserver/main.go`, `cmd/vaultfs/main.go` — empty stubs

### `internal/wal/` — Write-Ahead Log
- `wal.go` — WAL struct, Open/Close, Append, ReadAll
- `segment.go` — segment file management (rolling, size limit)
- `entry.go` — Entry type, binary encoding (length + CRC32 + data)
- `recovery.go` — replay on startup, discard entries after last valid CRC
- `wal_test.go` — table-driven tests covering:
  - Single and multi-entry append + readback
  - Crash recovery (simulate truncated entry)
  - CRC corruption detection
  - Concurrent appends (with -race)
  - Segment rotation when size limit reached

### `internal/clock/` — Distributed Clocks
- `lamport.go` — LamportClock struct (atomic uint64, Tick, Update, Now)
- `vector.go` — VectorClock struct (map[NodeID]uint64, Increment, Update, Compare, HappensBefore)
- `clock_test.go` — tests covering:
  - Lamport tick monotonicity
  - Lamport update on receive (max + 1)
  - Vector clock causality (HappensBefore, concurrent events)
  - Vector clock merge

**Minimum test count: 20 passing tests**

**Commits for this step (examples):**
```
chore(build): initialize go module and project scaffold
chore(build): add Makefile with test lint build targets
feat(wal): implement segmented write-ahead log with CRC32 integrity
feat(wal): add crash recovery with partial entry detection
test(wal): add table-driven tests for append recovery and concurrency
feat(clock): implement Lamport clock with atomic increment
feat(clock): implement vector clock with HappensBefore comparison
test(clock): add causality and concurrency tests for both clock types
docs(step-logs): add Step 1 completion log
```

---

## Step 2 — Raft Consensus + Metadata

**Goal:** Build the brain of VaultFS. The master cluster must be able to elect a leader,
replicate state, and persist metadata — before any file operations are possible.

**Deliverables:**

### `internal/raft/`
- `node.go` — RaftNode struct, state machine (Follower/Candidate/Leader), main loop
- `election.go` — RequestVote RPC, randomized election timeout (150–300ms), vote granting
- `log.go` — in-memory log ([]LogEntry), AppendEntries RPC, consistency check
- `replication.go` — leader sends AppendEntries to all followers, majority quorum ack
- `snapshot.go` — InstallSnapshot RPC, log compaction trigger (after 10k entries)
- `transport.go` — gRPC-based RPC transport between Raft peers
- `raft_test.go` — tests covering:
  - Single node becomes leader
  - Three node election, one candidate wins
  - Leader sends heartbeats, followers stay as followers
  - Log replication to quorum (majority ack)
  - Leader failure → new election → new leader continues log
  - Log consistency after partition heals

### `internal/metadata/`
- `store.go` — MetadataStore backed by BadgerDB: Open/Close, Get/Put/Delete with transactions
- `namespace.go` — inode tree: CreateFile, DeleteFile, Stat, ListDir, Rename
- `chunkmap.go` — in-memory chunk location map: AddLocation, RemoveLocation, GetLocations
- `version.go` — chunk version tracking (vector clock per chunk)
- `metadata_test.go` — namespace CRUD, chunk map updates, version conflicts

**Commits for this step (examples):**
```
feat(raft): implement Raft node state machine and election timeout
feat(raft): implement RequestVote RPC with term and log checks
feat(raft): implement AppendEntries RPC and log consistency check
feat(raft): implement log replication with majority quorum
feat(raft): add log compaction via snapshot
test(raft): add election and replication tests for 3-node cluster
feat(metadata): implement BadgerDB-backed metadata store
feat(metadata): implement namespace tree with inode operations
feat(metadata): implement in-memory chunk location map
test(metadata): add namespace CRUD and chunk map tests
docs(step-logs): add Step 2 completion log
```

---

## Step 3 — Chunk System

**Goal:** Build the data plane. Chunk servers can now store, replicate, and recover chunks.
The master can coordinate writes via leases and clean up deleted chunks.

**Deliverables:**

### `internal/chunk/`
- `store.go` — ChunkStore: WriteChunk, ReadChunk, DeleteChunk, ListChunks (disk-backed)
- `hasher.go` — SHA-256 chunk ID generation and verification
- `replication.go` — replicate chunk to secondary servers (pipeline: primary → secondary chain)
- `gc.go` — GC goroutine: scan for orphaned chunks, delete after grace period
- `heartbeat.go` — heartbeat sender (chunk server → master, every 5s), report stored chunks
- `chunk_test.go` — tests covering:
  - Write and read roundtrip
  - SHA-256 corruption detection (flip a bit, verify mismatch)
  - Replication to 3 nodes (mock secondaries)
  - GC deletes orphaned chunks, preserves referenced chunks

### `internal/metadata/lease.go`
- `LeaseManager` — grant/revoke/check leases, automatic expiry (60s), renewal
- `lease_test.go` — grant, expiry, revocation, concurrent lease requests

### Heartbeat monitor in master
- `internal/metadata/heartbeat.go` — track last heartbeat per node, mark dead nodes,
  trigger re-replication for under-replicated chunks

**Commits for this step (examples):**
```
feat(chunk): implement disk-backed chunk store with SHA-256 addressing
feat(chunk): add SHA-256 verification on read with corruption detection
feat(chunk): implement pipeline replication to secondary chunk servers
feat(chunk): add GC goroutine for orphaned chunk cleanup
feat(chunk): implement heartbeat sender with chunk inventory report
feat(metadata): implement lease manager with automatic expiry
feat(metadata): add heartbeat monitor with dead node detection
test(chunk): add roundtrip corruption detection and replication tests
docs(step-logs): add Step 3 completion log
```

---

## Step 4 — Client Surface

**Goal:** Make VaultFS usable. A developer can now write Go code to store and retrieve files,
or use the CLI directly.

**Deliverables:**

### `proto/` — Complete gRPC definitions
- `types.proto` — ChunkID, NodeID, FileInfo, ChunkLocation, Lease
- `master.proto` — MasterService: CreateFile, DeleteFile, Stat, ListDir, OpenForRead, OpenForWrite, FinalizWrite, GetLease
- `chunk.proto` — ChunkService: ReadChunk, WriteChunk, DeleteChunk, ReplicateChunk
- `admin.proto` — AdminService: ClusterStatus, TriggerGC, ListNodes

### `pkg/client/` — Public Go SDK
- `client.go` — Client struct: New(masterAddrs), Put(ctx, localPath, remotePath), Get(ctx, remotePath, localPath)
- `file.go` — FileWriter and FileReader: chunk splitting, parallel upload/download
- `pool.go` — connection pool to master and chunk servers (with failover)
- `retry.go` — exponential backoff retry for transient errors
- `client_test.go` — integration tests using in-process fake master + chunk server

### `cmd/vaultfs/` — CLI
- `main.go` — Cobra root command
- `cmd/put.go`, `cmd/get.go`, `cmd/ls.go`, `cmd/rm.go`, `cmd/status.go`
- Each command reads flags, calls `pkg/client`, prints structured output

**Commits for this step (examples):**
```
feat(proto): add shared types proto (ChunkID, NodeID, FileInfo)
feat(proto): add MasterService proto with namespace and lease RPCs
feat(proto): add ChunkService and AdminService protos
feat(sdk): implement Go client with Put/Get and chunk splitting
feat(sdk): add connection pool with master failover
feat(sdk): add exponential backoff retry for transient errors
feat(cli): add vaultfs CLI with put get ls rm status commands
test(sdk): add integration tests with in-process fake cluster
docs(step-logs): add Step 4 completion log
```

---

## Step 5 — Security + Docker

**Goal:** Make VaultFS secure and runnable as a real cluster with one command.

**Deliverables:**

### `internal/security/`
- `tls.go` — load cert/key/CA, build `tls.Config` for both client and server modes
- `certs.go` — helper to validate cert expiry and log warnings

### `Makefile` — `certs` target
- Generate cluster CA (self-signed)
- Generate cert+key for master, chunkserver, vaultfs client
- Store in `deploy/certs/` (gitignored)

### Dockerfiles
- `cmd/master/Dockerfile` — multi-stage build, scratch final image
- `cmd/chunkserver/Dockerfile` — multi-stage build, scratch final image
- `cmd/vaultfs/Dockerfile` — multi-stage build for CLI image

### `deploy/docker-compose.yml`
- `master-0`, `master-1`, `master-2` — with Raft peer env vars
- `chunkserver-0`, `chunkserver-1`, `chunkserver-2` — with master addresses
- `prometheus` — scrape config pointing to all /metrics endpoints
- `grafana` — auto-provisioned with VaultFS dashboard
- All services mount `deploy/certs/` and use mTLS

**Commits for this step (examples):**
```
feat(security): implement mTLS config loader for client and server
chore(build): add make certs target for local dev certificate generation
chore(deploy): add multi-stage Dockerfiles for master and chunkserver
chore(deploy): add Docker Compose with full 6-node cluster and observability
docs(step-logs): add Step 5 completion log
```

---

## Step 6 — Observability

**Goal:** Make VaultFS operationally transparent. Anyone running this cluster can see
exactly what's happening at a glance.

**Deliverables:**

### `internal/metrics/`
- `metrics.go` — register all Prometheus metrics:
  - `vaultfs_ops_total` (CounterVec, labels: type, status)
  - `vaultfs_wal_write_seconds` (Histogram, buckets: 0.1ms to 100ms)
  - `vaultfs_raft_elections_total` (Counter)
  - `vaultfs_replication_lag_seconds` (GaugeVec, label: node_id)
  - `vaultfs_heartbeat_missed_total` (CounterVec, label: node_id)
  - `vaultfs_active_leases` (Gauge)
- `server.go` — start `/metrics` HTTP endpoint

### Instrumentation
- Call `metrics.RecordOp(...)` in chunk store read/write paths
- Call `metrics.RecordWALWrite(duration)` in WAL append
- Call `metrics.RecordElection()` in Raft election completion
- Call `metrics.SetReplicationLag(nodeID, lag)` in replication pipeline
- Call `metrics.RecordMissedHeartbeat(nodeID)` in heartbeat monitor

### `deploy/k8s/monitoring/grafana-dashboard.json`
- Six panels: ops/sec, WAL latency heatmap, election counter, replication lag gauge, heartbeat missed counter, active leases gauge
- Auto-imports in Docker Compose and K8s via provisioning

### `deploy/k8s/monitoring/alerting-rules.yaml`
- Alert: `VaultFSReplicationLagHigh` (lag > 5s for any node)
- Alert: `VaultFSLeaderElectionFrequent` (more than 3 elections in 5 minutes)
- Alert: `VaultFSChunkServerDown` (missed heartbeats > 15s)

**Commits for this step (examples):**
```
feat(metrics): register all six Prometheus metrics with descriptive help strings
feat(metrics): instrument WAL append with write latency histogram
feat(metrics): instrument Raft election counter
feat(metrics): instrument chunk replication lag gauge
feat(metrics): instrument heartbeat monitor missed counter
chore(deploy): add Grafana dashboard JSON with six operational panels
chore(deploy): add Prometheus alerting rules for lag elections and node down
docs(step-logs): add Step 6 completion log
```

---

## Step 7 — Kubernetes + CI/CD

**Goal:** Make VaultFS deployable to a real Kubernetes cluster and automate all quality gates.

**Deliverables:**

### `deploy/k8s/`
- `namespace.yaml` — `vaultfs` namespace
- `configmap.yaml` — cluster config (replication factor, chunk size, election timeout)
- `secret.yaml` — TLS certs (base64 encoded, for local testing; production uses cert-manager)
- `master-deployment.yaml` — 3 replicas, resource limits, liveness/readiness probes
- `master-service.yaml` — ClusterIP + headless service for Raft peer discovery
- `chunkserver-statefulset.yaml` — StatefulSet, stable hostnames, VolumeClaimTemplate 50Gi
- `chunkserver-service.yaml` — headless service for stable DNS
- `monitoring/prometheus-servicemonitor.yaml` — scrape all /metrics via ServiceMonitor
- `monitoring/grafana-dashboard.json` — same dashboard, auto-import via ConfigMap

### `.github/workflows/ci.yml`
```yaml
on: [push, pull_request]
jobs:
  test:
    - go test -race -coverprofile=coverage.out ./...
  lint:
    - golangci-lint run
  docker:
    - docker build + push to ghcr.io (on main branch only)
```

### Final polish
- `CONTRIBUTING.md` — how to run locally, code conventions, PR checklist
- `LICENSE` — MIT
- README badges: CI status, Go version, license, coverage
- `docs/step-logs/STEP_7.md`

**Commits for this step (examples):**
```
chore(k8s): add namespace configmap and TLS secret manifests
chore(k8s): add master Deployment with liveness and readiness probes
chore(k8s): add chunkserver StatefulSet with VolumeClaimTemplate
chore(k8s): add headless services for Raft peer discovery and chunk server DNS
chore(k8s): add Prometheus ServiceMonitor for all VaultFS pods
chore(ci): add GitHub Actions workflow with test lint and docker jobs
docs: add CONTRIBUTING.md and MIT LICENSE
docs(step-logs): add Step 7 completion log
```
