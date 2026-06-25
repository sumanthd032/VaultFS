<div align="center">

# VaultFS

**A production-grade distributed filesystem built from first principles in Go.**

Inspired by the Google File System paper. Built for correctness, durability, and operational clarity.

[![CI](https://github.com/sumanthd032/vaultfs/actions/workflows/ci.yml/badge.svg)](https://github.com/sumanthd032/vaultfs/actions)
[![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</div>

---

## What Is VaultFS

VaultFS is a distributed filesystem that spreads your data across multiple machines while presenting a single unified namespace. Files are split into fixed-size chunks, each chunk replicated three times across different nodes. A Raft-based master cluster tracks where every chunk lives. Clients read and write as if talking to one machine.

**Core guarantees:**
- No data loss — write-ahead logs on every node, fsync before ack
- No silent corruption — every chunk is SHA-256 fingerprinted
- No single point of failure — 3-node Raft master cluster, automatic leader election
- No stale reads — lease-based write coordination prevents split-brain

## Architecture

```
Clients (CLI · Go SDK · REST gateway)
         │
         │ gRPC + mTLS
         ▼
┌─────────────────────────────────┐
│   Master Cluster (Raft, 3 nodes) │
│   Namespace · Chunk map · Leases │
└──────────┬──────────────────────┘
           │ gRPC + mTLS
    ┌──────┴──────┐
    ▼      ▼      ▼
 CS-0   CS-1   CS-2    ← Chunk servers (StatefulSet in K8s)
SHA-256 · WAL · Replication factor 3
```

Full architecture details → [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Quick Start

```bash
# Clone
git clone https://github.com/sumanthd032/vaultfs.git
cd vaultfs

# Generate certs and start full cluster (requires Docker)
make certs
make dev

# In another terminal — use the CLI
vaultfs put ./myfile.txt /remote/myfile.txt
vaultfs get /remote/myfile.txt ./downloaded.txt
vaultfs ls /remote/
vaultfs status
```

Open Grafana at http://localhost:3000 (admin/admin) to see the live cluster dashboard.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Go 1.22+ |
| RPC | gRPC + Protocol Buffers |
| Consensus | Custom Raft |
| Metadata store | BadgerDB |
| Observability | Prometheus + Grafana |
| Security | mTLS (cert-manager ready) |
| Local dev | Docker Compose |
| Production | Kubernetes (StatefulSets) |
| CI/CD | GitHub Actions |

Full rationale → [docs/TECH_STACK.md](docs/TECH_STACK.md)

## Development

```bash
make test          # run all tests
make lint          # golangci-lint
make proto         # regenerate protobuf
make build         # build all binaries
make docker-build  # build Docker images
```

## Project Structure

```
cmd/             CLI + node entrypoints
internal/        Core library (wal, raft, clock, chunk, metadata, metrics, security)
pkg/client/      Public Go SDK
proto/           gRPC definitions
deploy/          Docker Compose + Kubernetes manifests
docs/            Architecture, tech stack, build plan, step logs
```

## Key Design Decisions

**Why custom Raft?** Using etcd/raft would hide the most interesting part of the system. The implementation is readable and documented to serve as a learning reference.

**Why BadgerDB?** Embedded, no external process, Go-native, LSM-tree based — ideal for the master's metadata workload (frequent small reads/writes, rare full scans).

**Why StatefulSets for chunk servers?** Stable network identity (`chunkserver-0.vaultfs`) means the master's chunk location map survives pod restarts without remapping.

**Why mTLS everywhere?** In a real distributed system, every node must authenticate every other node. This is non-negotiable for security and models how production systems like Kubernetes itself work.

## License

MIT — see [LICENSE](LICENSE)
