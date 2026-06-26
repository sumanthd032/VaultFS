# Architecture - VaultFS

## System Overview

VaultFS follows the Google File System (GFS) architecture: a single logical namespace managed
by a replicated master cluster, with actual file data stored in fixed-size chunks distributed
across multiple chunk servers. Clients interact with the master for metadata and directly with
chunk servers for data.

```
+---------------------------------------------------------+
|                        CLIENTS                          |
|        CLI | Go SDK (pkg/client) | REST gateway         |
+-----------------------+---------------------------------+
                        | gRPC (mTLS)
          +-------------+-------------+
          v             v             v
+----------------------------------------------------------+
|              MASTER CLUSTER (Raft, 3 nodes)              |
|                                                          |
|  +-------------+   Raft   +-----------+  Raft  +------+ |
|  |  Master 0   |<-------->| Master 1  |<------>|  M2  | |
|  |  (LEADER)   |          |(FOLLOWER) |        |(FOL) | |
|  +-------------+          +-----------+        +------+ |
|                                                          |
|  Responsibilities (leader only):                         |
|  | Namespace tree (directory -> inode mapping)           |
|  | Chunk location map (chunkID -> []nodeID)              |
|  | Chunk version map (chunkID -> version)                |
|  | Lease manager (grant write leases to primary chunks)  |
|  | Heartbeat monitor (detect dead chunk servers)         |
|  | GC coordinator (mark orphaned chunks for deletion)    |
|                                                          |
|  Persistence: BadgerDB (replicated via Raft log)         |
+---------------------+------------------------------------+
                      | gRPC (mTLS)
        +-------------+-------------+
        v             v             v
+------------+  +------------+  +------------+
|ChunkServer0|  |ChunkServer1|  |ChunkServer2|   ... N nodes
|            |  |            |  |            |
| SHA-256    |  | SHA-256    |  | SHA-256    |
| chunks     |  | chunks     |  | chunks     |
| WAL        |  | WAL        |  | WAL        |
| /metrics   |  | /metrics   |  | /metrics   |
+------------+  +------------+  +------------+
        |               |               |
        +---------------+---------------+
                        v
              +------------------+
              |   OBSERVABILITY  |
              |  Prometheus      |
              |  Grafana         |
              +------------------+
```

---

## Component Details

### Master Cluster

**Raft consensus** governs master cluster state. Three masters run at all times. One is the
leader and handles all client requests. The other two are followers that replicate the leader's
log entries. If the leader dies, an election produces a new leader within ~150-300ms
(randomized election timeouts).

**Namespace tree** is an in-memory trie (backed by BadgerDB for persistence) mapping paths
like `/data/logs/app.log` to inodes. Each inode stores: file size, creation/modification time,
replication factor, and a list of chunk handles in order.

**Chunk location map** maps `chunkID -> []nodeID`. This is NOT persisted - it is rebuilt
on master startup from heartbeat reports sent by chunk servers. This is the GFS paper's
design choice: storing locations transiently avoids consistency problems when nodes
join/leave.

**Lease manager** grants time-limited leases (60 seconds) to a "primary" chunk server for
each chunk being written. The primary coordinates replication to secondary servers.
This serializes concurrent writes to the same chunk without master involvement in the
critical write path.

**Heartbeat monitor** expects a heartbeat from each chunk server every 5 seconds.
After 3 missed heartbeats, the server is marked dead, its chunks are removed from the
location map, and re-replication is triggered for under-replicated chunks.

### Chunk Servers

Each chunk server stores chunks as files on local disk. A chunk is a 64MB fixed-size blob.
Files larger than 64MB produce multiple chunks; files smaller than 64MB produce one
partially-filled chunk.

**SHA-256 addressing** means the chunk's ID is the SHA-256 hash of its content. This gives:
- Automatic deduplication (identical content -> same ID)
- Corruption detection (any bit flip changes the hash)
- Content-addressability (ID is self-verifying)

**Write path** for a single chunk write:
1. Client asks master for a write lease on the chunk
2. Master grants lease to a primary chunk server
3. Client pushes data to all replicas (pipeline: client -> CS0 -> CS1 -> CS2)
4. Client sends "commit" to primary
5. Primary applies write, forwards to secondaries, waits for acks
6. Primary acks client
7. Client tells master the write is complete

**WAL** on each chunk server ensures that even if the process crashes mid-write, the
chunk data is not partially applied. Every chunk write is logged to the WAL (with fsync)
before being applied to the chunk store.

### Client Layer

**Go SDK** (`pkg/client`) is the canonical way to interact with VaultFS from Go programs.
It handles connection pooling to masters, automatic failover to a new leader, chunk splitting,
parallel chunk uploads/downloads, and retry logic.

**CLI** (`cmd/vaultfs`) is a thin wrapper over the Go SDK using Cobra.

**gRPC protos** define three services:
- `MasterService` - namespace operations (create, delete, stat, list, open)
- `ChunkService` - chunk operations (read, write, delete, report)
- `AdminService` - cluster health, rebalance, GC trigger

---

## Data Flow: Write a File

```
vaultfs put bigfile.dat /data/bigfile.dat

1. Client splits bigfile.dat into 64MB chunks: [C0, C1, C2]
2. Client -> Master: CreateFile("/data/bigfile.dat", numChunks=3)
3. Master -> Client: [chunkHandle0, chunkHandle1, chunkHandle2]
4. For each chunk:
   a. Client -> Master: GrantLease(chunkHandle)
   b. Master -> Client: {primary: CS0, secondaries: [CS1, CS2], leaseExpiry}
   c. Client pushes chunk data to CS0 -> CS1 -> CS2 (pipeline)
   d. Client -> CS0: CommitWrite(chunkHandle)
   e. CS0 applies, forwards to CS1/CS2, waits for acks
   f. CS0 -> Client: WriteAck
5. Client -> Master: FinalizeFile("/data/bigfile.dat")
```

## Data Flow: Read a File

```
vaultfs get /data/bigfile.dat ./local.dat

1. Client -> Master: Open("/data/bigfile.dat")
2. Master -> Client: [{chunkHandle0, [CS0,CS1,CS2]}, {chunkHandle1, ...}, ...]
3. Client reads each chunk from the closest replica (round-robin or latency-based)
4. Client reassembles chunks into local file
5. SHA-256 of each chunk verified against chunkHandle before use
```

---

## Fault Tolerance

| Failure | VaultFS Response |
|---------|-----------------|
| Chunk server dies | Master detects via missed heartbeats, triggers re-replication of affected chunks to remaining healthy servers |
| Master follower dies | Raft continues with remaining nodes (tolerates 1 failure with 3 nodes) |
| Master leader dies | Raft election completes in ~200ms, new leader resumes serving requests |
| Chunk corrupted | SHA-256 mismatch detected on read, client retries from a different replica, master schedules re-replication |
| Network partition | Raft guarantees only one leader can exist per term; the side without quorum stops accepting writes |
| Mid-write crash | WAL on chunk server ensures write is either fully applied or fully rolled back on recovery |

---

## Security Model

All node-to-node communication uses mTLS:
- Each node has a certificate signed by the cluster CA
- Connections are rejected if the peer's certificate is not from the cluster CA
- In Kubernetes, cert-manager handles certificate issuance and rotation
- In local dev, `make certs` generates a self-signed CA and per-node certificates

---

## Kubernetes Topology

```
Namespace: vaultfs
|
+-- Deployment: vaultfs-master          (3 replicas, any node)
|   +-- Service: vaultfs-master-svc     (ClusterIP, headless for Raft peer discovery)
|
+-- StatefulSet: vaultfs-chunkserver    (3+ replicas, stable identity)
|   +-- chunkserver-0.vaultfs-cs-svc
|   +-- chunkserver-1.vaultfs-cs-svc
|   +-- chunkserver-N.vaultfs-cs-svc
|   +-- VolumeClaimTemplate: 50Gi per pod (chunk data persisted across restarts)
|
+-- ConfigMap: vaultfs-config
+-- Secret: vaultfs-tls
|
+-- Monitoring (kube-prometheus-stack)
    +-- Prometheus (scrapes /metrics from all pods via ServiceMonitor)
    +-- Grafana (vaultfs dashboard from deploy/k8s/monitoring/grafana-dashboard.json)
```

**Why StatefulSet for chunk servers?** Chunk servers need stable network identity because the
master's chunk location map uses node IDs. If a pod restarts with a new IP, the master must
be able to reconcile the node. Stable DNS names (`chunkserver-0.vaultfs-cs-svc`) make this
trivial - the node ID is derived from the stable hostname, not the ephemeral IP.
