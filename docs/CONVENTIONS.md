# Conventions — VaultFS

Code style, commit rules, and the step log template.

---

## Commit Style — Conventional Commits

Format: `<type>(<scope>): <subject>`

### Types
| Type | When to use |
|------|-------------|
| `feat` | New feature or capability |
| `fix` | Bug fix |
| `test` | Adding or updating tests |
| `refactor` | Restructure without behavior change |
| `docs` | Documentation only |
| `chore` | Build system, Makefile, CI, tooling |
| `perf` | Performance improvement |

### Scopes
`wal` · `raft` · `chunk` · `metadata` · `client` · `cli` · `sdk` · `proto` · `security` · `metrics` · `deploy` · `k8s` · `ci`

### Rules
- Subject: imperative mood, lowercase, no period, max 72 characters
- Body (optional): explain *why*, not *what* — the diff shows what
- **Never add `Co-authored-by: Claude` or any AI attribution**
- Every commit must leave the repo with passing tests
- Commit logical units, not one mega-commit per step

### Examples
```
feat(wal): implement segmented write-ahead log with CRC32 integrity
fix(raft): handle split vote by resetting election timeout
test(chunk): add SHA-256 corruption detection test
refactor(metadata): extract lease manager into dedicated file
docs(step-logs): add Step 3 completion log
chore(ci): add Codecov upload to GitHub Actions test job
perf(chunk): use sync.Pool for chunk read buffers
```

---

## Go Style Rules

### Package comments (required on every package)
```go
// Package wal implements a write-ahead log for durable, ordered entry storage.
// Entries are written to disk and fsynced before the caller receives an ack,
// ensuring no committed entry is lost on sudden process termination.
package wal
```

### Error handling
```go
// Wrap with context — caller should know where the error came from
return fmt.Errorf("chunk store: write chunk %s: %w", id, err)

// Sentinel errors at package level
var (
    ErrChunkNotFound  = errors.New("chunk: not found")
    ErrChecksumFailed = errors.New("chunk: SHA-256 mismatch, data corrupted")
)

// Never discard errors
n, err := w.Write(data)
if err != nil {
    return fmt.Errorf("wal: write entry data: %w", err)
}
_ = n // explicit discard if truly unused
```

### Interfaces — small, consumer-side
```go
// In the package that USES the log, define what it needs:
type LogAppender interface {
    Append(entry Entry) error
}
// Not in the WAL package. The WAL exports the concrete type.
```

### Context — everywhere blocking
```go
// Every function that does I/O or can block:
func (cs *ChunkServer) ReadChunk(ctx context.Context, id ChunkID) ([]byte, error)

// Honor context in loops:
select {
case <-ctx.Done():
    return ctx.Err()
case entry := <-entries:
    // process entry
}
```

### Mutexes — always documented
```go
type Master struct {
    mu        sync.RWMutex  // protects chunkMap, namespace, and leases
    chunkMap  map[ChunkID][]NodeID
    namespace *NamespaceTree
    leases    map[ChunkID]*Lease
}
```

### Tests — always table-driven
```go
func TestVectorClockHappensBefore(t *testing.T) {
    tests := []struct {
        name   string
        a, b   VectorClock
        result bool
    }{
        {
            name:   "a happens before b",
            a:      VectorClock{"n1": 1, "n2": 0},
            b:      VectorClock{"n1": 2, "n2": 1},
            result: true,
        },
        {
            name:   "concurrent events",
            a:      VectorClock{"n1": 2, "n2": 0},
            b:      VectorClock{"n1": 0, "n2": 2},
            result: false,
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := tt.a.HappensBefore(tt.b)
            if got != tt.result {
                t.Errorf("HappensBefore = %v, want %v", got, tt.result)
            }
        })
    }
}
```

### Temporary directories in tests
```go
// Always use t.TempDir() — cleaned up automatically, never hardcode paths
func TestWALCrashRecovery(t *testing.T) {
    dir := t.TempDir()
    w, err := wal.Open(dir)
    // ...
}
```

### Structured logging
```go
// log/slog (stdlib, Go 1.21+)
slog.Info("chunk replicated",
    "chunk_id", id,
    "primary", primary,
    "secondaries", secondaries,
    "duration_ms", duration.Milliseconds(),
)

slog.Warn("heartbeat missed",
    "node_id", nodeID,
    "last_seen", lastSeen,
    "missed_count", count,
)

// Debug for per-operation noise
slog.Debug("wal entry appended", "index", entry.Index, "size", len(entry.Data))
```

### Naming
```go
// No stutter — the package name is already the namespace
raft.Node          // not raft.RaftNode
wal.Entry          // not wal.WALEntry
chunk.Store        // not chunk.ChunkStore

// Receivers: short and consistent
func (m *Master) GrantLease(...)       // m for Master
func (cs *ChunkServer) ReadChunk(...)  // cs for ChunkServer
func (w *WAL) Append(...)             // w for WAL

// Exported constants as typed constants, not iota strings
type NodeState int
const (
    Follower  NodeState = iota
    Candidate
    Leader
)
```

---

## What NOT To Do (hard rules)

```go
// NO panic in library code
panic("something went wrong")  // NEVER — return the error

// NO init() functions
func init() { ... }  // NEVER — explicit initialization in constructors

// NO global state
var globalMaster *Master  // NEVER — inject via constructor

// NO untyped any where avoidable
func Process(data any) any  // NEVER if concrete types work

// NO log.Fatal outside main
log.Fatal("something bad")  // NEVER in internal/ or pkg/ — return error

// NO skipping errors
json.Marshal(v)  // NEVER — assign and check the error

// NO fmt.Println in library code
fmt.Println("debug info")  // NEVER — use slog.Debug
```

---

## Step Log Template

After each step completes, create `docs/step-logs/STEP_N.md` with this structure:

```markdown
# Step N — [Step Name]

## What was built

| File | Description |
|------|-------------|
| `internal/wal/wal.go` | Core WAL struct with Open, Append, ReadAll, Close |
| `internal/wal/segment.go` | Segment file rotation when size exceeds limit |
| ... | ... |

## Why each piece was built this way

### WAL segment rotation
**What we chose:** Rolling segments capped at 64MB.
**Alternatives considered:** Single file (unbounded growth, slow recovery), fixed-count segments.
**Why this choice:** Bounded recovery time (only scan current + previous segment), easy to GC old segments after snapshots.

### [Next decision]
...

## How this connects to the final goal

[Explain which later steps depend on what was built here, and how.]

## Why this matters (technically and for the portfolio)

[What does this demonstrate? What would an interviewer or reviewer notice?]

## Tests added

| Test | What it covers |
|------|---------------|
| `TestWALCrashRecovery` | Partial write followed by restart; verifies only committed entries are replayed |
| ... | ... |

## How to verify this step manually

\```bash
cd internal/wal && go test -v -race ./...
# Expected: all tests pass, no race conditions
\```
```
