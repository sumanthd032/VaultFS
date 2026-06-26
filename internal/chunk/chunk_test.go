package chunk

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

// -- hasher -------------------------------------------------------------------

func TestHashDeterministic(t *testing.T) {
	a := Hash([]byte("hello"))
	b := Hash([]byte("hello"))
	if a != b {
		t.Errorf("Hash not deterministic: %s != %s", a, b)
	}
	if !a.Valid() {
		t.Errorf("Hash produced invalid id: %s", a)
	}
}

func TestVerify(t *testing.T) {
	data := []byte("payload")
	id := Hash(data)

	tests := []struct {
		name    string
		id      ChunkID
		data    []byte
		wantErr bool
	}{
		{"matching", id, data, false},
		{"corrupted data", id, []byte("payloaX"), true},
		{"wrong id", Hash([]byte("other")), data, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Verify(tt.id, tt.data)
			if tt.wantErr && !errors.Is(err, ErrChecksumFailed) {
				t.Errorf("got %v, want ErrChecksumFailed", err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestChunkIDValid(t *testing.T) {
	tests := []struct {
		name string
		id   ChunkID
		want bool
	}{
		{"valid hash", Hash([]byte("x")), true},
		{"too short", "abc", false},
		{"uppercase rejected", "AA" + Hash([]byte("x"))[2:], false},
		{"non-hex char", ChunkID("g" + string(Hash([]byte("x"))[1:])), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.id.Valid(); got != tt.want {
				t.Errorf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

// -- store roundtrip ----------------------------------------------------------

func TestStoreWriteReadRoundtrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	tests := []struct {
		name string
		data []byte
	}{
		{"small", []byte("hello world")},
		{"empty", []byte{}},
		{"binary", []byte{0x00, 0xff, 0x42, 0x13, 0x37}},
		{"large", make([]byte, 1<<20)}, // 1 MiB of zeros
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := s.WriteChunk(ctx, tt.data)
			if err != nil {
				t.Fatalf("WriteChunk: %v", err)
			}
			if id != Hash(tt.data) {
				t.Errorf("returned id %s != Hash %s", id, Hash(tt.data))
			}
			got, err := s.ReadChunk(ctx, id)
			if err != nil {
				t.Fatalf("ReadChunk: %v", err)
			}
			if len(got) != len(tt.data) {
				t.Errorf("read %d bytes, wrote %d", len(got), len(tt.data))
			}
		})
	}
}

func TestStoreWriteIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	data := []byte("idempotent")

	id1, err := s.WriteChunk(ctx, data)
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	id2, err := s.WriteChunk(ctx, data)
	if err != nil {
		t.Fatalf("second write: %v", err)
	}
	if id1 != id2 {
		t.Errorf("idempotent write produced different ids: %s, %s", id1, id2)
	}
}

func TestStoreReadNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.ReadChunk(context.Background(), Hash([]byte("absent")))
	if !errors.Is(err, ErrChunkNotFound) {
		t.Errorf("got %v, want ErrChunkNotFound", err)
	}
}

// TestStoreReadCorruptionDetected flips a bit on disk and verifies the read
// fails the SHA-256 check rather than returning bad data.
func TestStoreReadCorruptionDetected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	data := []byte("important data that must not corrupt silently")

	id, err := s.WriteChunk(ctx, data)
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Corrupt the chunk file directly on disk.
	p := s.path(id)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read raw chunk: %v", err)
	}
	raw[0] ^= 0xFF
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatalf("corrupt chunk: %v", err)
	}

	_, err = s.ReadChunk(ctx, id)
	if !errors.Is(err, ErrChecksumFailed) {
		t.Errorf("corruption not detected: got %v, want ErrChecksumFailed", err)
	}
}

func TestStoreDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id, _ := s.WriteChunk(ctx, []byte("delete me"))

	if err := s.DeleteChunk(ctx, id); err != nil {
		t.Fatalf("DeleteChunk: %v", err)
	}
	if s.Has(id) {
		t.Error("chunk still present after delete")
	}
	// Deleting again is a no-op.
	if err := s.DeleteChunk(ctx, id); err != nil {
		t.Errorf("second delete should be a no-op, got %v", err)
	}
}

func TestStoreListChunks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	want := map[ChunkID]struct{}{}
	for _, d := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		id, _ := s.WriteChunk(ctx, d)
		want[id] = struct{}{}
	}
	got, err := s.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("ListChunks returned %d, want %d", len(got), len(want))
	}
	for _, id := range got {
		if _, ok := want[id]; !ok {
			t.Errorf("unexpected chunk %s", id)
		}
	}
}

func TestStoreFanoutLayout(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id, _ := s.WriteChunk(ctx, []byte("layout"))

	wantDir := filepath.Join(s.root, string(id[:fanoutLen]))
	if _, err := os.Stat(wantDir); err != nil {
		t.Errorf("fanout dir %s not created: %v", wantDir, err)
	}
}

func TestStoreConcurrentWrites(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Half write the same chunk (idempotency race), half unique chunks.
			data := []byte("shared")
			if n%2 == 0 {
				data = []byte{byte(n)}
			}
			if _, err := s.WriteChunk(ctx, data); err != nil {
				t.Errorf("concurrent WriteChunk: %v", err)
			}
		}(i)
	}
	wg.Wait()
}

// -- replication --------------------------------------------------------------

// fakeSender routes SendChunk to an in-memory map of node replicators,
// simulating a pipeline of chunk servers.
type fakeSender struct {
	nodes map[string]*Replicator
}

func (f *fakeSender) SendChunk(ctx context.Context, target string, id ChunkID, data []byte, downstream []string) error {
	r, ok := f.nodes[target]
	if !ok {
		return errors.New("fakeSender: unknown target " + target)
	}
	_, err := r.Replicate(ctx, data, downstream)
	return err
}

func TestPipelineReplicationToThreeNodes(t *testing.T) {
	ctx := context.Background()
	sender := &fakeSender{nodes: make(map[string]*Replicator)}

	// Three chunk servers, each with its own store.
	stores := make(map[string]*Store)
	for _, id := range []string{"primary", "secondary-1", "secondary-2"} {
		st := newTestStore(t)
		stores[id] = st
		sender.nodes[id] = NewReplicator(st, sender)
	}

	data := []byte("replicate me across the chain")
	// Primary stores locally and forwards through both secondaries.
	id, err := sender.nodes["primary"].Replicate(ctx, data, []string{"secondary-1", "secondary-2"})
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	// All three replicas must hold the chunk with verifiable integrity.
	for node, st := range stores {
		got, err := st.ReadChunk(ctx, id)
		if err != nil {
			t.Errorf("node %s missing chunk: %v", node, err)
			continue
		}
		if string(got) != string(data) {
			t.Errorf("node %s data mismatch", node)
		}
	}
}

func TestReplicationTailReturnsWithoutForwarding(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	// sender is nil because a tail node (empty chain) must never call it.
	r := NewReplicator(st, nil)
	id, err := r.Replicate(ctx, []byte("tail"), nil)
	if err != nil {
		t.Fatalf("Replicate tail: %v", err)
	}
	if !st.Has(id) {
		t.Error("tail node did not store chunk")
	}
}

func TestReplicationChainFailurePropagates(t *testing.T) {
	ctx := context.Background()
	sender := &fakeSender{nodes: make(map[string]*Replicator)}
	sender.nodes["primary"] = NewReplicator(newTestStore(t), sender)
	// "dead" is not registered, so forwarding to it fails.

	_, err := sender.nodes["primary"].Replicate(ctx, []byte("x"), []string{"dead"})
	if err == nil {
		t.Error("expected error when downstream node is unreachable")
	}
}

// -- garbage collection -------------------------------------------------------

// fakeRefs is a ReferenceChecker backed by a fixed set of referenced chunk IDs.
type fakeRefs struct {
	mu  sync.Mutex
	set map[ChunkID]struct{}
}

func newFakeRefs(ids ...ChunkID) *fakeRefs {
	r := &fakeRefs{set: make(map[ChunkID]struct{})}
	for _, id := range ids {
		r.set[id] = struct{}{}
	}
	return r
}

func (r *fakeRefs) IsReferenced(id ChunkID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.set[id]
	return ok
}

func TestGCDeletesOrphansKeepsReferenced(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	keep, _ := s.WriteChunk(ctx, []byte("referenced"))
	orphan, _ := s.WriteChunk(ctx, []byte("orphaned"))

	refs := newFakeRefs(keep)
	gc := NewGC(s, refs, 0) // zero grace: delete on first sweep

	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if !s.Has(keep) {
		t.Error("referenced chunk was deleted")
	}
	if s.Has(orphan) {
		t.Error("orphaned chunk was not deleted")
	}
}

func TestGCRespectsGracePeriod(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	orphan, _ := s.WriteChunk(ctx, []byte("young orphan"))

	refs := newFakeRefs() // nothing referenced
	gc := NewGC(s, refs, 10*time.Minute)

	base := time.Now()
	gc.now = func() time.Time { return base }

	// First sweep only records the orphan; grace not yet elapsed.
	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("first Sweep: %v", err)
	}
	if !s.Has(orphan) {
		t.Fatal("orphan deleted before grace period elapsed")
	}

	// Advance the clock beyond the grace period.
	gc.now = func() time.Time { return base.Add(11 * time.Minute) }
	if err := gc.Sweep(ctx); err != nil {
		t.Fatalf("second Sweep: %v", err)
	}
	if s.Has(orphan) {
		t.Error("orphan not deleted after grace period elapsed")
	}
}

func TestGCResetsTimerWhenReReferenced(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	id, _ := s.WriteChunk(ctx, []byte("flapping"))

	refs := newFakeRefs()
	gc := NewGC(s, refs, time.Hour)
	base := time.Now()
	gc.now = func() time.Time { return base }

	// Sweep once: records orphan timer.
	_ = gc.Sweep(ctx)

	// It becomes referenced again before the grace period elapses.
	refs.mu.Lock()
	refs.set[id] = struct{}{}
	refs.mu.Unlock()

	gc.now = func() time.Time { return base.Add(2 * time.Hour) }
	_ = gc.Sweep(ctx)

	if !s.Has(id) {
		t.Error("chunk deleted even though it was re-referenced")
	}
}

// -- heartbeat sender ---------------------------------------------------------

// fakeReporter records heartbeat inventories and signals on a channel.
type fakeReporter struct {
	got chan []ChunkID
}

func (r *fakeReporter) ReportHeartbeat(_ context.Context, _ string, chunks []ChunkID) error {
	select {
	case r.got <- chunks:
	default:
	}
	return nil
}

func TestHeartbeatSenderReportsInventory(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id, _ := s.WriteChunk(ctx, []byte("inventory item"))
	reporter := &fakeReporter{got: make(chan []ChunkID, 4)}

	sender := NewHeartbeatSender("node-A", s, reporter, 10*time.Millisecond)
	go sender.Run(ctx)

	select {
	case chunks := <-reporter.got:
		if len(chunks) != 1 || chunks[0] != id {
			t.Errorf("reported inventory %v, want [%s]", chunks, id)
		}
	case <-time.After(time.Second):
		t.Fatal("no heartbeat received within 1 s")
	}
}

func TestHeartbeatSenderDefaultInterval(t *testing.T) {
	s := newTestStore(t)
	sender := NewHeartbeatSender("n", s, &fakeReporter{got: make(chan []ChunkID, 1)}, 0)
	if sender.interval != DefaultHeartbeatInterval {
		t.Errorf("interval = %v, want default %v", sender.interval, DefaultHeartbeatInterval)
	}
}

func TestStoreCount(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if n, err := s.Count(); err != nil || n != 0 {
		t.Fatalf("empty count = %d, %v; want 0, nil", n, err)
	}
	if _, err := s.WriteChunk(ctx, []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteChunk(ctx, []byte("beta")); err != nil {
		t.Fatal(err)
	}
	// Content-addressed: re-writing the same bytes does not add a chunk.
	if _, err := s.WriteChunk(ctx, []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Count(); err != nil || n != 2 {
		t.Fatalf("count = %d, %v; want 2, nil", n, err)
	}
}
