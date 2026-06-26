package client

import (
	"bytes"
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

// -- fake chunk server --------------------------------------------------------

// fakeChunkServer stores chunks in memory and forwards along the replication
// chain, mimicking a real chunk server's pipeline behaviour.
type fakeChunkServer struct {
	vaultfsv1.UnimplementedChunkServiceServer
	addr string

	mu   sync.Mutex
	data map[string][]byte
}

func (s *fakeChunkServer) WriteChunk(ctx context.Context, req *vaultfsv1.WriteChunkRequest) (*vaultfsv1.WriteChunkResponse, error) {
	s.mu.Lock()
	s.data[req.GetChunkId()] = append([]byte(nil), req.GetData()...)
	s.mu.Unlock()

	if ds := req.GetDownstream(); len(ds) > 0 {
		cc, err := grpc.NewClient(ds[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		defer func() { _ = cc.Close() }()
		_, err = vaultfsv1.NewChunkServiceClient(cc).WriteChunk(ctx, &vaultfsv1.WriteChunkRequest{
			ChunkId: req.GetChunkId(), Data: req.GetData(), Downstream: ds[1:],
		})
		if err != nil {
			return nil, err
		}
	}
	return &vaultfsv1.WriteChunkResponse{ChunkId: req.GetChunkId()}, nil
}

func (s *fakeChunkServer) ReadChunk(_ context.Context, req *vaultfsv1.ReadChunkRequest) (*vaultfsv1.ReadChunkResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.data[req.GetChunkId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "chunk %s not found", req.GetChunkId())
	}
	return &vaultfsv1.ReadChunkResponse{Data: data}, nil
}

func (s *fakeChunkServer) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[id]
	return ok
}

func (s *fakeChunkServer) corrupt(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d, ok := s.data[id]; ok && len(d) > 0 {
		d[0] ^= 0xFF
	}
}

// -- fake master --------------------------------------------------------------

// fakeMaster implements MasterService and AdminService over an in-memory
// namespace, planning every chunk onto the provided chunk servers.
type fakeMaster struct {
	vaultfsv1.UnimplementedMasterServiceServer
	vaultfsv1.UnimplementedAdminServiceServer

	chunkNodes []*vaultfsv1.NodeInfo

	mu    sync.Mutex
	files map[string]*vaultfsv1.FileInfo
}

func newFakeMaster(chunkNodes []*vaultfsv1.NodeInfo) *fakeMaster {
	return &fakeMaster{
		chunkNodes: chunkNodes,
		files:      make(map[string]*vaultfsv1.FileInfo),
	}
}

func (m *fakeMaster) OpenForWrite(_ context.Context, req *vaultfsv1.OpenForWriteRequest) (*vaultfsv1.OpenForWriteResponse, error) {
	placements := make([]*vaultfsv1.ChunkLocation, len(req.GetChunkIds()))
	for i, id := range req.GetChunkIds() {
		placements[i] = &vaultfsv1.ChunkLocation{ChunkId: id, Nodes: m.chunkNodes}
	}
	return &vaultfsv1.OpenForWriteResponse{Placements: placements}, nil
}

func (m *fakeMaster) FinalizeWrite(_ context.Context, req *vaultfsv1.FinalizeWriteRequest) (*vaultfsv1.FinalizeWriteResponse, error) {
	now := time.Now().Unix()
	fi := &vaultfsv1.FileInfo{
		Path: req.GetPath(), Size: req.GetSize(), ChunkIds: req.GetChunkIds(),
		CreatedAtUnix: now, UpdatedAtUnix: now,
	}
	m.mu.Lock()
	m.files[req.GetPath()] = fi
	m.mu.Unlock()
	return &vaultfsv1.FinalizeWriteResponse{File: fi}, nil
}

func (m *fakeMaster) OpenForRead(_ context.Context, req *vaultfsv1.OpenForReadRequest) (*vaultfsv1.OpenForReadResponse, error) {
	m.mu.Lock()
	fi, ok := m.files[req.GetPath()]
	m.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "file %s not found", req.GetPath())
	}
	locs := make([]*vaultfsv1.ChunkLocation, len(fi.GetChunkIds()))
	for i, id := range fi.GetChunkIds() {
		locs[i] = &vaultfsv1.ChunkLocation{ChunkId: id, Nodes: m.chunkNodes}
	}
	return &vaultfsv1.OpenForReadResponse{File: fi, Locations: locs}, nil
}

func (m *fakeMaster) Stat(_ context.Context, req *vaultfsv1.StatRequest) (*vaultfsv1.StatResponse, error) {
	m.mu.Lock()
	fi, ok := m.files[req.GetPath()]
	m.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "file %s not found", req.GetPath())
	}
	return &vaultfsv1.StatResponse{File: fi}, nil
}

func (m *fakeMaster) ListDir(_ context.Context, _ *vaultfsv1.ListDirRequest) (*vaultfsv1.ListDirResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var entries []*vaultfsv1.FileInfo
	for _, fi := range m.files {
		entries = append(entries, fi)
	}
	return &vaultfsv1.ListDirResponse{Entries: entries}, nil
}

func (m *fakeMaster) DeleteFile(_ context.Context, req *vaultfsv1.DeleteFileRequest) (*vaultfsv1.DeleteFileResponse, error) {
	m.mu.Lock()
	delete(m.files, req.GetPath())
	m.mu.Unlock()
	return &vaultfsv1.DeleteFileResponse{}, nil
}

func (m *fakeMaster) ClusterStatus(_ context.Context, _ *vaultfsv1.ClusterStatusRequest) (*vaultfsv1.ClusterStatusResponse, error) {
	m.mu.Lock()
	fileCount := int64(len(m.files))
	m.mu.Unlock()
	return &vaultfsv1.ClusterStatusResponse{
		LeaderId: "master-0", Term: 1, FileCount: fileCount,
		ChunkCount: int64(len(m.chunkNodes)),
	}, nil
}

// -- harness ------------------------------------------------------------------

type fakeCluster struct {
	masterAddr string
	chunks     []*fakeChunkServer
}

// startCluster brings up one master and numChunks chunk servers on loopback,
// registering cleanup with the test.
func startCluster(t *testing.T, numChunks int) *fakeCluster {
	t.Helper()

	chunkServers := make([]*fakeChunkServer, numChunks)
	chunkNodes := make([]*vaultfsv1.NodeInfo, numChunks)
	var servers []*grpc.Server

	for i := 0; i < numChunks; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		cs := &fakeChunkServer{addr: lis.Addr().String(), data: make(map[string][]byte)}
		chunkServers[i] = cs
		chunkNodes[i] = &vaultfsv1.NodeInfo{NodeId: cs.addr, Address: cs.addr}

		gs := grpc.NewServer()
		vaultfsv1.RegisterChunkServiceServer(gs, cs)
		servers = append(servers, gs)
		go func() { _ = gs.Serve(lis) }()
	}

	master := newFakeMaster(chunkNodes)
	mlis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen master: %v", err)
	}
	mgs := grpc.NewServer()
	vaultfsv1.RegisterMasterServiceServer(mgs, master)
	vaultfsv1.RegisterAdminServiceServer(mgs, master)
	servers = append(servers, mgs)
	go func() { _ = mgs.Serve(mlis) }()

	t.Cleanup(func() {
		for _, s := range servers {
			s.Stop()
		}
	})

	return &fakeCluster{masterAddr: mlis.Addr().String(), chunks: chunkServers}
}

func newTestClient(t *testing.T, masterAddrs []string, chunkSize int) *Client {
	t.Helper()
	c, err := New(Config{MasterAddrs: masterAddrs, ChunkSize: chunkSize})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func writeLocalFile(t *testing.T, data []byte) string {
	t.Helper()
	path := t.TempDir() + "/input"
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	return path
}

// -- tests --------------------------------------------------------------------

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("New with no master addresses should fail")
	}
}

func TestPutGetRoundtrip(t *testing.T) {
	cluster := startCluster(t, 1)
	c := newTestClient(t, []string{cluster.masterAddr}, 1024) // small chunks -> multiple

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"single chunk", []byte("hello world")},
		{"multi chunk", make([]byte, 1024*3+17)},
		{"binary", []byte{0, 1, 2, 255, 254, 253, 42}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := range tt.data {
				tt.data[i] = byte(i % 251)
			}
			local := writeLocalFile(t, tt.data)
			ctx := context.Background()

			if err := c.Put(ctx, local, "/remote/"+tt.name); err != nil {
				t.Fatalf("Put: %v", err)
			}
			out := t.TempDir() + "/out"
			if err := c.Get(ctx, "/remote/"+tt.name, out); err != nil {
				t.Fatalf("Get: %v", err)
			}
			got, err := os.ReadFile(out)
			if err != nil {
				t.Fatalf("read output: %v", err)
			}
			if len(got) != len(tt.data) {
				t.Fatalf("size mismatch: got %d, want %d", len(got), len(tt.data))
			}
			for i := range got {
				if got[i] != tt.data[i] {
					t.Fatalf("byte %d mismatch", i)
				}
			}
		})
	}
}

func TestPipelineReplicationAcrossServers(t *testing.T) {
	cluster := startCluster(t, 3) // primary + 2 secondaries
	c := newTestClient(t, []string{cluster.masterAddr}, 0)

	data := []byte("replicate this chunk across the whole chain")
	local := writeLocalFile(t, data)
	if err := c.Put(context.Background(), local, "/r/file"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	id := hashChunk(data)
	for i, cs := range cluster.chunks {
		if !cs.has(id) {
			t.Errorf("chunk server %d did not receive chunk via pipeline", i)
		}
	}
}

func TestStatAndListDir(t *testing.T) {
	cluster := startCluster(t, 1)
	c := newTestClient(t, []string{cluster.masterAddr}, 0)
	ctx := context.Background()

	local := writeLocalFile(t, []byte("some content"))
	if err := c.Put(ctx, local, "/dir/a.txt"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	fi, err := c.Stat(ctx, "/dir/a.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.GetSize() != int64(len("some content")) {
		t.Errorf("Stat size = %d, want %d", fi.GetSize(), len("some content"))
	}

	entries, err := c.ListDir(ctx, "/dir")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("ListDir returned %d entries, want 1", len(entries))
	}
}

func TestStatNotFound(t *testing.T) {
	cluster := startCluster(t, 1)
	c := newTestClient(t, []string{cluster.masterAddr}, 0)
	if _, err := c.Stat(context.Background(), "/missing"); err == nil {
		t.Error("Stat of missing file should error")
	}
}

func TestDelete(t *testing.T) {
	cluster := startCluster(t, 1)
	c := newTestClient(t, []string{cluster.masterAddr}, 0)
	ctx := context.Background()

	local := writeLocalFile(t, []byte("delete me"))
	if err := c.Put(ctx, local, "/d.txt"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Delete(ctx, "/d.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Stat(ctx, "/d.txt"); err == nil {
		t.Error("file still present after delete")
	}
}

// TestGetCorruptionDetected corrupts the only replica and verifies Get fails the
// content-address integrity check rather than returning bad bytes.
func TestGetCorruptionDetected(t *testing.T) {
	cluster := startCluster(t, 1)
	c := newTestClient(t, []string{cluster.masterAddr}, 0)
	ctx := context.Background()

	data := []byte("integrity matters")
	local := writeLocalFile(t, data)
	if err := c.Put(ctx, local, "/c.txt"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	cluster.chunks[0].corrupt(hashChunk(data))

	out := t.TempDir() + "/out"
	if err := c.Get(ctx, "/c.txt", out); err == nil {
		t.Error("Get should fail when the only replica is corrupt")
	}
}

// TestGetFailsOverCorruptReplica verifies that a corrupt replica is skipped when
// a healthy replica exists.
func TestGetFailsOverCorruptReplica(t *testing.T) {
	cluster := startCluster(t, 2)
	c := newTestClient(t, []string{cluster.masterAddr}, 0)
	ctx := context.Background()

	data := []byte("two replicas, one corrupt")
	local := writeLocalFile(t, data)
	if err := c.Put(ctx, local, "/f.txt"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Corrupt only the first replica; the reader should fall through to the second.
	cluster.chunks[0].corrupt(hashChunk(data))

	out := t.TempDir() + "/out"
	if err := c.Get(ctx, "/f.txt", out); err != nil {
		t.Fatalf("Get should succeed via the healthy replica: %v", err)
	}
	got, _ := os.ReadFile(out)
	if string(got) != string(data) {
		t.Error("recovered data does not match original")
	}
}

func TestMasterFailover(t *testing.T) {
	cluster := startCluster(t, 1)
	// First address is dead (nothing listening); second is the real master.
	dead := "127.0.0.1:1" // port 1 is not listening
	c := newTestClient(t, []string{dead, cluster.masterAddr}, 0)
	ctx := context.Background()

	local := writeLocalFile(t, []byte("survives a dead master"))
	if err := c.Put(ctx, local, "/ha.txt"); err != nil {
		t.Fatalf("Put should fail over to the live master: %v", err)
	}
	if _, err := c.Stat(ctx, "/ha.txt"); err != nil {
		t.Fatalf("Stat after failover: %v", err)
	}
}

func TestStatus(t *testing.T) {
	cluster := startCluster(t, 2)
	c := newTestClient(t, []string{cluster.masterAddr}, 0)

	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.GetLeaderId() != "master-0" {
		t.Errorf("leader = %q, want master-0", st.GetLeaderId())
	}
	if st.GetChunkCount() != 2 {
		t.Errorf("chunk count = %d, want 2", st.GetChunkCount())
	}
}

// -- unit tests ---------------------------------------------------------------

func TestSplitChunks(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		chunkSize int
		want      int
	}{
		{"empty", nil, 4, 0},
		{"exact multiple", []byte("aaaabbbb"), 4, 2},
		{"with remainder", []byte("aaaabbbbc"), 4, 3},
		{"single small", []byte("ab"), 4, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks, err := splitChunks(bytes.NewReader(tt.input), tt.chunkSize)
			if err != nil {
				t.Fatalf("splitChunks: %v", err)
			}
			if len(chunks) != tt.want {
				t.Errorf("got %d chunks, want %d", len(chunks), tt.want)
			}
			// Reassembled chunks must equal the input.
			var joined []byte
			for _, ch := range chunks {
				joined = append(joined, ch.data...)
			}
			if string(joined) != string(tt.input) {
				t.Errorf("reassembled %q, want %q", joined, tt.input)
			}
		})
	}
}

func TestVerifyChunk(t *testing.T) {
	data := []byte("payload")
	id := hashChunk(data)
	if err := verifyChunk(id, data); err != nil {
		t.Errorf("matching data should verify: %v", err)
	}
	if err := verifyChunk(id, []byte("payloaX")); err == nil {
		t.Error("corrupted data should fail verification")
	}
}

func TestRetryStopsOnNonTransient(t *testing.T) {
	rp := retryPolicy{maxAttempts: 5, baseDelay: time.Millisecond, maxDelay: time.Millisecond}
	calls := 0
	err := rp.do(context.Background(), func() error {
		calls++
		return status.Error(codes.InvalidArgument, "bad")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Errorf("non-transient error should not retry: called %d times", calls)
	}
}

func TestRetrySucceedsAfterTransient(t *testing.T) {
	rp := retryPolicy{maxAttempts: 5, baseDelay: time.Millisecond, maxDelay: time.Millisecond}
	calls := 0
	err := rp.do(context.Background(), func() error {
		calls++
		if calls < 3 {
			return status.Error(codes.Unavailable, "down")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("should succeed on the 3rd attempt: %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 attempts, got %d", calls)
	}
}
