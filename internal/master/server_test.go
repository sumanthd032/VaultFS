package master

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sumanthd032/vaultfs/internal/metadata"
	"github.com/sumanthd032/vaultfs/internal/raft"
	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

func testChunkNodes() []*vaultfsv1.NodeInfo {
	return []*vaultfsv1.NodeInfo{
		{NodeId: "cs-0", Address: "cs-0:9100"},
		{NodeId: "cs-1", Address: "cs-1:9100"},
		{NodeId: "cs-2", Address: "cs-2:9100"},
	}
}

// newTestMaster builds a single-node Raft master that elects itself leader.
func newTestMaster(t *testing.T) *Server {
	t.Helper()
	store, err := metadata.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ns := metadata.NewNamespace(store)
	leases := metadata.NewLeaseManager(time.Minute)

	commitCh := make(chan raft.Entry, 64)
	cfg := raft.Config{
		ID:                 "m0",
		ElectionMinTimeout: 20 * time.Millisecond,
		ElectionMaxTimeout: 40 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
		CommitCh:           commitCh,
	}
	node := raft.New(cfg)
	net := raft.NewInMemNetwork()
	node.Start(net.Transport("m0"))

	srv := New(ns, leases, node, testChunkNodes(), 3)
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Run(ctx, commitCh)

	t.Cleanup(func() {
		cancel()
		node.Stop()
		_ = store.Close()
	})

	waitLeader(t, node)
	return srv
}

func waitLeader(t *testing.T, node *raft.Node) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if node.State() == raft.Leader {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("node did not become leader in time")
}

func TestCreateAndStat(t *testing.T) {
	s := newTestMaster(t)
	ctx := context.Background()

	if _, err := s.CreateFile(ctx, &vaultfsv1.CreateFileRequest{Path: "/a.txt"}); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	resp, err := s.Stat(ctx, &vaultfsv1.StatRequest{Path: "/a.txt"})
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if resp.GetFile().GetPath() != "/a.txt" {
		t.Errorf("path = %q, want /a.txt", resp.GetFile().GetPath())
	}
}

func TestStatNotFound(t *testing.T) {
	s := newTestMaster(t)
	_, err := s.Stat(context.Background(), &vaultfsv1.StatRequest{Path: "/missing"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("got %v, want NotFound", status.Code(err))
	}
}

func TestFinalizeAndOpenForRead(t *testing.T) {
	s := newTestMaster(t)
	ctx := context.Background()
	chunkIDs := []string{"chunk-a", "chunk-b"}

	if _, err := s.FinalizeWrite(ctx, &vaultfsv1.FinalizeWriteRequest{
		Path: "/data.bin", ChunkIds: chunkIDs, Size: 2048,
	}); err != nil {
		t.Fatalf("FinalizeWrite: %v", err)
	}

	resp, err := s.OpenForRead(ctx, &vaultfsv1.OpenForReadRequest{Path: "/data.bin"})
	if err != nil {
		t.Fatalf("OpenForRead: %v", err)
	}
	if resp.GetFile().GetSize() != 2048 {
		t.Errorf("size = %d, want 2048", resp.GetFile().GetSize())
	}
	if len(resp.GetLocations()) != 2 {
		t.Fatalf("locations = %d, want 2", len(resp.GetLocations()))
	}
	if len(resp.GetLocations()[0].GetNodes()) != 3 {
		t.Errorf("replicas = %d, want 3", len(resp.GetLocations()[0].GetNodes()))
	}
}

func TestOpenForWritePlacement(t *testing.T) {
	s := newTestMaster(t)
	resp, err := s.OpenForWrite(context.Background(), &vaultfsv1.OpenForWriteRequest{
		Path: "/x", ChunkIds: []string{"c1", "c2"},
	})
	if err != nil {
		t.Fatalf("OpenForWrite: %v", err)
	}
	if len(resp.GetPlacements()) != 2 {
		t.Fatalf("placements = %d, want 2", len(resp.GetPlacements()))
	}
	for _, p := range resp.GetPlacements() {
		if len(p.GetNodes()) != 3 {
			t.Errorf("placement nodes = %d, want 3 (replication factor)", len(p.GetNodes()))
		}
	}
}

func TestDelete(t *testing.T) {
	s := newTestMaster(t)
	ctx := context.Background()
	_, _ = s.FinalizeWrite(ctx, &vaultfsv1.FinalizeWriteRequest{Path: "/d.txt", ChunkIds: []string{"c"}, Size: 1})

	if _, err := s.DeleteFile(ctx, &vaultfsv1.DeleteFileRequest{Path: "/d.txt"}); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if _, err := s.Stat(ctx, &vaultfsv1.StatRequest{Path: "/d.txt"}); status.Code(err) != codes.NotFound {
		t.Errorf("file still present after delete")
	}
}

func TestListDir(t *testing.T) {
	s := newTestMaster(t)
	ctx := context.Background()
	_, _ = s.FinalizeWrite(ctx, &vaultfsv1.FinalizeWriteRequest{Path: "/dir/a", ChunkIds: []string{"c"}, Size: 1})
	_, _ = s.FinalizeWrite(ctx, &vaultfsv1.FinalizeWriteRequest{Path: "/dir/b", ChunkIds: []string{"c"}, Size: 1})

	resp, err := s.ListDir(ctx, &vaultfsv1.ListDirRequest{Path: "/dir"})
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(resp.GetEntries()) != 2 {
		t.Errorf("entries = %d, want 2", len(resp.GetEntries()))
	}
}

func TestGetLease(t *testing.T) {
	s := newTestMaster(t)
	ctx := context.Background()

	resp, err := s.GetLease(ctx, &vaultfsv1.GetLeaseRequest{ChunkId: "c1", Holder: "cs-0"})
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if resp.GetLease().GetHolder() != "cs-0" {
		t.Errorf("holder = %q, want cs-0", resp.GetLease().GetHolder())
	}
	// A different holder is refused while the lease is valid.
	if _, err := s.GetLease(ctx, &vaultfsv1.GetLeaseRequest{ChunkId: "c1", Holder: "cs-1"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("got %v, want FailedPrecondition", status.Code(err))
	}
}

func TestClusterStatus(t *testing.T) {
	s := newTestMaster(t)
	resp, err := s.ClusterStatus(context.Background(), &vaultfsv1.ClusterStatusRequest{})
	if err != nil {
		t.Fatalf("ClusterStatus: %v", err)
	}
	if resp.GetLeaderId() != "m0" {
		t.Errorf("leader = %q, want m0", resp.GetLeaderId())
	}
	if resp.GetChunkCount() != 3 {
		t.Errorf("chunk count = %d, want 3", resp.GetChunkCount())
	}
}
