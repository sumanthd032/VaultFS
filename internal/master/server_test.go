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
func newTestMaster(t *testing.T, opts ...Option) *Server {
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

	srv := New(ns, leases, node, testChunkNodes(), 3, opts...)
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
	if len(resp.GetNodes()) != 3 {
		t.Errorf("nodes = %d, want 3", len(resp.GetNodes()))
	}
	// A fresh namespace holds no files or chunks.
	if resp.GetFileCount() != 0 || resp.GetChunkCount() != 0 {
		t.Errorf("file=%d chunk=%d, want 0/0", resp.GetFileCount(), resp.GetChunkCount())
	}
}

// TestClusterStatusCountsFilesAndChunks verifies the top-line counts come from
// the namespace: writing a two-chunk file makes them report 1 file, 2 chunks.
func TestClusterStatusCountsFilesAndChunks(t *testing.T) {
	s := newTestMaster(t)
	ctx := context.Background()
	if _, err := s.FinalizeWrite(ctx, &vaultfsv1.FinalizeWriteRequest{
		Path: "/a.bin", ChunkIds: []string{"chunk-a", "chunk-b"}, Size: 100,
	}); err != nil {
		t.Fatalf("FinalizeWrite: %v", err)
	}
	resp, err := s.ClusterStatus(ctx, &vaultfsv1.ClusterStatusRequest{})
	if err != nil {
		t.Fatalf("ClusterStatus: %v", err)
	}
	if resp.GetFileCount() != 1 {
		t.Errorf("file count = %d, want 1", resp.GetFileCount())
	}
	if resp.GetChunkCount() != 2 {
		t.Errorf("chunk count = %d, want 2", resp.GetChunkCount())
	}
}

// TestHeartbeatUpdatesNodeStatus verifies that a chunk server's heartbeat sets
// its reported chunk count, marks it alive, and records a heartbeat time.
func TestHeartbeatUpdatesNodeStatus(t *testing.T) {
	monitor := metadata.NewMonitor(metadata.NewChunkMap(), 0, 3)
	s := newTestMaster(t, WithMonitor(monitor), WithChunkMap(metadata.NewChunkMap()))
	ctx := context.Background()

	// Before any heartbeat the node is dead with no heartbeat time.
	before, err := s.ClusterStatus(ctx, &vaultfsv1.ClusterStatusRequest{})
	if err != nil {
		t.Fatalf("ClusterStatus: %v", err)
	}
	if before.GetNodes()[0].GetState() != vaultfsv1.NodeState_NODE_STATE_DEAD {
		t.Errorf("node state = %v, want DEAD before heartbeat", before.GetNodes()[0].GetState())
	}

	if _, err := s.Heartbeat(ctx, &vaultfsv1.HeartbeatRequest{NodeId: "cs-0", ChunkCount: 7}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	after, err := s.ClusterStatus(ctx, &vaultfsv1.ClusterStatusRequest{})
	if err != nil {
		t.Fatalf("ClusterStatus: %v", err)
	}
	var cs0 *vaultfsv1.NodeStatus
	for _, n := range after.GetNodes() {
		if n.GetNode().GetNodeId() == "cs-0" {
			cs0 = n
		}
	}
	if cs0 == nil {
		t.Fatal("cs-0 not in status")
	}
	if cs0.GetState() != vaultfsv1.NodeState_NODE_STATE_ALIVE {
		t.Errorf("state = %v, want ALIVE", cs0.GetState())
	}
	if cs0.GetChunkCount() != 7 {
		t.Errorf("chunk count = %d, want 7", cs0.GetChunkCount())
	}
	if cs0.GetLastHeartbeatUnix() == 0 {
		t.Error("last heartbeat not recorded")
	}
}

func TestHeartbeatRejectsEmptyNodeID(t *testing.T) {
	s := newTestMaster(t)
	if _, err := s.Heartbeat(context.Background(), &vaultfsv1.HeartbeatRequest{}); err == nil {
		t.Fatal("expected error for empty node_id")
	}
}
