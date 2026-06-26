package chunkserver

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/sumanthd032/vaultfs/internal/chunk"
	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

// insecureDialer dials a downstream chunk server over a plaintext loopback
// connection, used only in tests.
func insecureDialer(_ context.Context, addr string) (vaultfsv1.ChunkServiceClient, func(), error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, func() {}, err
	}
	return vaultfsv1.NewChunkServiceClient(cc), func() { _ = cc.Close() }, nil
}

// startServer launches a chunk Server on loopback and returns it with its address.
func startServer(t *testing.T, nodeID string) (*Server, string) {
	t.Helper()
	store, err := chunk.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	srv := New(nodeID, store, insecureDialer)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	vaultfsv1.RegisterChunkServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	return srv, lis.Addr().String()
}

func TestWriteReadRoundtrip(t *testing.T) {
	srv, _ := startServer(t, "cs-0")
	ctx := context.Background()
	data := []byte("hello chunk")
	id := chunk.Hash(data)

	if _, err := srv.WriteChunk(ctx, &vaultfsv1.WriteChunkRequest{ChunkId: string(id), Data: data}); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	resp, err := srv.ReadChunk(ctx, &vaultfsv1.ReadChunkRequest{ChunkId: string(id)})
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if string(resp.GetData()) != string(data) {
		t.Errorf("read mismatch: got %q", resp.GetData())
	}
}

func TestWriteRejectsMismatchedID(t *testing.T) {
	srv, _ := startServer(t, "cs-0")
	_, err := srv.WriteChunk(context.Background(), &vaultfsv1.WriteChunkRequest{
		ChunkId: string(chunk.Hash([]byte("a"))), Data: []byte("different"),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("got code %v, want InvalidArgument", status.Code(err))
	}
}

func TestReadNotFound(t *testing.T) {
	srv, _ := startServer(t, "cs-0")
	_, err := srv.ReadChunk(context.Background(), &vaultfsv1.ReadChunkRequest{
		ChunkId: string(chunk.Hash([]byte("absent"))),
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("got code %v, want NotFound", status.Code(err))
	}
}

func TestDeleteChunk(t *testing.T) {
	srv, _ := startServer(t, "cs-0")
	ctx := context.Background()
	data := []byte("delete me")
	id := chunk.Hash(data)
	_, _ = srv.WriteChunk(ctx, &vaultfsv1.WriteChunkRequest{ChunkId: string(id), Data: data})

	if _, err := srv.DeleteChunk(ctx, &vaultfsv1.DeleteChunkRequest{ChunkId: string(id)}); err != nil {
		t.Fatalf("DeleteChunk: %v", err)
	}
	if _, err := srv.ReadChunk(ctx, &vaultfsv1.ReadChunkRequest{ChunkId: string(id)}); status.Code(err) != codes.NotFound {
		t.Errorf("chunk still present after delete")
	}
}

// TestPipelineForward writes to the primary with one downstream node and checks
// both servers end up holding the chunk.
func TestPipelineForward(t *testing.T) {
	primary, _ := startServer(t, "cs-primary")
	secondary, secAddr := startServer(t, "cs-secondary")
	ctx := context.Background()

	data := []byte("forward me down the chain")
	id := chunk.Hash(data)
	if _, err := primary.WriteChunk(ctx, &vaultfsv1.WriteChunkRequest{
		ChunkId: string(id), Data: data, Downstream: []string{secAddr},
	}); err != nil {
		t.Fatalf("WriteChunk with downstream: %v", err)
	}

	if _, err := secondary.ReadChunk(ctx, &vaultfsv1.ReadChunkRequest{ChunkId: string(id)}); err != nil {
		t.Errorf("secondary did not receive forwarded chunk: %v", err)
	}
}

// TestReplicateChunk pulls a chunk from a source server into a fresh server.
func TestReplicateChunk(t *testing.T) {
	source, srcAddr := startServer(t, "cs-source")
	dest, _ := startServer(t, "cs-dest")
	ctx := context.Background()

	data := []byte("re-replicate after failure")
	id := chunk.Hash(data)
	if _, err := source.WriteChunk(ctx, &vaultfsv1.WriteChunkRequest{ChunkId: string(id), Data: data}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	if _, err := dest.ReplicateChunk(ctx, &vaultfsv1.ReplicateChunkRequest{
		ChunkId: string(id), SourceAddress: srcAddr,
	}); err != nil {
		t.Fatalf("ReplicateChunk: %v", err)
	}
	if _, err := dest.ReadChunk(ctx, &vaultfsv1.ReadChunkRequest{ChunkId: string(id)}); err != nil {
		t.Errorf("destination missing replicated chunk: %v", err)
	}
}
