// Package chunkserver implements the ChunkService gRPC server: the data plane
// of a single chunk server. It stores content-addressed chunks on local disk
// and forwards them along the replication chain.
package chunkserver

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sumanthd032/vaultfs/internal/chunk"
	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

// Dialer creates a ChunkServiceClient for a downstream chunk server together
// with a cleanup function. It is injected so tests can supply in-process clients
// and production can supply an mTLS gRPC dialer.
type Dialer func(ctx context.Context, addr string) (client vaultfsv1.ChunkServiceClient, cleanup func(), err error)

// Server implements vaultfsv1.ChunkServiceServer over a chunk.Store.
type Server struct {
	vaultfsv1.UnimplementedChunkServiceServer

	store  *chunk.Store
	nodeID string
	dial   Dialer
}

// New returns a chunk Server backed by store. dial is used to forward chunks to
// downstream replicas; if nil, forwarding is disabled (single-replica writes).
func New(nodeID string, store *chunk.Store, dial Dialer) *Server {
	return &Server{store: store, nodeID: nodeID, dial: dial}
}

// WriteChunk stores the chunk locally, verifying its content address, then
// forwards it to the next chunk server in the downstream chain.
func (s *Server) WriteChunk(ctx context.Context, req *vaultfsv1.WriteChunkRequest) (*vaultfsv1.WriteChunkResponse, error) {
	if err := chunk.Verify(chunk.ChunkID(req.GetChunkId()), req.GetData()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "chunk id does not match data: %v", err)
	}
	id, err := s.store.WriteChunk(ctx, req.GetData())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "store chunk: %v", err)
	}

	if ds := req.GetDownstream(); len(ds) > 0 && s.dial != nil {
		if err := s.forward(ctx, ds, req); err != nil {
			return nil, err
		}
	}
	slog.Debug("chunk stored", "node", s.nodeID, "chunk_id", id, "downstream", len(req.GetDownstream()))
	return &vaultfsv1.WriteChunkResponse{ChunkId: string(id)}, nil
}

// forward sends the chunk to the head of the downstream chain, passing the rest
// of the chain along.
func (s *Server) forward(ctx context.Context, downstream []string, req *vaultfsv1.WriteChunkRequest) error {
	next := downstream[0]
	cli, cleanup, err := s.dial(ctx, next)
	if err != nil {
		return status.Errorf(codes.Unavailable, "dial downstream %s: %v", next, err)
	}
	defer cleanup()

	if _, err := cli.WriteChunk(ctx, &vaultfsv1.WriteChunkRequest{
		ChunkId: req.GetChunkId(), Data: req.GetData(), Downstream: downstream[1:],
	}); err != nil {
		return status.Errorf(codes.Unavailable, "forward to %s: %v", next, err)
	}
	return nil
}

// ReadChunk returns the chunk's bytes, verifying integrity on the way out.
func (s *Server) ReadChunk(ctx context.Context, req *vaultfsv1.ReadChunkRequest) (*vaultfsv1.ReadChunkResponse, error) {
	data, err := s.store.ReadChunk(ctx, chunk.ChunkID(req.GetChunkId()))
	if errors.Is(err, chunk.ErrChunkNotFound) {
		return nil, status.Errorf(codes.NotFound, "chunk %s not found", req.GetChunkId())
	}
	if err != nil {
		return nil, status.Errorf(codes.DataLoss, "read chunk: %v", err)
	}
	return &vaultfsv1.ReadChunkResponse{Data: data}, nil
}

// DeleteChunk removes a chunk from local storage.
func (s *Server) DeleteChunk(ctx context.Context, req *vaultfsv1.DeleteChunkRequest) (*vaultfsv1.DeleteChunkResponse, error) {
	if err := s.store.DeleteChunk(ctx, chunk.ChunkID(req.GetChunkId())); err != nil {
		return nil, status.Errorf(codes.Internal, "delete chunk: %v", err)
	}
	return &vaultfsv1.DeleteChunkResponse{}, nil
}

// ReplicateChunk pulls a chunk from a source chunk server and stores it locally.
// It is used by the master's re-replication controller to restore the
// replication factor after a node failure.
func (s *Server) ReplicateChunk(ctx context.Context, req *vaultfsv1.ReplicateChunkRequest) (*vaultfsv1.ReplicateChunkResponse, error) {
	if s.dial == nil {
		return nil, status.Error(codes.Unimplemented, "replication dialer not configured")
	}
	cli, cleanup, err := s.dial(ctx, req.GetSourceAddress())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial source %s: %v", req.GetSourceAddress(), err)
	}
	defer cleanup()

	resp, err := cli.ReadChunk(ctx, &vaultfsv1.ReadChunkRequest{ChunkId: req.GetChunkId()})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "fetch from source: %v", err)
	}
	id, err := s.store.WriteChunk(ctx, resp.GetData())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "store replicated chunk: %v", err)
	}
	return &vaultfsv1.ReplicateChunkResponse{ChunkId: string(id)}, nil
}
