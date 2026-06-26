// Package master implements the MasterService and AdminService gRPC servers.
//
// The master owns the namespace and coordinates writes. Namespace mutations are
// replicated through the Raft log: a write RPC proposes a command, waits for it
// to be committed and applied on the local node, then responds, giving
// read-your-writes consistency. Reads are served from the locally applied state.
package master

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sumanthd032/vaultfs/internal/metadata"
	"github.com/sumanthd032/vaultfs/internal/raft"
	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

// opKind enumerates the replicated namespace mutations.
type opKind uint8

const (
	opCreate opKind = iota
	opFinalize
	opDelete
)

// command is the unit replicated through the Raft log.
type command struct {
	OpID     string
	Kind     opKind
	Path     string
	ChunkIDs []string
	Size     int64
}

// Server implements MasterService and AdminService backed by a Raft-replicated
// namespace.
type Server struct {
	vaultfsv1.UnimplementedMasterServiceServer
	vaultfsv1.UnimplementedAdminServiceServer

	ns         *metadata.Namespace
	leases     *metadata.LeaseManager
	chunkNodes []*vaultfsv1.NodeInfo
	rf         int
	node       *raft.Node

	opSeq atomic.Uint64

	mu      sync.Mutex // protects waiters
	waiters map[string]chan error
}

// New creates a master Server. node is a started Raft node whose committed
// entries arrive on commitCh; the caller wires those together. chunkNodes is the
// set of chunk servers available for placement and rf is the replication factor.
func New(ns *metadata.Namespace, leases *metadata.LeaseManager, node *raft.Node, chunkNodes []*vaultfsv1.NodeInfo, rf int) *Server {
	if rf <= 0 {
		rf = metadata.DefaultReplicationFactor
	}
	return &Server{
		ns:         ns,
		leases:     leases,
		chunkNodes: chunkNodes,
		rf:         rf,
		node:       node,
		waiters:    make(map[string]chan error),
	}
}

// Run consumes committed entries until ctx is cancelled, applying each to the
// namespace and waking any waiter blocked on that command.
func (s *Server) Run(ctx context.Context, commitCh <-chan raft.Entry) {
	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-commitCh:
			cmd, err := decode(entry.Command)
			if err != nil {
				continue
			}
			applyErr := s.apply(cmd)
			s.mu.Lock()
			ch := s.waiters[cmd.OpID]
			s.mu.Unlock()
			if ch != nil {
				ch <- applyErr
			}
		}
	}
}

// apply mutates the local namespace for a committed command.
func (s *Server) apply(cmd command) error {
	switch cmd.Kind {
	case opCreate:
		if _, err := s.ns.Stat(cmd.Path); err == nil {
			return nil // already present (idempotent replay)
		}
		if err := s.ensureParents(cmd.Path); err != nil {
			return err
		}
		return s.ns.CreateFile(metadata.FileInfo{Path: cmd.Path})
	case opFinalize:
		fi := metadata.FileInfo{Path: cmd.Path, Size: cmd.Size, ChunkIDs: cmd.ChunkIDs}
		if _, err := s.ns.Stat(cmd.Path); err == nil {
			return s.ns.UpdateFile(fi)
		}
		if err := s.ensureParents(cmd.Path); err != nil {
			return err
		}
		return s.ns.CreateFile(fi)
	case opDelete:
		if err := s.ns.DeleteFile(cmd.Path); err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		return nil
	default:
		return fmt.Errorf("master: unknown op kind %d", cmd.Kind)
	}
}

// ensureParents creates any missing ancestor directories of path, giving
// writes mkdir -p semantics. It runs inside apply, so it is deterministic
// across Raft nodes.
func (s *Server) ensureParents(path string) error {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	cur := ""
	for i := 0; i < len(parts)-1; i++ {
		cur += "/" + parts[i]
		if _, err := s.ns.Stat(cur); err == nil {
			continue
		} else if !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		if err := s.ns.CreateDir(cur); err != nil && !errors.Is(err, metadata.ErrAlreadyExists) {
			return err
		}
	}
	return nil
}

// propose replicates cmd through Raft and blocks until it is applied locally.
func (s *Server) propose(ctx context.Context, cmd command) error {
	cmd.OpID = fmt.Sprintf("%d", s.opSeq.Add(1))
	ch := make(chan error, 1)
	s.mu.Lock()
	s.waiters[cmd.OpID] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.waiters, cmd.OpID)
		s.mu.Unlock()
	}()

	data, err := encode(cmd)
	if err != nil {
		return status.Errorf(codes.Internal, "encode command: %v", err)
	}
	if err := s.node.Propose(ctx, data); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return status.Error(codes.Unavailable, "not the leader")
		}
		return status.Errorf(codes.Internal, "propose: %v", err)
	}
	select {
	case err := <-ch:
		if err != nil {
			return status.Errorf(codes.Internal, "apply: %v", err)
		}
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

// -- MasterService reads ------------------------------------------------------

// Stat returns metadata for a path.
func (s *Server) Stat(_ context.Context, req *vaultfsv1.StatRequest) (*vaultfsv1.StatResponse, error) {
	fi, err := s.ns.Stat(req.GetPath())
	if errors.Is(err, metadata.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "%s not found", req.GetPath())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stat: %v", err)
	}
	return &vaultfsv1.StatResponse{File: fileInfoToProto(fi)}, nil
}

// ListDir lists the direct children of a directory.
func (s *Server) ListDir(_ context.Context, req *vaultfsv1.ListDirRequest) (*vaultfsv1.ListDirResponse, error) {
	entries, err := s.ns.ListDir(req.GetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dir: %v", err)
	}
	out := make([]*vaultfsv1.FileInfo, len(entries))
	for i, e := range entries {
		out[i] = fileInfoToProto(e)
	}
	return &vaultfsv1.ListDirResponse{Entries: out}, nil
}

// OpenForRead returns a file's metadata and the locations of its chunks.
func (s *Server) OpenForRead(_ context.Context, req *vaultfsv1.OpenForReadRequest) (*vaultfsv1.OpenForReadResponse, error) {
	fi, err := s.ns.Stat(req.GetPath())
	if errors.Is(err, metadata.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "%s not found", req.GetPath())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open for read: %v", err)
	}
	locs := make([]*vaultfsv1.ChunkLocation, len(fi.ChunkIDs))
	for i, id := range fi.ChunkIDs {
		locs[i] = &vaultfsv1.ChunkLocation{ChunkId: id, Nodes: s.placement()}
	}
	return &vaultfsv1.OpenForReadResponse{File: fileInfoToProto(fi), Locations: locs}, nil
}

// OpenForWrite plans replica placement for the requested chunks. It does not
// mutate state; the file is recorded by FinalizeWrite.
func (s *Server) OpenForWrite(_ context.Context, req *vaultfsv1.OpenForWriteRequest) (*vaultfsv1.OpenForWriteResponse, error) {
	if len(s.chunkNodes) == 0 {
		return nil, status.Error(codes.Unavailable, "no chunk servers available")
	}
	placements := make([]*vaultfsv1.ChunkLocation, len(req.GetChunkIds()))
	for i, id := range req.GetChunkIds() {
		placements[i] = &vaultfsv1.ChunkLocation{ChunkId: id, Nodes: s.placement()}
	}
	return &vaultfsv1.OpenForWriteResponse{Placements: placements}, nil
}

// placement returns the chunk servers a new chunk should be written to: the
// first rf nodes (or all of them when fewer than rf exist).
func (s *Server) placement() []*vaultfsv1.NodeInfo {
	n := s.rf
	if n > len(s.chunkNodes) {
		n = len(s.chunkNodes)
	}
	return s.chunkNodes[:n]
}

// -- MasterService writes (replicated through Raft) ---------------------------

// CreateFile records an empty file in the namespace.
func (s *Server) CreateFile(ctx context.Context, req *vaultfsv1.CreateFileRequest) (*vaultfsv1.CreateFileResponse, error) {
	if err := s.propose(ctx, command{Kind: opCreate, Path: req.GetPath()}); err != nil {
		return nil, err
	}
	fi, err := s.ns.Stat(req.GetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "post-create stat: %v", err)
	}
	return &vaultfsv1.CreateFileResponse{File: fileInfoToProto(fi)}, nil
}

// FinalizeWrite commits a file with its full chunk list and size.
func (s *Server) FinalizeWrite(ctx context.Context, req *vaultfsv1.FinalizeWriteRequest) (*vaultfsv1.FinalizeWriteResponse, error) {
	cmd := command{Kind: opFinalize, Path: req.GetPath(), ChunkIDs: req.GetChunkIds(), Size: req.GetSize()}
	if err := s.propose(ctx, cmd); err != nil {
		return nil, err
	}
	fi, err := s.ns.Stat(req.GetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "post-finalize stat: %v", err)
	}
	return &vaultfsv1.FinalizeWriteResponse{File: fileInfoToProto(fi)}, nil
}

// DeleteFile removes a file from the namespace.
func (s *Server) DeleteFile(ctx context.Context, req *vaultfsv1.DeleteFileRequest) (*vaultfsv1.DeleteFileResponse, error) {
	if err := s.propose(ctx, command{Kind: opDelete, Path: req.GetPath()}); err != nil {
		return nil, err
	}
	return &vaultfsv1.DeleteFileResponse{}, nil
}

// GetLease grants or renews the write lease on a chunk.
func (s *Server) GetLease(_ context.Context, req *vaultfsv1.GetLeaseRequest) (*vaultfsv1.GetLeaseResponse, error) {
	lease, err := s.leases.Grant(req.GetChunkId(), req.GetHolder())
	if errors.Is(err, metadata.ErrLeaseHeld) {
		return nil, status.Error(codes.FailedPrecondition, "lease held by another node")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grant lease: %v", err)
	}
	return &vaultfsv1.GetLeaseResponse{Lease: &vaultfsv1.Lease{
		ChunkId: lease.ChunkID, Holder: lease.Holder, ExpiryUnix: lease.Expiry.Unix(),
	}}, nil
}

// -- AdminService -------------------------------------------------------------

// ClusterStatus reports Raft leadership and the known chunk servers.
func (s *Server) ClusterStatus(_ context.Context, _ *vaultfsv1.ClusterStatusRequest) (*vaultfsv1.ClusterStatusResponse, error) {
	nodes := make([]*vaultfsv1.NodeStatus, len(s.chunkNodes))
	for i, n := range s.chunkNodes {
		nodes[i] = &vaultfsv1.NodeStatus{Node: n, State: vaultfsv1.NodeState_NODE_STATE_ALIVE}
	}
	return &vaultfsv1.ClusterStatusResponse{
		LeaderId:   s.node.LeaderID(),
		Term:       s.node.Term(),
		ChunkCount: int64(len(s.chunkNodes)),
		Nodes:      nodes,
	}, nil
}

// ListNodes returns the known chunk servers.
func (s *Server) ListNodes(_ context.Context, _ *vaultfsv1.ListNodesRequest) (*vaultfsv1.ListNodesResponse, error) {
	nodes := make([]*vaultfsv1.NodeStatus, len(s.chunkNodes))
	for i, n := range s.chunkNodes {
		nodes[i] = &vaultfsv1.NodeStatus{Node: n, State: vaultfsv1.NodeState_NODE_STATE_ALIVE}
	}
	return &vaultfsv1.ListNodesResponse{Nodes: nodes}, nil
}

// -- helpers ------------------------------------------------------------------

func fileInfoToProto(fi metadata.FileInfo) *vaultfsv1.FileInfo {
	return &vaultfsv1.FileInfo{
		Path:          fi.Path,
		IsDir:         fi.IsDir,
		Size:          fi.Size,
		ChunkIds:      fi.ChunkIDs,
		Mode:          fi.Mode,
		CreatedAtUnix: fi.CreatedAt.Unix(),
		UpdatedAtUnix: fi.UpdatedAt.Unix(),
	}
}

func encode(cmd command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(data []byte) (command, error) {
	var cmd command
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd)
	return cmd, err
}
