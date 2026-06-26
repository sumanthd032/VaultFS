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
	"github.com/sumanthd032/vaultfs/internal/metrics"
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
	metrics    *metrics.Metrics
	monitor    *metadata.Monitor
	chunkMap   *metadata.ChunkMap

	opSeq atomic.Uint64

	mu          sync.Mutex // protects waiters and chunkCounts
	waiters     map[string]chan error
	chunkCounts map[string]int64 // node ID -> chunks reported in its last heartbeat
}

// Option configures a Server.
type Option func(*Server)

// WithMetrics attaches a metrics sink to the server.
func WithMetrics(m *metrics.Metrics) Option {
	return func(s *Server) { s.metrics = m }
}

// WithMonitor attaches a liveness monitor that records chunk-server heartbeats.
func WithMonitor(m *metadata.Monitor) Option {
	return func(s *Server) { s.monitor = m }
}

// WithChunkMap attaches a chunk map that the master populates as writes commit,
// so the liveness monitor can detect under-replicated chunks. It must be the
// same instance passed to the monitor.
func WithChunkMap(cm *metadata.ChunkMap) Option {
	return func(s *Server) { s.chunkMap = cm }
}

// New creates a master Server. node is a started Raft node whose committed
// entries arrive on commitCh; the caller wires those together. chunkNodes is the
// set of chunk servers available for placement and rf is the replication factor.
func New(ns *metadata.Namespace, leases *metadata.LeaseManager, node *raft.Node, chunkNodes []*vaultfsv1.NodeInfo, rf int, opts ...Option) *Server {
	if rf <= 0 {
		rf = metadata.DefaultReplicationFactor
	}
	s := &Server{
		ns:          ns,
		leases:      leases,
		chunkNodes:  chunkNodes,
		rf:          rf,
		node:        node,
		waiters:     make(map[string]chan error),
		chunkCounts: make(map[string]int64),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
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
			if err := s.ns.UpdateFile(fi); err != nil {
				return err
			}
		} else {
			if err := s.ensureParents(cmd.Path); err != nil {
				return err
			}
			if err := s.ns.CreateFile(fi); err != nil {
				return err
			}
		}
		s.recordChunkLocations(cmd.ChunkIDs)
		return nil
	case opDelete:
		if err := s.ns.DeleteFile(cmd.Path); err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return err
		}
		return nil
	default:
		return fmt.Errorf("master: unknown op kind %d", cmd.Kind)
	}
}

// recordChunkLocations registers the deterministic placement of each chunk in
// the chunk map so the liveness monitor can detect under-replicated chunks
// after a node failure. Placement matches what the client used (the first rf
// nodes), and AddLocation is idempotent, so replays are safe.
func (s *Server) recordChunkLocations(chunkIDs []string) {
	if s.chunkMap == nil {
		return
	}
	placement := s.placement()
	for _, id := range chunkIDs {
		for _, n := range placement {
			s.chunkMap.AddLocation(id, metadata.Location{NodeID: n.GetNodeId(), Address: n.GetAddress()})
		}
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
		s.metrics.RecordOp("create_file", metrics.StatusError)
		return nil, err
	}
	fi, err := s.ns.Stat(req.GetPath())
	if err != nil {
		s.metrics.RecordOp("create_file", metrics.StatusError)
		return nil, status.Errorf(codes.Internal, "post-create stat: %v", err)
	}
	s.metrics.RecordOp("create_file", metrics.StatusOK)
	return &vaultfsv1.CreateFileResponse{File: fileInfoToProto(fi)}, nil
}

// FinalizeWrite commits a file with its full chunk list and size.
func (s *Server) FinalizeWrite(ctx context.Context, req *vaultfsv1.FinalizeWriteRequest) (*vaultfsv1.FinalizeWriteResponse, error) {
	cmd := command{Kind: opFinalize, Path: req.GetPath(), ChunkIDs: req.GetChunkIds(), Size: req.GetSize()}
	if err := s.propose(ctx, cmd); err != nil {
		s.metrics.RecordOp("finalize_write", metrics.StatusError)
		return nil, err
	}
	fi, err := s.ns.Stat(req.GetPath())
	if err != nil {
		s.metrics.RecordOp("finalize_write", metrics.StatusError)
		return nil, status.Errorf(codes.Internal, "post-finalize stat: %v", err)
	}
	s.metrics.RecordOp("finalize_write", metrics.StatusOK)
	return &vaultfsv1.FinalizeWriteResponse{File: fileInfoToProto(fi)}, nil
}

// DeleteFile removes a file from the namespace.
func (s *Server) DeleteFile(ctx context.Context, req *vaultfsv1.DeleteFileRequest) (*vaultfsv1.DeleteFileResponse, error) {
	if err := s.propose(ctx, command{Kind: opDelete, Path: req.GetPath()}); err != nil {
		s.metrics.RecordOp("delete_file", metrics.StatusError)
		return nil, err
	}
	s.metrics.RecordOp("delete_file", metrics.StatusOK)
	return &vaultfsv1.DeleteFileResponse{}, nil
}

// GetLease grants or renews the write lease on a chunk.
func (s *Server) GetLease(_ context.Context, req *vaultfsv1.GetLeaseRequest) (*vaultfsv1.GetLeaseResponse, error) {
	lease, err := s.leases.Grant(req.GetChunkId(), req.GetHolder())
	if errors.Is(err, metadata.ErrLeaseHeld) {
		s.metrics.RecordOp("get_lease", metrics.StatusError)
		return nil, status.Error(codes.FailedPrecondition, "lease held by another node")
	}
	if err != nil {
		s.metrics.RecordOp("get_lease", metrics.StatusError)
		return nil, status.Errorf(codes.Internal, "grant lease: %v", err)
	}
	s.metrics.RecordOp("get_lease", metrics.StatusOK)
	s.metrics.SetActiveLeases(s.leases.Count())
	return &vaultfsv1.GetLeaseResponse{Lease: &vaultfsv1.Lease{
		ChunkId: lease.ChunkID, Holder: lease.Holder, ExpiryUnix: lease.Expiry.Unix(),
	}}, nil
}

// Heartbeat records a chunk server's liveness and reported chunk inventory. It
// is called periodically by every chunk server.
func (s *Server) Heartbeat(_ context.Context, req *vaultfsv1.HeartbeatRequest) (*vaultfsv1.HeartbeatResponse, error) {
	id := req.GetNodeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if s.monitor != nil {
		s.monitor.RecordHeartbeat(id)
	}
	s.mu.Lock()
	s.chunkCounts[id] = req.GetChunkCount()
	s.mu.Unlock()
	return &vaultfsv1.HeartbeatResponse{}, nil
}

// nodeStatuses builds the per-node status list, enriching each known chunk
// server with its last reported chunk count and, when a monitor is configured,
// its liveness and most recent heartbeat time. Without a monitor every node is
// reported alive so single-node and test setups behave as before.
func (s *Server) nodeStatuses() []*vaultfsv1.NodeStatus {
	s.mu.Lock()
	counts := make(map[string]int64, len(s.chunkCounts))
	for id, c := range s.chunkCounts {
		counts[id] = c
	}
	s.mu.Unlock()

	nodes := make([]*vaultfsv1.NodeStatus, len(s.chunkNodes))
	for i, n := range s.chunkNodes {
		st := &vaultfsv1.NodeStatus{
			Node:       n,
			State:      vaultfsv1.NodeState_NODE_STATE_ALIVE,
			ChunkCount: counts[n.GetNodeId()],
		}
		if s.monitor != nil {
			st.State = vaultfsv1.NodeState_NODE_STATE_DEAD
			if ts, ok := s.monitor.LastSeen(n.GetNodeId()); ok {
				st.LastHeartbeatUnix = ts.Unix()
			}
			if s.monitor.IsAlive(n.GetNodeId()) {
				st.State = vaultfsv1.NodeState_NODE_STATE_ALIVE
			}
		}
		nodes[i] = st
	}
	return nodes
}

// -- AdminService -------------------------------------------------------------

// ClusterStatus reports Raft leadership, namespace totals, and the known chunk
// servers with their liveness and chunk counts.
func (s *Server) ClusterStatus(_ context.Context, _ *vaultfsv1.ClusterStatusRequest) (*vaultfsv1.ClusterStatusResponse, error) {
	fileCount, chunkCount, err := s.ns.Stats()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "namespace stats: %v", err)
	}
	return &vaultfsv1.ClusterStatusResponse{
		LeaderId:   s.node.LeaderID(),
		Term:       s.node.Term(),
		FileCount:  int64(fileCount),
		ChunkCount: int64(chunkCount),
		Nodes:      s.nodeStatuses(),
	}, nil
}

// ListNodes returns the known chunk servers with their liveness and chunk counts.
func (s *Server) ListNodes(_ context.Context, _ *vaultfsv1.ListNodesRequest) (*vaultfsv1.ListNodesResponse, error) {
	return &vaultfsv1.ListNodesResponse{Nodes: s.nodeStatuses()}, nil
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
