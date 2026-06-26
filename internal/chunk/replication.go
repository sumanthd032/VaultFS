package chunk

import (
	"context"
	"fmt"
	"log/slog"
)

// ChunkSender forwards a chunk to a downstream chunk server. It is defined here,
// on the consumer side, and implemented by the gRPC chunk transport (Step 4)
// and by test fakes. The downstream slice is the remaining replication chain:
// the receiver stores the chunk locally, then forwards to downstream[0] with
// downstream[1:] as its own chain.
type ChunkSender interface {
	SendChunk(ctx context.Context, target string, id ChunkID, data []byte, downstream []string) error
}

// Replicator stores chunks locally and forwards them along a replication chain,
// GFS-style: data flows linearly primary -> secondary -> secondary rather than
// fanning out from the primary, so each node's outbound bandwidth is used once.
type Replicator struct {
	store  *Store
	sender ChunkSender
}

// NewReplicator returns a Replicator backed by store, using sender to reach peers.
func NewReplicator(store *Store, sender ChunkSender) *Replicator {
	return &Replicator{store: store, sender: sender}
}

// Replicate persists data locally, then forwards it down chain. chain is the
// ordered list of downstream node IDs that still need the chunk; the local node
// is not included. It returns the chunk's ID once the entire chain has acked.
//
// A failure anywhere in the chain aborts replication and is returned to the
// caller, who is responsible for retrying or selecting new replicas.
func (r *Replicator) Replicate(ctx context.Context, data []byte, chain []string) (ChunkID, error) {
	id, err := r.store.WriteChunk(ctx, data)
	if err != nil {
		return "", fmt.Errorf("chunk: replicate local write: %w", err)
	}

	if len(chain) == 0 {
		return id, nil // tail of the chain reached
	}

	next, downstream := chain[0], chain[1:]
	if err := r.sender.SendChunk(ctx, next, id, data, downstream); err != nil {
		return "", fmt.Errorf("chunk: replicate %s to %s: %w", id, next, err)
	}
	slog.Debug("chunk forwarded", "chunk_id", id, "next", next, "remaining", len(downstream))
	return id, nil
}
