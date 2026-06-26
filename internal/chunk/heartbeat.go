package chunk

import (
	"context"
	"log/slog"
	"time"
)

// DefaultHeartbeatInterval is how often a chunk server reports to the master.
const DefaultHeartbeatInterval = 5 * time.Second

// HeartbeatReporter delivers a chunk server's inventory to the master. Defined
// on the consumer side; implemented by the gRPC master transport (Step 4) and
// by test fakes.
type HeartbeatReporter interface {
	ReportHeartbeat(ctx context.Context, nodeID string, chunks []ChunkID) error
}

// HeartbeatSender periodically reports this chunk server's stored-chunk
// inventory to the master, so the master can track liveness and detect
// under-replicated chunks.
type HeartbeatSender struct {
	nodeID   string
	store    *Store
	reporter HeartbeatReporter
	interval time.Duration
}

// NewHeartbeatSender returns a sender for nodeID. If interval is zero,
// DefaultHeartbeatInterval is used.
func NewHeartbeatSender(nodeID string, store *Store, reporter HeartbeatReporter, interval time.Duration) *HeartbeatSender {
	if interval <= 0 {
		interval = DefaultHeartbeatInterval
	}
	return &HeartbeatSender{
		nodeID:   nodeID,
		store:    store,
		reporter: reporter,
		interval: interval,
	}
}

// Run sends a heartbeat immediately, then every interval until ctx is cancelled.
func (h *HeartbeatSender) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	if err := h.beat(ctx); err != nil {
		slog.Warn("chunk heartbeat failed", "node_id", h.nodeID, "err", err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := h.beat(ctx); err != nil {
				slog.Warn("chunk heartbeat failed", "node_id", h.nodeID, "err", err)
			}
		}
	}
}

// beat gathers the current inventory and reports it once.
func (h *HeartbeatSender) beat(ctx context.Context) error {
	chunks, err := h.store.ListChunks()
	if err != nil {
		return err
	}
	return h.reporter.ReportHeartbeat(ctx, h.nodeID, chunks)
}
