package metadata

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// DefaultDeadTimeout is how long the master waits after the last heartbeat
// before declaring a chunk server dead. With a 5 s heartbeat interval this
// tolerates two missed beats.
const DefaultDeadTimeout = 15 * time.Second

// DefaultReplicationFactor is the target number of replicas per chunk.
const DefaultReplicationFactor = 3

// ReplicationTask describes a chunk that has fallen below the replication factor
// and the live replicas available to copy from.
type ReplicationTask struct {
	ChunkID string
	Sources []Location // existing replicas that can serve as the copy source
	Need    int        // number of additional replicas required
}

// Monitor tracks chunk-server liveness from heartbeats. When a server misses
// heartbeats beyond the dead timeout, the monitor evicts its replicas from the
// chunk map and surfaces the chunks that must be re-replicated.
type Monitor struct {
	mu       sync.Mutex           // protects lastSeen
	lastSeen map[string]time.Time // node ID -> time of most recent heartbeat

	chunkMap          *ChunkMap
	timeout           time.Duration
	replicationFactor int
	now               func() time.Time // injectable clock for tests

	// OnNodeDead, if set, is called for each node a Sweep declares dead. It
	// keeps the monitor decoupled from any metrics implementation.
	OnNodeDead func(nodeID string)
}

// NewMonitor returns a Monitor over chunkMap. If timeout or factor are
// non-positive, their defaults are used.
func NewMonitor(chunkMap *ChunkMap, timeout time.Duration, factor int) *Monitor {
	if timeout <= 0 {
		timeout = DefaultDeadTimeout
	}
	if factor <= 0 {
		factor = DefaultReplicationFactor
	}
	return &Monitor{
		lastSeen:          make(map[string]time.Time),
		chunkMap:          chunkMap,
		timeout:           timeout,
		replicationFactor: factor,
		now:               time.Now,
	}
}

// RecordHeartbeat marks nodeID as alive as of now.
func (m *Monitor) RecordHeartbeat(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSeen[nodeID] = m.now()
}

// LastSeen returns the time of nodeID's most recent heartbeat and whether any
// heartbeat has been recorded for it.
func (m *Monitor) LastSeen(nodeID string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, ok := m.lastSeen[nodeID]
	return ts, ok
}

// IsAlive reports whether nodeID has heartbeated within the dead timeout.
func (m *Monitor) IsAlive(nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, ok := m.lastSeen[nodeID]
	return ok && m.now().Sub(ts) <= m.timeout
}

// LiveNodes returns the node IDs that have heartbeated within the timeout.
func (m *Monitor) LiveNodes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	var live []string
	for id, ts := range m.lastSeen {
		if now.Sub(ts) <= m.timeout {
			live = append(live, id)
		}
	}
	return live
}

// Sweep declares timed-out nodes dead, evicts their replicas from the chunk map,
// and returns the dead node IDs together with the resulting re-replication tasks.
func (m *Monitor) Sweep() (dead []string, tasks []ReplicationTask) {
	m.mu.Lock()
	now := m.now()
	for id, ts := range m.lastSeen {
		if now.Sub(ts) > m.timeout {
			dead = append(dead, id)
			delete(m.lastSeen, id)
		}
	}
	m.mu.Unlock()

	for _, id := range dead {
		m.chunkMap.RemoveNode(id)
		if m.OnNodeDead != nil {
			m.OnNodeDead(id)
		}
		slog.Warn("chunk server declared dead", "node_id", id)
	}

	if len(dead) == 0 {
		return dead, nil
	}

	for chunkID, locs := range m.chunkMap.UnderReplicated(m.replicationFactor) {
		tasks = append(tasks, ReplicationTask{
			ChunkID: chunkID,
			Sources: locs,
			Need:    m.replicationFactor - len(locs),
		})
	}
	return dead, tasks
}

// Run sweeps every interval until ctx is cancelled, invoking onTasks (if non-nil)
// with any re-replication work produced by each sweep.
func (m *Monitor) Run(ctx context.Context, interval time.Duration, onTasks func([]ReplicationTask)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, tasks := m.Sweep(); len(tasks) > 0 && onTasks != nil {
				onTasks(tasks)
			}
		}
	}
}
