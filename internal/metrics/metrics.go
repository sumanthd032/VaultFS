// Package metrics defines the Prometheus instrumentation for VaultFS.
//
// A Metrics value owns its own registry and collectors; it is created once in
// each binary and injected into the components that record into it (no global
// state). Components that sit below the binary take small optional callbacks
// wired to these methods, so the lower layers stay decoupled from Prometheus.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Operation status label values for RecordOp.
const (
	StatusOK    = "ok"
	StatusError = "error"
)

// Metrics holds every VaultFS collector and the registry they are registered on.
type Metrics struct {
	registry *prometheus.Registry

	ops            *prometheus.CounterVec
	walWrite       prometheus.Histogram
	elections      prometheus.Counter
	replicationLag *prometheus.GaugeVec
	heartbeatMiss  *prometheus.CounterVec
	activeLeases   prometheus.Gauge
}

// New creates a Metrics with all collectors registered on a fresh registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		ops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vaultfs_ops_total",
			Help: "Total VaultFS operations by type and status.",
		}, []string{"type", "status"}),
		walWrite: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "vaultfs_wal_write_seconds",
			Help: "WAL append latency in seconds.",
			// 0.1ms to ~100ms.
			Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1},
		}),
		elections: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "vaultfs_raft_elections_total",
			Help: "Total Raft elections started by this node.",
		}),
		replicationLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vaultfs_replication_lag_seconds",
			Help: "Most recent chunk replication latency to a downstream node, in seconds.",
		}, []string{"node_id"}),
		heartbeatMiss: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vaultfs_heartbeat_missed_total",
			Help: "Total missed heartbeats that led to a node being declared dead.",
		}, []string{"node_id"}),
		activeLeases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "vaultfs_active_leases",
			Help: "Number of currently held chunk write leases.",
		}),
	}
	reg.MustRegister(m.ops, m.walWrite, m.elections, m.replicationLag, m.heartbeatMiss, m.activeLeases)
	return m
}

// Registry returns the underlying registry, used to build the /metrics handler.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// RecordOp counts one operation of the given type with the given status.
func (m *Metrics) RecordOp(opType, status string) {
	if m == nil {
		return
	}
	m.ops.WithLabelValues(opType, status).Inc()
}

// RecordWALWrite observes a WAL append latency.
func (m *Metrics) RecordWALWrite(d time.Duration) {
	if m == nil {
		return
	}
	m.walWrite.Observe(d.Seconds())
}

// RecordElection counts one Raft election started by this node.
func (m *Metrics) RecordElection() {
	if m == nil {
		return
	}
	m.elections.Inc()
}

// SetReplicationLag records the latest replication latency to nodeID.
func (m *Metrics) SetReplicationLag(nodeID string, d time.Duration) {
	if m == nil {
		return
	}
	m.replicationLag.WithLabelValues(nodeID).Set(d.Seconds())
}

// RecordMissedHeartbeat counts a node declared dead for missing heartbeats.
func (m *Metrics) RecordMissedHeartbeat(nodeID string) {
	if m == nil {
		return
	}
	m.heartbeatMiss.WithLabelValues(nodeID).Inc()
}

// SetActiveLeases records the current number of held leases.
func (m *Metrics) SetActiveLeases(n int) {
	if m == nil {
		return
	}
	m.activeLeases.Set(float64(n))
}
