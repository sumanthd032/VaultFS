package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// scrape renders the current metrics exposition text via the HTTP handler.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body)
}

func TestAllMetricsRegistered(t *testing.T) {
	m := New()
	// Touch each metric so it appears in the exposition output.
	m.RecordOp("write", StatusOK)
	m.RecordWALWrite(2 * time.Millisecond)
	m.RecordElection()
	m.SetReplicationLag("cs-0", 150*time.Millisecond)
	m.RecordMissedHeartbeat("cs-1")
	m.SetActiveLeases(3)

	out := scrape(t, m)
	want := []string{
		"vaultfs_ops_total",
		"vaultfs_wal_write_seconds",
		"vaultfs_raft_elections_total",
		"vaultfs_replication_lag_seconds",
		"vaultfs_heartbeat_missed_total",
		"vaultfs_active_leases",
	}
	for _, name := range want {
		if !strings.Contains(out, name) {
			t.Errorf("metric %q missing from exposition output", name)
		}
	}
}

func TestRecordOpLabels(t *testing.T) {
	m := New()
	m.RecordOp("read", StatusOK)
	m.RecordOp("read", StatusOK)
	m.RecordOp("write", StatusError)

	out := scrape(t, m)
	tests := []struct {
		name string
		want string
	}{
		{"read ok count", `vaultfs_ops_total{status="ok",type="read"} 2`},
		{"write error count", `vaultfs_ops_total{status="error",type="write"} 1`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !strings.Contains(out, tt.want) {
				t.Errorf("missing %q in:\n%s", tt.want, out)
			}
		})
	}
}

func TestSetGauges(t *testing.T) {
	m := New()
	m.SetActiveLeases(5)
	m.SetReplicationLag("cs-2", time.Second)

	out := scrape(t, m)
	if !strings.Contains(out, "vaultfs_active_leases 5") {
		t.Error("active leases gauge not set to 5")
	}
	if !strings.Contains(out, `vaultfs_replication_lag_seconds{node_id="cs-2"} 1`) {
		t.Error("replication lag gauge not set for cs-2")
	}
}

// TestNilSafe verifies the record methods are no-ops on a nil Metrics, so
// components can be constructed without metrics in tests.
func TestNilSafe(t *testing.T) {
	var m *Metrics
	m.RecordOp("x", StatusOK)
	m.RecordWALWrite(time.Millisecond)
	m.RecordElection()
	m.SetReplicationLag("n", time.Second)
	m.RecordMissedHeartbeat("n")
	m.SetActiveLeases(1)
}
