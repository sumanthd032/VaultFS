package metadata

import (
	"testing"
	"time"
)

func TestMonitorRecordAndLive(t *testing.T) {
	cm := NewChunkMap()
	m := NewMonitor(cm, 15*time.Second, 3)
	base := time.Now()
	m.now = func() time.Time { return base }

	m.RecordHeartbeat("node-A")
	m.RecordHeartbeat("node-B")

	if live := m.LiveNodes(); len(live) != 2 {
		t.Errorf("LiveNodes = %d, want 2", len(live))
	}

	// node-A keeps beating; node-B goes silent past the timeout.
	m.now = func() time.Time { return base.Add(20 * time.Second) }
	m.RecordHeartbeat("node-A")
	live := m.LiveNodes()
	if len(live) != 1 || live[0] != "node-A" {
		t.Errorf("LiveNodes after timeout = %v, want [node-A]", live)
	}
}

func TestMonitorSweepDeclaresDeadAndEvicts(t *testing.T) {
	cm := NewChunkMap()
	// chunk-1 is replicated on A, B, C (factor satisfied).
	cm.AddLocation("chunk-1", Location{NodeID: "A"})
	cm.AddLocation("chunk-1", Location{NodeID: "B"})
	cm.AddLocation("chunk-1", Location{NodeID: "C"})

	m := NewMonitor(cm, 15*time.Second, 3)
	base := time.Now()
	m.now = func() time.Time { return base }
	m.RecordHeartbeat("A")
	m.RecordHeartbeat("B")
	m.RecordHeartbeat("C")

	// C dies (no further heartbeat); A and B keep beating.
	m.now = func() time.Time { return base.Add(20 * time.Second) }
	m.RecordHeartbeat("A")
	m.RecordHeartbeat("B")

	dead, tasks := m.Sweep()
	if len(dead) != 1 || dead[0] != "C" {
		t.Errorf("dead = %v, want [C]", dead)
	}
	// chunk-1 now has 2 replicas; it is under-replicated (factor 3) by 1.
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1: %+v", len(tasks), tasks)
	}
	if tasks[0].ChunkID != "chunk-1" || tasks[0].Need != 1 {
		t.Errorf("task = %+v, want chunk-1 need 1", tasks[0])
	}
	if len(tasks[0].Sources) != 2 {
		t.Errorf("task sources = %d, want 2 live replicas", len(tasks[0].Sources))
	}
	// The dead node's replica must be evicted from the map.
	for _, loc := range cm.GetLocations("chunk-1") {
		if loc.NodeID == "C" {
			t.Error("dead node C still present in chunk map")
		}
	}
}

func TestMonitorSweepNoDeadNoTasks(t *testing.T) {
	cm := NewChunkMap()
	cm.AddLocation("c1", Location{NodeID: "A"})
	m := NewMonitor(cm, 15*time.Second, 3)
	base := time.Now()
	m.now = func() time.Time { return base }
	m.RecordHeartbeat("A")

	// Within timeout - no node is dead, so no re-replication is triggered even
	// though c1 is under-replicated. Re-replication is driven by failures, not
	// by steady-state under-replication in this path.
	dead, tasks := m.Sweep()
	if len(dead) != 0 {
		t.Errorf("dead = %v, want none", dead)
	}
	if len(tasks) != 0 {
		t.Errorf("tasks = %v, want none", tasks)
	}
}

func TestMonitorDefaults(t *testing.T) {
	m := NewMonitor(NewChunkMap(), 0, 0)
	if m.timeout != DefaultDeadTimeout {
		t.Errorf("timeout = %v, want default", m.timeout)
	}
	if m.replicationFactor != DefaultReplicationFactor {
		t.Errorf("factor = %d, want default", m.replicationFactor)
	}
}

func TestMonitorLastSeenAndIsAlive(t *testing.T) {
	cm := NewChunkMap()
	m := NewMonitor(cm, 15*time.Second, 3)
	base := time.Now()
	m.now = func() time.Time { return base }

	if _, ok := m.LastSeen("n1"); ok {
		t.Error("LastSeen should be absent before any heartbeat")
	}
	if m.IsAlive("n1") {
		t.Error("IsAlive should be false before any heartbeat")
	}

	m.RecordHeartbeat("n1")
	if ts, ok := m.LastSeen("n1"); !ok || !ts.Equal(base) {
		t.Errorf("LastSeen = %v, %v; want %v, true", ts, ok, base)
	}
	if !m.IsAlive("n1") {
		t.Error("IsAlive should be true right after a heartbeat")
	}

	// Past the dead timeout it is no longer alive.
	m.now = func() time.Time { return base.Add(20 * time.Second) }
	if m.IsAlive("n1") {
		t.Error("IsAlive should be false after the timeout")
	}
}
