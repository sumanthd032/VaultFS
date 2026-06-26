package raft

import (
	"context"
	"testing"
	"time"
)

// testConfig returns a Config with very short timeouts suitable for unit tests.
func testConfig(id string, peers []string, commitCh chan<- Entry) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		ElectionMinTimeout: 20 * time.Millisecond,
		ElectionMaxTimeout: 40 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
		CommitCh:           commitCh,
	}
}

// waitForLeader polls until exactly one leader exists, or the test times out.
func waitForLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var leader *Node
		found := 0
		for _, n := range nodes {
			if n.State() == Leader {
				leader = n
				found++
			}
		}
		if found == 1 {
			return leader
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no single leader elected within 2 s")
	return nil
}

// makeCluster creates a cluster of n nodes sharing an InMemNetwork.
func makeCluster(t *testing.T, count int) ([]*Node, *InMemNetwork) {
	t.Helper()
	net := NewInMemNetwork()

	ids := make([]string, count)
	for i := range ids {
		ids[i] = string(rune('A' + i))
	}

	nodes := make([]*Node, count)
	for i, id := range ids {
		peers := make([]string, 0, count-1)
		for j, pid := range ids {
			if j != i {
				peers = append(peers, pid)
			}
		}
		cfg := testConfig(id, peers, nil)
		nodes[i] = New(cfg)
		nodes[i].Start(net.Transport(id))
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})
	return nodes, net
}

// -- election ----------------------------------------------------------------

func TestSingleNodeBecomesLeader(t *testing.T) {
	net := NewInMemNetwork()
	cfg := testConfig("A", nil, nil) // no peers -> majority is just self
	n := New(cfg)
	n.Start(net.Transport("A"))
	t.Cleanup(n.Stop)

	waitForLeader(t, []*Node{n})
	if n.State() != Leader {
		t.Fatalf("expected Leader, got %s", n.State())
	}
}

func TestThreeNodeElection(t *testing.T) {
	nodes, _ := makeCluster(t, 3)
	leader := waitForLeader(t, nodes)

	// Verify exactly one leader and all others are followers.
	for _, n := range nodes {
		if n.id == leader.id {
			continue
		}
		if n.State() == Leader {
			t.Errorf("node %s is also a leader - split brain", n.id)
		}
	}
}

func TestFiveNodeElection(t *testing.T) {
	nodes, _ := makeCluster(t, 5)
	waitForLeader(t, nodes)
}

func TestElectionTermAdvances(t *testing.T) {
	nodes, _ := makeCluster(t, 3)
	waitForLeader(t, nodes)

	// All nodes should be in the same term (>= 1).
	terms := make([]uint64, len(nodes))
	for i, n := range nodes {
		terms[i] = n.Term()
		if terms[i] == 0 {
			t.Errorf("node %s has term 0 after election", n.id)
		}
	}
}

// -- heartbeats --------------------------------------------------------------

func TestHeartbeatPreventsFollowerElection(t *testing.T) {
	nodes, _ := makeCluster(t, 3)
	leader := waitForLeader(t, nodes)
	initialTerm := leader.Term()

	// Let the cluster run for several heartbeat intervals.
	time.Sleep(100 * time.Millisecond)

	// Term should not have changed - no spurious elections while leader is alive.
	if leader.State() != Leader {
		t.Fatal("leader was deposed without a partition")
	}
	if leader.Term() != initialTerm {
		t.Errorf("term changed from %d to %d without partition", initialTerm, leader.Term())
	}
}

// -- log replication ---------------------------------------------------------

func TestLogReplicationToQuorum(t *testing.T) {
	commitChs := make([]chan Entry, 3)
	for i := range commitChs {
		commitChs[i] = make(chan Entry, 16)
	}

	net := NewInMemNetwork()
	ids := []string{"A", "B", "C"}
	nodes := make([]*Node, 3)
	for i, id := range ids {
		peers := make([]string, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		cfg := testConfig(id, peers, commitChs[i])
		nodes[i] = New(cfg)
		nodes[i].Start(net.Transport(id))
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	leader := waitForLeader(t, nodes)

	const numCmds = 5
	for i := 0; i < numCmds; i++ {
		if err := leader.Propose(context.Background(), []byte{byte(i)}); err != nil {
			t.Fatalf("Propose(%d): %v", i, err)
		}
	}

	// Every node must receive all 5 commits.
	for nodeIdx, ch := range commitChs {
		received := 0
		deadline := time.After(2 * time.Second)
		for received < numCmds {
			select {
			case e := <-ch:
				if e.Command[0] != byte(received) {
					t.Errorf("node %d: got command %d, want %d", nodeIdx, e.Command[0], received)
				}
				received++
			case <-deadline:
				t.Fatalf("node %d: only received %d/%d commits", nodeIdx, received, numCmds)
			}
		}
	}
}

func TestProposeRejectsOnFollower(t *testing.T) {
	nodes, _ := makeCluster(t, 3)
	leader := waitForLeader(t, nodes)

	// Find a follower.
	var follower *Node
	for _, n := range nodes {
		if n.id != leader.id {
			follower = n
			break
		}
	}

	err := follower.Propose(context.Background(), []byte("cmd"))
	if err == nil {
		t.Fatal("Propose on follower should return ErrNotLeader")
	}
}

// -- leader failure ----------------------------------------------------------

func TestLeaderFailureTriggerReelection(t *testing.T) {
	nodes, _ := makeCluster(t, 3)
	leader := waitForLeader(t, nodes)
	oldLeaderID := leader.id
	oldTerm := leader.Term()

	// Kill the leader.
	leader.Stop()

	// The remaining two nodes must elect a new leader.
	remaining := make([]*Node, 0, 2)
	for _, n := range nodes {
		if n.id != oldLeaderID {
			remaining = append(remaining, n)
		}
	}
	newLeader := waitForLeader(t, remaining)

	if newLeader.Term() <= oldTerm {
		t.Errorf("new term %d must be > old term %d", newLeader.Term(), oldTerm)
	}
	if newLeader.id == oldLeaderID {
		t.Errorf("new leader is the same node as the old (stopped) leader")
	}
}

// -- partition & heal --------------------------------------------------------

func TestLogConsistencyAfterPartitionHeals(t *testing.T) {
	commitChs := make([]chan Entry, 3)
	for i := range commitChs {
		commitChs[i] = make(chan Entry, 64)
	}

	net := NewInMemNetwork()
	ids := []string{"A", "B", "C"}
	nodes := make([]*Node, 3)
	for i, id := range ids {
		peers := make([]string, 0, 2)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		cfg := testConfig(id, peers, commitChs[i])
		nodes[i] = New(cfg)
		nodes[i].Start(net.Transport(id))
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	leader := waitForLeader(t, nodes)

	// Commit one entry before the partition.
	if err := leader.Propose(context.Background(), []byte{0x01}); err != nil {
		t.Fatalf("pre-partition Propose: %v", err)
	}
	drainEntries(t, commitChs, 1, 2*time.Second)

	// Isolate the leader from both followers.
	var followers []*Node
	for _, n := range nodes {
		if n.id != leader.id {
			followers = append(followers, n)
			net.Partition(leader.id, n.id)
		}
	}

	// The two followers must elect a new leader.
	newLeader := waitForLeader(t, followers)

	// Commit one entry via the new leader (old leader can't commit - no quorum).
	if err := newLeader.Propose(context.Background(), []byte{0x02}); err != nil {
		t.Fatalf("post-partition Propose: %v", err)
	}

	// Heal the partition.
	net.HealAll()

	// Wait for the old leader to step down and the log to converge.
	time.Sleep(200 * time.Millisecond)

	// Old leader should no longer be leader.
	if leader.State() == Leader {
		t.Error("old leader is still claiming leadership after partition healed")
	}
	// Each channel already had entry-1 drained; wait for exactly 1 more entry
	// (entry-2) on each channel, confirming the partitioned node caught up.
	drainEntries(t, commitChs, 1, 3*time.Second)
}

// drainEntries reads at least n entries from each channel, failing on timeout.
func drainEntries(t *testing.T, chs []chan Entry, n int, timeout time.Duration) {
	t.Helper()
	for idx, ch := range chs {
		got := 0
		// Drain what's already buffered.
	drain:
		for {
			select {
			case <-ch:
				got++
				if got >= n {
					break drain
				}
			default:
				break drain
			}
		}
		if got >= n {
			continue
		}
		deadline := time.After(timeout)
		for got < n {
			select {
			case <-ch:
				got++
			case <-deadline:
				t.Errorf("channel %d: received %d/%d entries within %s", idx, got, n, timeout)
				return
			}
		}
	}
}

// -- raftLog unit tests -------------------------------------------------------

func TestRaftLogAppendAndGet(t *testing.T) {
	l := newRaftLog()
	l.Append(
		Entry{Index: 1, Term: 1, Command: []byte("a")},
		Entry{Index: 2, Term: 1, Command: []byte("b")},
	)

	tests := []struct {
		index   uint64
		wantCmd string
	}{
		{1, "a"},
		{2, "b"},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := l.Slice(tt.index, tt.index+1)
			if len(got) == 0 {
				t.Fatalf("Slice(%d, %d) returned nothing", tt.index, tt.index+1)
			}
			if string(got[0].Command) != tt.wantCmd {
				t.Errorf("got %q, want %q", got[0].Command, tt.wantCmd)
			}
		})
	}
}

func TestRaftLogTruncate(t *testing.T) {
	l := newRaftLog()
	for i := uint64(1); i <= 5; i++ {
		l.Append(Entry{Index: i, Term: 1})
	}
	l.TruncateAfter(3) // keep 1, 2; remove 3, 4, 5
	if l.LastIndex() != 2 {
		t.Errorf("LastIndex = %d, want 2", l.LastIndex())
	}
}

func TestRaftLogSetSnapshot(t *testing.T) {
	l := newRaftLog()
	for i := uint64(1); i <= 10; i++ {
		l.Append(Entry{Index: i, Term: 1})
	}
	l.SetSnapshot(5, 1)
	if l.snapIndex != 5 {
		t.Errorf("snapIndex = %d, want 5", l.snapIndex)
	}
	if l.Len() != 5 { // entries 6-10 remain
		t.Errorf("Len = %d, want 5", l.Len())
	}
	if l.LastIndex() != 10 {
		t.Errorf("LastIndex = %d, want 10", l.LastIndex())
	}
}

func TestRaftLogTermCompacted(t *testing.T) {
	l := newRaftLog()
	l.Append(Entry{Index: 1, Term: 1}, Entry{Index: 2, Term: 2})
	l.SetSnapshot(2, 2)
	_, err := l.Term(1)
	if err == nil {
		t.Error("Term(1) should error after compaction at index 2")
	}
}

// TestRaftLogSlice is a helper used by TestRaftLogAppendAndGet.
// Give raftLog a Slice-only test on its own.
func TestRaftLogSlice(t *testing.T) {
	l := newRaftLog()
	for i := uint64(1); i <= 5; i++ {
		l.Append(Entry{Index: i, Term: 1, Command: []byte{byte(i)}})
	}
	got := l.Slice(2, 4) // entries 2, 3
	if len(got) != 2 {
		t.Fatalf("Slice(2,4) len = %d, want 2", len(got))
	}
	if got[0].Index != 2 || got[1].Index != 3 {
		t.Errorf("wrong entries: %+v", got)
	}
}
