package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These are in-process integration tests: each node runs a real GRPCTransport
// on localhost, so RPCs cross the actual gRPC stack (serialization, context
// deadlines, concurrency) rather than the in-memory transport the unit tests
// use. Killing a node means stopping its Raft loops and closing its transport,
// which is how the multi-node election livelock and similar bugs surface.

// raftCluster is an in-process Raft cluster wired over the gRPC transport.
type raftCluster struct {
	t          *testing.T
	nodes      []*Node
	transports []*GRPCTransport

	mu        sync.Mutex
	committed [][]Entry // per-node committed entries, in delivery order
	alive     []bool
	done      chan struct{}
}

// startRaftCluster brings up n nodes on consecutive localhost ports starting at
// basePort and records every committed entry each node delivers.
func startRaftCluster(t *testing.T, n, basePort int) *raftCluster {
	t.Helper()
	c := &raftCluster{
		t:         t,
		committed: make([][]Entry, n),
		alive:     make([]bool, n),
		done:      make(chan struct{}),
	}
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	for i := 0; i < n; i++ {
		var peers []string
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, addrs[j])
			}
		}
		commitCh := make(chan Entry, 512)
		cfg := Config{
			ID:                 fmt.Sprintf("n%d", i),
			Peers:              peers,
			ElectionMinTimeout: 150 * time.Millisecond,
			ElectionMaxTimeout: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			CommitCh:           commitCh,
		}
		tr, err := NewGRPCTransport(addrs[i])
		if err != nil {
			t.Fatalf("transport %d: %v", i, err)
		}
		node := New(cfg)
		node.Start(tr)
		c.nodes = append(c.nodes, node)
		c.transports = append(c.transports, tr)
		c.alive[i] = true

		idx := i
		go func() {
			for {
				select {
				case e := <-commitCh:
					c.mu.Lock()
					c.committed[idx] = append(c.committed[idx], e)
					c.mu.Unlock()
				case <-c.done:
					return
				}
			}
		}()
	}

	t.Cleanup(func() {
		close(c.done)
		for i, node := range c.nodes {
			c.mu.Lock()
			a := c.alive[i]
			c.mu.Unlock()
			if a {
				node.Stop()
				_ = c.transports[i].Close()
			}
		}
	})
	return c
}

// leader returns the index and node of the single live leader, or (-1, nil).
func (c *raftCluster) leader() (int, *Node) {
	idx, count := -1, 0
	for i, n := range c.nodes {
		c.mu.Lock()
		a := c.alive[i]
		c.mu.Unlock()
		if a && n.State() == Leader {
			idx, count = i, count+1
		}
	}
	if count == 1 {
		return idx, c.nodes[idx]
	}
	return -1, nil
}

// waitLeader blocks until exactly one live node is leader.
func (c *raftCluster) waitLeader(timeout time.Duration) (int, *Node) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if i, n := c.leader(); n != nil {
			return i, n
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no single leader elected within timeout")
	return -1, nil
}

// kill stops a node's Raft loops and closes its transport, simulating a crash.
func (c *raftCluster) kill(i int) {
	c.mu.Lock()
	c.alive[i] = false
	c.mu.Unlock()
	c.nodes[i].Stop()
	_ = c.transports[i].Close()
}

// propose finds the current leader and proposes cmd, retrying across leader
// changes until it is accepted or the timeout expires.
func (c *raftCluster) propose(cmd string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, n := c.leader(); n != nil {
			if err := n.Propose(context.Background(), []byte(cmd)); err == nil {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// committedCommands returns node i's committed commands in order.
func (c *raftCluster) committedCommands(i int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.committed[i]))
	for k, e := range c.committed[i] {
		out[k] = string(e.Command)
	}
	return out
}

// nodesWithAtLeast counts live nodes that have committed at least n entries.
func (c *raftCluster) nodesWithAtLeast(n int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	cnt := 0
	for i := range c.nodes {
		if c.alive[i] && len(c.committed[i]) >= n {
			cnt++
		}
	}
	return cnt
}

// maxCommitted returns the largest committed length among live nodes.
func (c *raftCluster) maxCommitted() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := 0
	for i := range c.nodes {
		if c.alive[i] && len(c.committed[i]) > m {
			m = len(c.committed[i])
		}
	}
	return m
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", timeout, msg)
}

// assertAgreement checks the Raft state-machine safety property across live
// nodes: their committed command sequences must agree wherever they overlap
// (one is a prefix of the other). A divergence means an entry was lost or
// rewritten after the leader was killed.
func assertAgreement(t *testing.T, c *raftCluster) {
	t.Helper()
	var seqs [][]string
	var ids []int
	for i := range c.nodes {
		c.mu.Lock()
		a := c.alive[i]
		c.mu.Unlock()
		if a {
			seqs = append(seqs, c.committedCommands(i))
			ids = append(ids, i)
		}
	}
	for a := 0; a < len(seqs); a++ {
		for b := a + 1; b < len(seqs); b++ {
			n := len(seqs[a])
			if len(seqs[b]) < n {
				n = len(seqs[b])
			}
			for k := 0; k < n; k++ {
				if seqs[a][k] != seqs[b][k] {
					t.Fatalf("divergence at index %d: node n%d committed %q, node n%d committed %q",
						k+1, ids[a], seqs[a][k], ids[b], seqs[b][k])
				}
			}
		}
	}
}

// TestGRPCKillLeaderPreservesCommittedEntries proposes a batch, waits for it to
// commit on a quorum, kills the leader, and verifies the surviving cluster
// still holds every committed entry and accepts new writes.
func TestGRPCKillLeaderPreservesCommittedEntries(t *testing.T) {
	c := startRaftCluster(t, 3, 19300)
	leaderIdx, _ := c.waitLeader(5 * time.Second)

	for i := 0; i < 5; i++ {
		if !c.propose(fmt.Sprintf("pre-%d", i), 5*time.Second) {
			t.Fatalf("could not propose pre-%d", i)
		}
	}
	// Wait until a quorum (2 of 3) has the 5 entries committed and durable.
	waitFor(t, 5*time.Second, func() bool { return c.nodesWithAtLeast(5) >= 2 },
		"5 entries committed on a quorum")

	c.kill(leaderIdx)

	// A survivor must take over and contain all committed entries.
	_, newLeader := c.waitLeader(5 * time.Second)

	for i := 0; i < 5; i++ {
		if !c.propose(fmt.Sprintf("post-%d", i), 5*time.Second) {
			t.Fatalf("could not propose post-%d after kill", i)
		}
	}
	waitFor(t, 5*time.Second, func() bool { return c.maxCommitted() >= 10 },
		"10 entries committed after recovery")

	assertAgreement(t, c)

	// The new leader must have every pre-kill entry (durability) and the
	// post-kill entries (progress), in order.
	var leaderCmds []string
	for i, n := range c.nodes {
		if n == newLeader {
			leaderCmds = c.committedCommands(i)
		}
	}
	want := []string{"pre-0", "pre-1", "pre-2", "pre-3", "pre-4", "post-0", "post-1", "post-2", "post-3", "post-4"}
	if len(leaderCmds) < len(want) {
		t.Fatalf("new leader committed %d entries, want >= %d: %v", len(leaderCmds), len(want), leaderCmds)
	}
	for i, w := range want {
		if leaderCmds[i] != w {
			t.Fatalf("committed[%d] = %q, want %q (full: %v)", i, leaderCmds[i], w, leaderCmds)
		}
	}
}

// TestGRPCKillLeaderMidWrite kills the leader while a writer is continuously
// proposing, then verifies the survivors never diverge and the cluster keeps
// committing new entries after the failure.
func TestGRPCKillLeaderMidWrite(t *testing.T) {
	c := startRaftCluster(t, 3, 19310)
	leaderIdx, _ := c.waitLeader(5 * time.Second)

	var seq int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			n := atomic.AddInt64(&seq, 1)
			if !c.propose(fmt.Sprintf("w-%d", n), time.Second) {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Let writes flow, then kill the leader mid-stream.
	waitFor(t, 5*time.Second, func() bool { return c.maxCommitted() >= 5 }, "initial writes commit")
	committedBefore := c.maxCommitted()
	c.kill(leaderIdx)

	// A new leader must emerge and writes must keep committing past the kill.
	c.waitLeader(5 * time.Second)
	waitFor(t, 5*time.Second, func() bool { return c.maxCommitted() > committedBefore+3 },
		"writes continue committing after the kill")

	close(stop)
	wg.Wait()
	time.Sleep(300 * time.Millisecond) // let survivors converge

	assertAgreement(t, c)
}
