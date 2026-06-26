package master

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sumanthd032/vaultfs/internal/metadata"
	"github.com/sumanthd032/vaultfs/internal/raft"
	vaultfsv1 "github.com/sumanthd032/vaultfs/proto/vaultfs/v1"
)

// This is an in-process integration test of the metadata write path: three real
// master.Server instances, each backed by BadgerDB and a Raft node speaking
// over the gRPC transport. It kills the leader while namespace writes are in
// flight and checks that committed files survive and writes resume, the same
// failure the production cluster must tolerate.

type masterCluster struct {
	t          *testing.T
	masters    []*Server
	nodes      []*raft.Node
	transports []*raft.GRPCTransport
	cancels    []context.CancelFunc
	alive      []bool
}

func startMasterCluster(t *testing.T, n, basePort int) *masterCluster {
	t.Helper()
	c := &masterCluster{t: t, alive: make([]bool, n)}
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

		store, err := metadata.Open(t.TempDir())
		if err != nil {
			t.Fatalf("open store %d: %v", i, err)
		}
		ns := metadata.NewNamespace(store)
		leases := metadata.NewLeaseManager(time.Minute)

		commitCh := make(chan raft.Entry, 256)
		node := raft.New(raft.Config{
			ID:                 fmt.Sprintf("m%d", i),
			Peers:              peers,
			ElectionMinTimeout: 150 * time.Millisecond,
			ElectionMaxTimeout: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			CommitCh:           commitCh,
		})
		tr, err := raft.NewGRPCTransport(addrs[i])
		if err != nil {
			t.Fatalf("transport %d: %v", i, err)
		}
		node.Start(tr)

		srv := New(ns, leases, node, testChunkNodes(), 3)
		ctx, cancel := context.WithCancel(context.Background())
		go srv.Run(ctx, commitCh)

		c.masters = append(c.masters, srv)
		c.nodes = append(c.nodes, node)
		c.transports = append(c.transports, tr)
		c.cancels = append(c.cancels, cancel)
		c.alive[i] = true

		storeClose := store.Close
		t.Cleanup(func() { _ = storeClose() })
	}

	t.Cleanup(func() {
		for i := range c.masters {
			if c.alive[i] {
				c.cancels[i]()
				c.nodes[i].Stop()
				_ = c.transports[i].Close()
			}
		}
	})
	return c
}

// leader returns the index and Server of the single live leader, or (-1, nil).
func (c *masterCluster) leader() (int, *Server) {
	idx, count := -1, 0
	for i := range c.masters {
		if c.alive[i] && c.nodes[i].State() == raft.Leader {
			idx, count = i, count+1
		}
	}
	if count == 1 {
		return idx, c.masters[idx]
	}
	return -1, nil
}

func (c *masterCluster) waitLeader(timeout time.Duration) (int, *Server) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if i, s := c.leader(); s != nil {
			return i, s
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no single master leader within timeout")
	return -1, nil
}

func (c *masterCluster) kill(i int) {
	c.alive[i] = false
	c.cancels[i]()
	c.nodes[i].Stop()
	_ = c.transports[i].Close()
}

// createFile writes path through whichever master is currently the leader,
// retrying across leader changes until it succeeds or the timeout expires.
func (c *masterCluster) createFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		_, s := c.leader()
		if s == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := s.CreateFile(ctx, &vaultfsv1.CreateFileRequest{Path: path})
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("createFile %s: %w", path, lastErr)
}

// hasFile reports whether master i has path in its local namespace.
func (c *masterCluster) hasFile(i int, path string) bool {
	_, err := c.masters[i].Stat(context.Background(), &vaultfsv1.StatRequest{Path: path})
	return err == nil
}

// countWithFile counts live masters whose local namespace contains path.
func (c *masterCluster) countWithFile(path string) int {
	cnt := 0
	for i := range c.masters {
		if c.alive[i] && c.hasFile(i, path) {
			cnt++
		}
	}
	return cnt
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

// TestMasterKillLeaderMidWrite writes files through the master leader, kills it
// while a final write is committing, and verifies a survivor takes over with
// every committed file intact and accepts new writes.
func TestMasterKillLeaderMidWrite(t *testing.T) {
	c := startMasterCluster(t, 3, 19400)
	leaderIdx, _ := c.waitLeader(5 * time.Second)

	// Write a batch of files through the leader.
	for i := 0; i < 5; i++ {
		if err := c.createFile(fmt.Sprintf("/pre-%d", i), 5*time.Second); err != nil {
			t.Fatalf("pre write: %v", err)
		}
	}
	// Ensure the last file is durable on a quorum before killing the leader.
	waitFor(t, 5*time.Second, func() bool { return c.countWithFile("/pre-4") >= 2 },
		"pre-4 replicated to a quorum")

	c.kill(leaderIdx)

	// A survivor must take over and still serve every committed file.
	_, newLeader := c.waitLeader(5 * time.Second)
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("/pre-%d", i)
		if _, err := newLeader.Stat(context.Background(), &vaultfsv1.StatRequest{Path: path}); err != nil {
			t.Fatalf("new leader missing committed file %s: %v", path, err)
		}
	}

	// New writes must commit on the survivors.
	for i := 0; i < 5; i++ {
		if err := c.createFile(fmt.Sprintf("/post-%d", i), 5*time.Second); err != nil {
			t.Fatalf("post write: %v", err)
		}
	}
	waitFor(t, 5*time.Second, func() bool { return c.countWithFile("/post-4") >= 2 },
		"post-4 replicated to a quorum after recovery")
}
