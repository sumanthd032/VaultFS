package raft

import (
	"context"
	"testing"
	"time"
)

// startGRPCNode brings up a node backed by a real gRPC transport on addr.
func startGRPCNode(t *testing.T, id, addr string, peers []string) *Node {
	t.Helper()
	tr, err := NewGRPCTransport(addr)
	if err != nil {
		t.Fatalf("transport %s: %v", id, err)
	}
	cfg := Config{
		ID:                 id,
		Peers:              peers,
		ElectionMinTimeout: 150 * time.Millisecond,
		ElectionMaxTimeout: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
	n := New(cfg)
	n.Start(tr)
	t.Cleanup(n.Stop)
	t.Cleanup(func() { _ = tr.Close() })
	return n
}

// TestGRPCThreeNodeElection brings up three nodes over the real gRPC transport
// and requires them to elect a single leader. It guards against regressions in
// the vote path that the in-memory transport cannot catch, such as cancelling
// the election context before the vote RPCs return.
func TestGRPCThreeNodeElection(t *testing.T) {
	addrs := map[string]string{
		"A": "127.0.0.1:19201",
		"B": "127.0.0.1:19202",
		"C": "127.0.0.1:19203",
	}

	var nodes []*Node
	for id, addr := range addrs {
		var peers []string
		for pid, paddr := range addrs {
			if pid != id {
				peers = append(peers, paddr)
			}
		}
		nodes = append(nodes, startGRPCNode(t, id, addr, peers))
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leaders := 0
		for _, n := range nodes {
			if n.State() == Leader {
				leaders++
			}
		}
		if leaders == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, n := range nodes {
		t.Logf("node %s state=%s term=%d", n.id, n.State(), n.Term())
	}
	t.Fatal("no single leader elected within 5 s over gRPC transport")
}

// TestGRPCSingleRequestVote checks a single RequestVote round-trip over the gRPC
// transport with the gob codec: an empty-log node grants its vote to a
// higher-term candidate.
func TestGRPCSingleRequestVote(t *testing.T) {
	bCfg := Config{
		ID:                 "B",
		Peers:              []string{"127.0.0.1:19210"},
		ElectionMinTimeout: 10 * time.Second, // keep B from starting its own election
		ElectionMaxTimeout: 20 * time.Second,
		HeartbeatInterval:  50 * time.Millisecond,
	}
	bTr, err := NewGRPCTransport("127.0.0.1:19211")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bTr.Close() })
	bNode := New(bCfg)
	bNode.Start(bTr)
	t.Cleanup(bNode.Stop)

	aTr, err := NewGRPCTransport("127.0.0.1:19210")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = aTr.Close() })

	time.Sleep(100 * time.Millisecond) // let B's server come up

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, err := aTr.SendRequestVote(ctx, "127.0.0.1:19211", RequestVoteArgs{
		Term: 1, CandidateID: "A",
	})
	if err != nil {
		t.Fatalf("SendRequestVote returned error: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted, got reply=%+v", reply)
	}
}
