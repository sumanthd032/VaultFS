package raft

import (
	"context"
	"fmt"
	"sync"
)

// Transport sends Raft RPCs to remote peers and dispatches incoming RPCs to
// an RPCHandler. Implementations must be safe for concurrent use.
type Transport interface {
	SendRequestVote(ctx context.Context, peer string, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peer string, args AppendEntriesArgs) (AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error)

	// Register makes id reachable on this transport.
	Register(id string, handler RPCHandler)
	Close() error
}

// RPCHandler is implemented by Node so the transport can dispatch inbound RPCs.
type RPCHandler interface {
	HandleRequestVote(ctx context.Context, args RequestVoteArgs) (RequestVoteReply, error)
	HandleAppendEntries(ctx context.Context, args AppendEntriesArgs) (AppendEntriesReply, error)
	HandleInstallSnapshot(ctx context.Context, args InstallSnapshotArgs) (InstallSnapshotReply, error)
}

// InMemNetwork is a shared, in-process network used by tests. Multiple nodes
// share one network; messages are delivered synchronously. Partitions can be
// injected to simulate network failures.
type InMemNetwork struct {
	mu         sync.RWMutex
	handlers   map[string]RPCHandler  // nodeID -> handler
	partitions map[[2]string]struct{} // {from,to} pairs that are blocked
}

// NewInMemNetwork returns an empty in-process test network.
func NewInMemNetwork() *InMemNetwork {
	return &InMemNetwork{
		handlers:   make(map[string]RPCHandler),
		partitions: make(map[[2]string]struct{}),
	}
}

// Register adds or replaces the handler for id.
func (n *InMemNetwork) Register(id string, h RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Partition blocks all messages between a and b (bidirectional).
func (n *InMemNetwork) Partition(a, b string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions[[2]string{a, b}] = struct{}{}
	n.partitions[[2]string{b, a}] = struct{}{}
}

// Heal removes the partition between a and b.
func (n *InMemNetwork) Heal(a, b string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.partitions, [2]string{a, b})
	delete(n.partitions, [2]string{b, a})
}

// HealAll removes all partitions.
func (n *InMemNetwork) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions = make(map[[2]string]struct{})
}

func (n *InMemNetwork) isPartitioned(from, to string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, ok := n.partitions[[2]string{from, to}]
	return ok
}

func (n *InMemNetwork) getHandler(id string) (RPCHandler, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	h, ok := n.handlers[id]
	return h, ok
}

// Transport returns a Transport for the node with the given id.
func (n *InMemNetwork) Transport(id string) Transport {
	return &inMemTransport{id: id, net: n}
}

// inMemTransport is the per-node view of an InMemNetwork.
type inMemTransport struct {
	id  string
	net *InMemNetwork
}

func (t *inMemTransport) Register(id string, handler RPCHandler) {
	t.net.Register(id, handler)
}

func (t *inMemTransport) SendRequestVote(ctx context.Context, peer string, args RequestVoteArgs) (RequestVoteReply, error) {
	h, err := t.reach(peer)
	if err != nil {
		return RequestVoteReply{}, err
	}
	return h.HandleRequestVote(ctx, args)
}

func (t *inMemTransport) SendAppendEntries(ctx context.Context, peer string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	h, err := t.reach(peer)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	return h.HandleAppendEntries(ctx, args)
}

func (t *inMemTransport) SendInstallSnapshot(ctx context.Context, peer string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	h, err := t.reach(peer)
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	return h.HandleInstallSnapshot(ctx, args)
}

func (t *inMemTransport) Close() error { return nil }

func (t *inMemTransport) reach(peer string) (RPCHandler, error) {
	if t.net.isPartitioned(t.id, peer) {
		return nil, fmt.Errorf("raft: %s -> %s is partitioned", t.id, peer)
	}
	h, ok := t.net.getHandler(peer)
	if !ok {
		return nil, fmt.Errorf("raft: peer %s not registered", peer)
	}
	return h, nil
}
