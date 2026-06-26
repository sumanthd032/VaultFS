package raft

import (
	"context"
	"log/slog"
)

// TakeSnapshot compacts the log up to lastApplied and returns the resulting
// Snapshot. The caller supplies the serialised state-machine state in data.
func (n *Node) TakeSnapshot(data []byte) (Snapshot, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	idx := n.lastApplied
	term, err := n.log.Term(idx)
	if err != nil {
		return Snapshot{}, err
	}
	n.log.SetSnapshot(idx, term)
	slog.Info("raft: snapshot taken", "node", n.id, "index", idx, "term", term)
	return Snapshot{
		LastIncludedIndex: idx,
		LastIncludedTerm:  term,
		Data:              data,
	}, nil
}

// sendSnapshot sends an InstallSnapshot RPC to a lagging peer.
// Must NOT hold n.mu when called.
func (n *Node) sendSnapshot(peer string) {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: n.log.snapIndex,
		LastIncludedTerm:  n.log.snapTerm,
		Done:              true,
	}
	n.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.HeartbeatInterval*10)
	defer cancel()

	reply, err := n.transport.SendInstallSnapshot(ctx, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}
	if n.state != Leader {
		return
	}
	// After a successful snapshot delivery the peer is caught up to snapIndex.
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	n.nextIndex[peer] = args.LastIncludedIndex + 1
}

// HandleInstallSnapshot processes an inbound InstallSnapshot RPC (§7).
func (n *Node) HandleInstallSnapshot(_ context.Context, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := InstallSnapshotReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply, nil
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
		reply.Term = n.currentTerm
	}
	n.leaderID = args.LeaderID
	n.signalElectionReset()

	if args.LastIncludedIndex <= n.log.snapIndex {
		return reply, nil // already have a newer snapshot
	}

	n.log.SetSnapshot(args.LastIncludedIndex, args.LastIncludedTerm)
	if args.LastIncludedIndex > n.commitIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > n.lastApplied {
		n.lastApplied = args.LastIncludedIndex
	}

	slog.Info("raft: installed snapshot", "node", n.id,
		"index", args.LastIncludedIndex, "term", args.LastIncludedTerm)
	return reply, nil
}
