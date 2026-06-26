package raft

import (
	"context"
	"log/slog"
	"sort"
	"sync"
)

// maybeSendHeartbeats sends AppendEntries to all peers if this node is the
// leader. Called from the main loop; must NOT hold n.mu.
func (n *Node) maybeSendHeartbeats() {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}
	peers := append([]string(nil), n.peers...)
	n.mu.RUnlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			n.replicateTo(peer)
		}(peer)
	}
	wg.Wait()
}

// replicateTo sends the appropriate AppendEntries (or heartbeat) to one peer.
// Must NOT hold n.mu when called.
func (n *Node) replicateTo(peer string) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peer]
	prevIdx := nextIdx - 1
	prevTerm, err := n.log.Term(prevIdx)
	if err != nil {
		// The peer needs a snapshot; we'll handle that in snapshot.go.
		n.mu.Unlock()
		n.sendSnapshot(peer)
		return
	}

	entries := n.log.Slice(nextIdx, n.log.lastIndex()+1)
	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.HeartbeatInterval*3)
	defer cancel()

	reply, err := n.transport.SendAppendEntries(ctx, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != args.Term {
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + uint64(len(args.Entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitIndex()
	} else {
		// Fast log back-up: use ConflictIndex/ConflictTerm if provided.
		if reply.ConflictTerm != 0 {
			// Find the last entry in our log with ConflictTerm.
			newNext := reply.ConflictIndex
			lastIdx := n.log.lastIndex()
			for i := lastIdx; i > n.log.snapIndex; i-- {
				t, terr := n.log.Term(i)
				if terr != nil {
					break
				}
				if t == reply.ConflictTerm {
					newNext = i + 1
					break
				}
				if t < reply.ConflictTerm {
					break
				}
			}
			n.nextIndex[peer] = newNext
		} else if reply.ConflictIndex > 0 {
			n.nextIndex[peer] = reply.ConflictIndex
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

// advanceCommitIndex advances commitIndex when a new entry is replicated on a
// majority. Must be called with n.mu held and state == Leader.
func (n *Node) advanceCommitIndex() {
	// Collect all matchIndex values (including self).
	indices := make([]uint64, 0, len(n.matchIndex))
	for _, idx := range n.matchIndex {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] > indices[j] })

	// The median is the highest index replicated on a majority.
	quorum := (len(n.peers) + 1 + 1) / 2 // majority of total nodes (peers + self)
	if quorum > len(indices) {
		return
	}
	newCommit := indices[quorum-1]

	// Only advance for entries from the current term (section 5.4.2).
	if newCommit > n.commitIndex {
		t, err := n.log.Term(newCommit)
		if err == nil && t == n.currentTerm {
			n.commitIndex = newCommit
			slog.Debug("raft: commit index advanced", "node", n.id, "commit", newCommit)
			n.signalApply()
		}
	}
}

// HandleAppendEntries processes an inbound AppendEntries RPC (section 5.3).
func (n *Node) HandleAppendEntries(_ context.Context, args AppendEntriesArgs) (AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply, nil
	}
	if args.Term > n.currentTerm || n.state != Follower {
		n.becomeFollower(args.Term)
		reply.Term = n.currentTerm
	}
	n.leaderID = args.LeaderID
	n.signalElectionReset()

	// Check that prevLogIndex/prevLogTerm match our log.
	if args.PrevLogIndex > 0 {
		prevTerm, err := n.log.Term(args.PrevLogIndex)
		if err != nil || prevTerm != args.PrevLogTerm {
			// Tell the leader where to roll back.
			reply.ConflictIndex, reply.ConflictTerm = n.conflictHint(args.PrevLogIndex, prevTerm)
			return reply, nil
		}
	}

	// Append new entries, removing any conflicting tail first.
	for i, e := range args.Entries {
		existingTerm, err := n.log.Term(e.Index)
		if err != nil {
			// Index not in log; append remaining entries.
			n.log.Append(args.Entries[i:]...)
			break
		}
		if existingTerm != e.Term {
			// Conflict: delete this entry and all that follow, then append.
			n.log.TruncateAfter(e.Index)
			n.log.Append(args.Entries[i:]...)
			break
		}
		// Terms match: entry already present, keep scanning.
	}

	// Update commitIndex.
	if args.LeaderCommit > n.commitIndex {
		lastNew := n.log.lastIndex()
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
		n.signalApply()
	}

	reply.Success = true
	return reply, nil
}

// conflictHint builds the ConflictIndex/ConflictTerm for a fast log rollback.
// Must be called with n.mu held.
func (n *Node) conflictHint(prevLogIndex, prevTerm uint64) (conflictIndex, conflictTerm uint64) {
	lastIdx := n.log.lastIndex()
	if prevLogIndex > lastIdx {
		// Follower log is too short.
		return lastIdx + 1, 0
	}
	// Find the first index of the conflicting term.
	conflictTerm = prevTerm
	conflictIndex = prevLogIndex
	for conflictIndex > n.log.snapIndex+1 {
		t, err := n.log.Term(conflictIndex - 1)
		if err != nil || t != conflictTerm {
			break
		}
		conflictIndex--
	}
	return conflictIndex, conflictTerm
}
