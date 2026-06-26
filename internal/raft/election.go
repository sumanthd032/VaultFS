package raft

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
)

// ErrNotLeader is returned by Propose when this node is not the current leader.
var ErrNotLeader = errors.New("raft: not the leader")

// maybeStartElection triggers a new election if the node is not already a leader.
// Called from the main loop when the election timer fires; must NOT hold n.mu.
func (n *Node) maybeStartElection() {
	n.mu.Lock()
	if n.state == Leader {
		n.mu.Unlock()
		return
	}

	// Transition to Candidate.
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	term := n.currentTerm
	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	slog.Info("raft: starting election", "node", n.id, "term", term)

	votes := int64(1) // vote for self
	quorum := int64((len(peers)+1)/2 + 1)
	var once sync.Once
	var wg sync.WaitGroup

	// Single-node cluster: we already have quorum.
	if votes >= quorum {
		n.mu.Lock()
		if n.state == Candidate && n.currentTerm == term {
			once.Do(func() { n.becomeLeader() })
		}
		n.mu.Unlock()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionMinTimeout)
	defer cancel()

	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			reply, err := n.transport.SendRequestVote(ctx, peer, RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if !reply.VoteGranted || n.state != Candidate || n.currentTerm != term {
				return
			}

			if atomic.AddInt64(&votes, 1) >= quorum {
				once.Do(func() { n.becomeLeader() })
			}
		}(peer)
	}
	// Don't wait — goroutines update state when they get replies.
}

// HandleRequestVote processes an inbound RequestVote RPC (§5.2, §5.4).
func (n *Node) HandleRequestVote(_ context.Context, args RequestVoteArgs) (RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply, nil
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
		reply.Term = n.currentTerm
	}

	// Grant vote only if we haven't voted yet (or already voted for this candidate)
	// AND the candidate's log is at least as up-to-date as ours.
	alreadyVoted := n.votedFor != "" && n.votedFor != args.CandidateID
	if alreadyVoted {
		return reply, nil
	}
	if !n.logUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return reply, nil
	}

	n.votedFor = args.CandidateID
	reply.VoteGranted = true
	n.signalElectionReset()
	slog.Debug("raft: voted", "node", n.id, "for", args.CandidateID, "term", args.Term)
	return reply, nil
}

// logUpToDate returns true if the candidate's log is at least as up-to-date as
// ours, using the Raft log comparison rule (§5.4.1).
// Must be called with n.mu held.
func (n *Node) logUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	myLastTerm := n.log.lastTerm()
	myLastIndex := n.log.lastIndex()

	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIndex >= myLastIndex
}
