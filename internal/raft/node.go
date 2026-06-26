package raft

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// compactThreshold is the log length that triggers a snapshot.
const compactThreshold = 10_000

// Node is a single Raft consensus participant.
//
// Start it with Start(); propose commands with Propose(); shut it down with
// Stop(). Committed entries are delivered in order on cfg.CommitCh.
type Node struct {
	mu sync.RWMutex // protects all fields below

	id    string
	cfg   Config // immutable after Start
	peers []string

	// Persistent Raft state (must survive restarts in production; in-memory here).
	currentTerm uint64
	votedFor    string
	log         *raftLog

	// Volatile state
	state       NodeState
	leaderID    string
	commitIndex uint64
	lastApplied uint64

	// Leader-only volatile state (re-initialised on each election win).
	nextIndex  map[string]uint64 // peer → next log index to send
	matchIndex map[string]uint64 // peer → highest replicated index

	transport Transport

	// Internal signalling channels; all buffered to avoid blocking.
	stopCh        chan struct{}
	electionReset chan struct{} // main loop: reset election timer
	heartbeatSig  chan struct{} // main loop: send heartbeats immediately
	applySig      chan struct{} // apply loop: commitIndex advanced
}

// New creates a Node from cfg. Call Start() to begin participating in consensus.
func New(cfg Config) *Node {
	n := &Node{
		id:            cfg.ID,
		cfg:           cfg,
		peers:         cfg.Peers,
		log:           newRaftLog(),
		state:         Follower,
		stopCh:        make(chan struct{}),
		electionReset: make(chan struct{}, 1),
		heartbeatSig:  make(chan struct{}, 1),
		applySig:      make(chan struct{}, 1),
	}
	return n
}

// Start registers the node with its transport and launches background goroutines.
func (n *Node) Start(t Transport) {
	n.transport = t
	t.Register(n.id, n)
	go n.run()
	go n.applyLoop()
}

// Stop shuts the node down cleanly. It is safe to call Stop more than once.
func (n *Node) Stop() {
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
}

// Propose appends a command to the leader's log. Returns an error if this node
// is not the current leader.
func (n *Node) Propose(ctx context.Context, command []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return ErrNotLeader
	}
	nextIndex := n.log.lastIndex() + 1
	e := Entry{Index: nextIndex, Term: n.currentTerm, Command: command}
	n.log.Append(e)
	n.matchIndex[n.id] = nextIndex

	// Signal replication goroutines.
	select {
	case n.heartbeatSig <- struct{}{}:
	default:
	}
	return nil
}

// State returns the node's current Raft role.
func (n *Node) State() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// LeaderID returns the ID of the current leader, or "" if unknown.
func (n *Node) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

// Term returns the node's current term.
func (n *Node) Term() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// run is the main Raft state-machine loop.
func (n *Node) run() {
	heartbeatTicker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	electionTimer := time.NewTimer(n.randomElectionTimeout())
	defer electionTimer.Stop()

	for {
		select {
		case <-n.stopCh:
			return

		case <-heartbeatTicker.C:
			n.maybeSendHeartbeats()

		case <-n.heartbeatSig:
			n.maybeSendHeartbeats()

		case <-electionTimer.C:
			n.maybeStartElection()
			electionTimer.Reset(n.randomElectionTimeout())

		case <-n.electionReset:
			if !electionTimer.Stop() {
				select {
				case <-electionTimer.C:
				default:
				}
			}
			electionTimer.Reset(n.randomElectionTimeout())
		}
	}
}

// applyLoop delivers committed entries to cfg.CommitCh.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applySig:
			n.applyCommitted()
		}
	}
}

func (n *Node) applyCommitted() {
	n.mu.Lock()
	commit := n.commitIndex
	applied := n.lastApplied
	n.mu.Unlock()

	if commit <= applied {
		return
	}
	entries := n.log.Slice(applied+1, commit+1)
	for _, e := range entries {
		if n.cfg.CommitCh != nil {
			select {
			case n.cfg.CommitCh <- e:
			case <-n.stopCh:
				return
			}
		}
		n.mu.Lock()
		n.lastApplied = e.Index
		n.mu.Unlock()
	}

	// Trigger snapshot if log is getting large.
	if n.log.Len() >= compactThreshold {
		n.mu.Lock()
		last := n.lastApplied
		term, err := n.log.Term(last)
		n.mu.Unlock()
		if err == nil {
			n.log.SetSnapshot(last, term)
			slog.Info("raft: compacted log", "node", n.id, "snap_index", last)
		}
	}
}

// becomeFollower transitions the node to Follower for the given term.
// Must be called with n.mu held.
func (n *Node) becomeFollower(term uint64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	slog.Debug("raft: became follower", "node", n.id, "term", term)
}

// becomeLeader transitions the node to Leader. Must be called with n.mu held.
func (n *Node) becomeLeader() {
	if n.state != Candidate {
		return
	}
	n.state = Leader
	n.leaderID = n.id
	lastIdx := n.log.lastIndex()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}
	// Count self in matchIndex for commit quorum calculation.
	n.matchIndex[n.id] = lastIdx
	slog.Info("raft: became leader", "node", n.id, "term", n.currentTerm)

	select {
	case n.heartbeatSig <- struct{}{}:
	default:
	}
}

// signalElectionReset tells the main loop to reset the election timer.
// Safe to call without holding n.mu.
func (n *Node) signalElectionReset() {
	select {
	case n.electionReset <- struct{}{}:
	default:
	}
}

// signalApply tells the apply loop that commitIndex may have advanced.
func (n *Node) signalApply() {
	select {
	case n.applySig <- struct{}{}:
	default:
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	min := n.cfg.ElectionMinTimeout
	spread := int64(n.cfg.ElectionMaxTimeout - min)
	return min + time.Duration(rand.Int63n(spread)) //nolint:gosec
}
