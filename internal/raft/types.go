// Package raft implements the Raft consensus algorithm for leader election and
// replicated log management across VaultFS master nodes.
//
// A cluster of N nodes tolerates (N-1)/2 failures. Each node is either a
// Follower, Candidate, or Leader. The package exposes a Node that callers
// drive by proposing commands; committed entries are delivered on CommitCh.
package raft

import "time"

// NodeState represents the Raft consensus role.
type NodeState int

const (
	Follower  NodeState = iota
	Candidate           // nolint:deadcode
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Entry is a single Raft log entry.
type Entry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Config holds the tuning parameters for a Raft node.
// All timeout values must be positive. ElectionMinTimeout < ElectionMaxTimeout.
type Config struct {
	ID                 string
	Peers              []string
	ElectionMinTimeout time.Duration
	ElectionMaxTimeout time.Duration
	HeartbeatInterval  time.Duration
	// CommitCh receives committed entries in order. If nil, commits are discarded.
	CommitCh chan<- Entry
}

// DefaultConfig returns a production-ready Config for the given node and peers.
func DefaultConfig(id string, peers []string, commitCh chan<- Entry) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		ElectionMinTimeout: 150 * time.Millisecond,
		ElectionMaxTimeout: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		CommitCh:           commitCh,
	}
}

// RequestVoteArgs is the payload for the RequestVote RPC (section 5.2).
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the reply for the RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the payload for the AppendEntries RPC (section 5.3).
// When Entries is empty this acts as a heartbeat.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesReply is the reply for the AppendEntries RPC.
// ConflictIndex/ConflictTerm support the fast log back-up optimisation (section 5.3).
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}

// InstallSnapshotArgs is the payload for the InstallSnapshot RPC (section 7).
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
	Offset            int64
	Done              bool
}

// InstallSnapshotReply is the reply for the InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term uint64
}

// Snapshot captures the state machine state up to LastIncludedIndex.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}
