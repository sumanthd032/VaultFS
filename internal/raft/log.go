package raft

import (
	"errors"
	"fmt"
	"sync"
)

// ErrCompacted is returned when the requested index has been discarded by a snapshot.
var ErrCompacted = errors.New("raft: log entry compacted into snapshot")

// ErrIndexNotFound is returned when the requested index is beyond the log.
var ErrIndexNotFound = errors.New("raft: log index not found")

// raftLog is an in-memory Raft log. Entries before snapIndex have been
// compacted; physical entries begin at snapIndex+1.
//
// Invariant: entries[i].Index == snapIndex + 1 + i
type raftLog struct {
	mu        sync.RWMutex // protects entries, snapIndex, snapTerm
	entries   []Entry
	snapIndex uint64
	snapTerm  uint64
}

func newRaftLog() *raftLog {
	return &raftLog{}
}

// LastIndex returns the index of the most recent entry (0 if empty).
func (l *raftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex()
}

func (l *raftLog) lastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.snapIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the most recent entry (0 if empty).
func (l *raftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastTerm()
}

func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// Term returns the term for the given index, or an error if unavailable.
func (l *raftLog) Term(index uint64) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 {
		return 0, nil
	}
	if index == l.snapIndex {
		return l.snapTerm, nil
	}
	if index < l.snapIndex {
		return 0, fmt.Errorf("%w: index %d", ErrCompacted, index)
	}
	offset := index - l.snapIndex - 1
	if offset >= uint64(len(l.entries)) {
		return 0, fmt.Errorf("%w: index %d", ErrIndexNotFound, index)
	}
	return l.entries[offset].Term, nil
}

// Append adds entries to the end of the log.
func (l *raftLog) Append(entries ...Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

// TruncateAfter removes all entries with Index >= index.
func (l *raftLog) TruncateAfter(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= l.snapIndex {
		l.entries = nil
		return
	}
	offset := index - l.snapIndex - 1
	if offset < uint64(len(l.entries)) {
		l.entries = l.entries[:offset]
	}
}

// Slice returns a copy of entries with Index in [from, to).
// Returns nil if the range is empty or compacted.
func (l *raftLog) Slice(from, to uint64) []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if from >= to || from <= l.snapIndex || len(l.entries) == 0 {
		return nil
	}
	fromOff := from - l.snapIndex - 1
	toOff := to - l.snapIndex - 1
	if fromOff >= uint64(len(l.entries)) {
		return nil
	}
	if toOff > uint64(len(l.entries)) {
		toOff = uint64(len(l.entries))
	}
	result := make([]Entry, toOff-fromOff)
	copy(result, l.entries[fromOff:toOff])
	return result
}

// SetSnapshot discards all entries up to and including index, recording the
// snapshot's last-included term. This is the log compaction step.
func (l *raftLog) SetSnapshot(index, term uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index <= l.snapIndex {
		return
	}
	// entries[offset] is the first entry with Index > index.
	offset := index - l.snapIndex
	if offset >= uint64(len(l.entries)) {
		l.entries = nil
	} else {
		remaining := make([]Entry, uint64(len(l.entries))-offset)
		copy(remaining, l.entries[offset:])
		l.entries = remaining
	}
	l.snapIndex = index
	l.snapTerm = term
}

// Len returns the number of in-memory entries (not including snapshotted entries).
func (l *raftLog) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}
