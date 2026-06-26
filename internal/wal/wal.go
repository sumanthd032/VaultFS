package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DefaultMaxSegmentSize is the default rolling segment size (64 MiB).
// When a segment reaches this size the WAL creates a new one.
const DefaultMaxSegmentSize int64 = 64 * 1024 * 1024

// WAL is a write-ahead log backed by a sequence of rolling segment files.
// All exported methods are safe for concurrent use.
type WAL struct {
	mu       sync.Mutex // protects segments, lastIdx
	dir      string
	segments []*segment // ordered oldest -> newest; last element is active
	lastIdx  uint64
	maxSegSz int64
	onAppend func(time.Duration) // optional latency observer
}

// Option is a functional option for Open.
type Option func(*WAL)

// WithMaxSegmentSize sets the maximum byte size of a single segment file.
// When a pending write would exceed this limit, the WAL rotates to a new
// segment before writing. Useful in tests to force rotation with small data.
func WithMaxSegmentSize(n int64) Option {
	return func(w *WAL) { w.maxSegSz = n }
}

// WithAppendObserver registers a callback invoked with the wall-clock duration
// of each successful Append, used for latency metrics. It keeps the WAL
// decoupled from any particular metrics implementation.
func WithAppendObserver(fn func(time.Duration)) Option {
	return func(w *WAL) { w.onAppend = fn }
}

// Open opens (or creates) a WAL rooted at dir.
//
// On first use, Open creates dir and an initial segment file. On subsequent
// opens it replays all existing segments to restore lastIdx, truncating any
// partial tail entry left by a prior crash.
func Open(dir string, opts ...Option) (*WAL, error) {
	w := &WAL{
		dir:      dir,
		maxSegSz: DefaultMaxSegmentSize,
	}
	for _, opt := range opts {
		opt(w)
	}

	if err := os.MkdirAll(filepath.Clean(dir), 0700); err != nil {
		return nil, fmt.Errorf("wal: create dir %q: %w", dir, err)
	}

	if err := w.recover(); err != nil {
		return nil, err
	}

	return w, nil
}

// Append writes entry to disk and fsyncs before returning.
//
// The WAL rotates to a new segment file when the current one would exceed
// the configured maximum size. Index values must be supplied by the caller;
// the WAL does not enforce monotonicity (Raft log replication needs this
// flexibility during log truncation).
func (w *WAL) Append(entry Entry) error {
	if w.onAppend != nil {
		start := time.Now()
		defer func() { w.onAppend(time.Since(start)) }()
	}

	var buf bytes.Buffer
	if err := entry.Encode(&buf); err != nil {
		return err
	}
	encoded := buf.Bytes()

	w.mu.Lock()
	defer w.mu.Unlock()

	active := w.active()
	if active.size+int64(len(encoded)) > w.maxSegSz {
		if err := w.rotate(); err != nil {
			return err
		}
		active = w.active()
	}

	if err := active.write(encoded); err != nil {
		return err
	}
	if err := active.sync(); err != nil {
		return err
	}

	if entry.Index > w.lastIdx {
		w.lastIdx = entry.Index
	}
	return nil
}

// ReadAll returns all valid entries from every segment, in append order.
// It re-reads from disk on each call to guarantee up-to-date results.
func (w *WAL) ReadAll() ([]Entry, error) {
	w.mu.Lock()
	segs := make([]*segment, len(w.segments))
	copy(segs, w.segments)
	w.mu.Unlock()

	var all []Entry
	for _, seg := range segs {
		entries, _, err := replayFile(seg.path)
		if err != nil {
			return nil, fmt.Errorf("wal: read segment %d: %w", seg.id, err)
		}
		all = append(all, entries...)
	}
	return all, nil
}

// LastIndex returns the index of the most recently appended entry.
// Returns 0 if the WAL is empty.
func (w *WAL) LastIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIdx
}

// Close syncs and closes the active segment. The WAL must not be used after Close.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.active().close()
}

// active returns the last (writable) segment. Caller must hold mu.
func (w *WAL) active() *segment {
	return w.segments[len(w.segments)-1]
}

// rotate closes the current active segment and creates the next one.
// Caller must hold mu.
func (w *WAL) rotate() error {
	active := w.active()
	if err := active.close(); err != nil {
		return fmt.Errorf("wal: rotate close: %w", err)
	}

	newID := active.id + 1
	seg, err := createSegment(w.dir, newID)
	if err != nil {
		return fmt.Errorf("wal: rotate create: %w", err)
	}
	w.segments = append(w.segments, seg)
	return nil
}
