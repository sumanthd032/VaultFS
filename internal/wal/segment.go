package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// segment represents one rolling file on disk.
// The active (last) segment's file handle is kept open for appends.
// Older segments have file == nil and are only opened on demand for reads.
type segment struct {
	id   uint64
	path string
	file *os.File // non-nil only for the active segment
	size int64    // bytes written so far
}

// segmentPath returns the canonical path for segment id inside dir.
// Zero-padded to 16 digits so lexicographic order equals numeric order.
func segmentPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016d.wal", id))
}

// segmentIDFromPath parses the numeric ID embedded in a segment file name.
func segmentIDFromPath(path string) (uint64, error) {
	base := filepath.Base(path)
	base = strings.TrimSuffix(base, ".wal")
	id, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("wal: invalid segment name %q: %w", filepath.Base(path), err)
	}
	return id, nil
}

// createSegment creates a new, empty segment file and returns it open for writing.
func createSegment(dir string, id uint64) (*segment, error) {
	path := segmentPath(dir, id)
	// O_EXCL ensures we never silently overwrite an existing segment.
	f, err := os.OpenFile(filepath.Clean(path), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal: create segment %d: %w", id, err)
	}
	return &segment{id: id, path: path, file: f, size: 0}, nil
}

// openSegmentForWrite opens an existing segment file for appending.
// validBytes is the byte offset of the last confirmed good entry; the file is
// truncated to this boundary to discard any partial tail from a prior crash.
func openSegmentForWrite(path string, id uint64, validBytes int64) (*segment, error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal: open segment %d: %w", id, err)
	}

	if err := f.Truncate(validBytes); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: truncate segment %d to %d: %w", id, validBytes, err)
	}

	if _, err := f.Seek(0, 2); err != nil { // seek to end
		_ = f.Close()
		return nil, fmt.Errorf("wal: seek segment %d: %w", id, err)
	}

	return &segment{id: id, path: path, file: f, size: validBytes}, nil
}

// write appends p to the segment and updates its size counter.
func (s *segment) write(p []byte) error {
	n, err := s.file.Write(p)
	s.size += int64(n)
	if err != nil {
		return fmt.Errorf("wal: segment %d write: %w", s.id, err)
	}
	return nil
}

// sync flushes the segment to durable storage.
func (s *segment) sync() error {
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("wal: segment %d sync: %w", s.id, err)
	}
	return nil
}

// close syncs and closes the segment file.
func (s *segment) close() error {
	if s.file == nil {
		return nil
	}
	if err := s.file.Sync(); err != nil {
		_ = s.file.Close()
		s.file = nil
		return fmt.Errorf("wal: segment %d sync on close: %w", s.id, err)
	}
	if err := s.file.Close(); err != nil {
		s.file = nil
		return fmt.Errorf("wal: segment %d close: %w", s.id, err)
	}
	s.file = nil
	return nil
}
