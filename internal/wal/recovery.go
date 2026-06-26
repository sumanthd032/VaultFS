package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// replayFile reads all valid entries from the segment at path.
//
// It stops-without returning an error-when it encounters:
//   - io.EOF (clean end of file)
//   - io.ErrUnexpectedEOF (crash-truncated partial entry)
//   - ErrChecksumMismatch (bit-flipped data)
//
// validBytes is the file offset immediately after the last successfully decoded
// entry. Callers use this to truncate the file to a safe boundary.
func replayFile(path string) (entries []Entry, validBytes int64, err error) {
	f, err := os.Open(filepath.Clean(path)) //nolint:gosec
	if err != nil {
		return nil, 0, fmt.Errorf("wal: open for replay %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var offset int64
	for {
		entry, decErr := Decode(f)
		if decErr != nil {
			if decErr == io.EOF ||
				errors.Is(decErr, io.ErrUnexpectedEOF) ||
				errors.Is(decErr, ErrChecksumMismatch) {
				break
			}
			return entries, offset, fmt.Errorf("wal: replay decode: %w", decErr)
		}
		offset += encodedSize(len(entry.Data))
		entries = append(entries, entry)
	}
	return entries, offset, nil
}

// recover scans dir for existing segment files, replays them in order, and
// prepares the WAL for appending. The last segment is truncated to its last
// valid entry boundary to recover from a prior crash.
func (w *WAL) recover() error {
	pattern := filepath.Join(w.dir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("wal: glob segments: %w", err)
	}
	sort.Strings(matches)

	for i, path := range matches {
		id, idErr := segmentIDFromPath(path)
		if idErr != nil {
			return idErr
		}

		entries, validEnd, repErr := replayFile(path)
		if repErr != nil {
			return fmt.Errorf("wal: replay %q: %w", path, repErr)
		}

		for _, e := range entries {
			if e.Index > w.lastIdx {
				w.lastIdx = e.Index
			}
		}

		isLast := i == len(matches)-1
		if isLast {
			seg, openErr := openSegmentForWrite(path, id, validEnd)
			if openErr != nil {
				return fmt.Errorf("wal: open active segment: %w", openErr)
			}
			w.segments = append(w.segments, seg)
		} else {
			info, stErr := os.Stat(path)
			if stErr != nil {
				return fmt.Errorf("wal: stat %q: %w", path, stErr)
			}
			w.segments = append(w.segments, &segment{
				id:   id,
				path: path,
				size: info.Size(),
			})
		}
	}

	// No existing segments: create the first one.
	if len(w.segments) == 0 {
		seg, cErr := createSegment(w.dir, 1)
		if cErr != nil {
			return fmt.Errorf("wal: create initial segment: %w", cErr)
		}
		w.segments = append(w.segments, seg)
	}

	return nil
}
