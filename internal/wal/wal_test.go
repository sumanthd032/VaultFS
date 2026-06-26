package wal_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sumanthd032/vaultfs/internal/wal"
)

// helpers

func openWAL(t *testing.T, opts ...wal.Option) *wal.WAL {
	t.Helper()
	w, err := wal.Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Logf("Close: %v", err)
		}
	})
	return w
}

func openWALAt(t *testing.T, dir string, opts ...wal.Option) *wal.WAL {
	t.Helper()
	w, err := wal.Open(dir, opts...)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Logf("Close: %v", err)
		}
	})
	return w
}

func mustAppend(t *testing.T, w *wal.WAL, index uint64, data string) {
	t.Helper()
	if err := w.Append(wal.Entry{Index: index, Data: []byte(data)}); err != nil {
		t.Fatalf("Append(%d): %v", index, err)
	}
}

// TestWALEmpty verifies that a freshly opened WAL has no entries and lastIdx 0.
func TestWALEmpty(t *testing.T) {
	w := openWAL(t)

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("want 0 entries, got %d", len(entries))
	}
	if got := w.LastIndex(); got != 0 {
		t.Errorf("LastIndex = %d, want 0", got)
	}
}

// TestWALSingleEntry verifies append and readback for single entries of varying sizes.
func TestWALSingleEntry(t *testing.T) {
	tests := []struct {
		name  string
		index uint64
		data  string
	}{
		{"empty data", 1, ""},
		{"small data", 2, "hello"},
		{"binary-like data", 3, "\x00\xFF\x01\xFE"},
		{"large data", 4, string(make([]byte, 4096))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := openWAL(t)
			mustAppend(t, w, tt.index, tt.data)

			entries, err := w.ReadAll()
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if len(entries) != 1 {
				t.Fatalf("want 1 entry, got %d", len(entries))
			}
			if entries[0].Index != tt.index {
				t.Errorf("Index = %d, want %d", entries[0].Index, tt.index)
			}
			if string(entries[0].Data) != tt.data {
				t.Errorf("Data mismatch")
			}
		})
	}
}

// TestWALMultiEntry verifies that multiple appended entries are all recoverable
// in the same order.
func TestWALMultiEntry(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{"two entries", 2},
		{"ten entries", 10},
		{"hundred entries", 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := openWAL(t)
			for i := 1; i <= tt.count; i++ {
				mustAppend(t, w, uint64(i), fmt.Sprintf("entry-%d", i))
			}

			entries, err := w.ReadAll()
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if len(entries) != tt.count {
				t.Fatalf("want %d entries, got %d", tt.count, len(entries))
			}
			for i, e := range entries {
				if e.Index != uint64(i+1) {
					t.Errorf("entries[%d].Index = %d, want %d", i, e.Index, i+1)
				}
			}
		})
	}
}

// TestWALCloseAndReopen verifies that entries survive a close/reopen cycle.
func TestWALCloseAndReopen(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, 1, "alpha")
	mustAppend(t, w, 2, "beta")
	mustAppend(t, w, 3, "gamma")
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2 := openWALAt(t, dir)
	entries, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after reopen: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("want 3 entries after reopen, got %d", len(entries))
	}
	want := []struct {
		idx  uint64
		data string
	}{{1, "alpha"}, {2, "beta"}, {3, "gamma"}}
	for i, e := range entries {
		if e.Index != want[i].idx || string(e.Data) != want[i].data {
			t.Errorf("entries[%d] = {%d, %q}, want {%d, %q}",
				i, e.Index, e.Data, want[i].idx, want[i].data)
		}
	}
	if w2.LastIndex() != 3 {
		t.Errorf("LastIndex = %d, want 3", w2.LastIndex())
	}
}

// TestWALCrashRecovery_TruncatedEntry verifies that an entry truncated at the
// file tail (simulating a crash mid-write) is silently discarded on recovery.
func TestWALCrashRecovery_TruncatedEntry(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, 1, "committed")
	mustAppend(t, w, 2, "committed-2")
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Find the segment file and append a partial record to simulate crash.
	segs, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil || len(segs) == 0 {
		t.Fatal("no segment files found")
	}
	f, err := os.OpenFile(segs[0], os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Write an incomplete header (only 5 of 12 bytes) to simulate crash mid-write.
	if _, err := f.Write([]byte{0x0A, 0x00, 0x00, 0x00, 0x00}); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: truncated tail must be discarded.
	w2 := openWALAt(t, dir)
	entries, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 entries after crash recovery, got %d", len(entries))
	}
	if entries[0].Index != 1 || string(entries[0].Data) != "committed" {
		t.Errorf("unexpected entry[0]: %+v", entries[0])
	}
	if entries[1].Index != 2 || string(entries[1].Data) != "committed-2" {
		t.Errorf("unexpected entry[1]: %+v", entries[1])
	}

	// Appending after recovery must work.
	mustAppend(t, w2, 3, "post-recovery")
	entries, err = w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll post-recovery: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("want 3 entries after post-recovery append, got %d", len(entries))
	}
}

// TestWALCRCCorruption verifies that a bit-flipped CRC causes the entry—and
// all subsequent entries—to be discarded during recovery.
func TestWALCRCCorruption(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, 1, "first")
	mustAppend(t, w, 2, "second")
	mustAppend(t, w, 3, "third")
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segs, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil || len(segs) == 0 {
		t.Fatal("no segment files found")
	}

	data, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatal(err)
	}

	// Entry 1 size: headerSize(12) + idxFieldSize(8) + len("first")(5) = 25.
	// CRC field for entry 2 starts at offset 25+8 = 33 (within the 4-byte CRC).
	const entry1Size = 12 + 8 + 5
	if len(data) <= entry1Size+9 {
		t.Fatalf("segment too small (%d bytes)", len(data))
	}
	data[entry1Size+8] ^= 0xFF // flip CRC byte of entry 2

	if err := os.WriteFile(segs[0], data, 0600); err != nil { //nolint:gosec // segs[0] is a t.TempDir() path
		t.Fatal(err)
	}

	// Recovery must yield only entry 1.
	w2 := openWALAt(t, dir)
	entries, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry after CRC corruption, got %d", len(entries))
	}
	if entries[0].Index != 1 || string(entries[0].Data) != "first" {
		t.Errorf("unexpected entry: %+v", entries[0])
	}
}

// TestWALConcurrentAppends uses -race to detect data races during parallel writes.
func TestWALConcurrentAppends(t *testing.T) {
	w := openWAL(t)

	const (
		numWorkers  = 8
		perWorker   = 20
		totalWanted = numWorkers * perWorker
	)

	var counter atomic.Uint64
	var wg sync.WaitGroup
	errs := make(chan error, numWorkers)

	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				idx := counter.Add(1)
				e := wal.Entry{Index: idx, Data: []byte(fmt.Sprintf("w-%d", idx))}
				if err := w.Append(e); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent append: %v", err)
	}

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != totalWanted {
		t.Errorf("want %d entries, got %d", totalWanted, len(entries))
	}
}

// TestWALSegmentRotation verifies that the WAL creates a new segment file
// when the active one would exceed the size limit, and that all entries remain
// readable across segments.
func TestWALSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	// Each entry = headerSize(12) + idxFieldSize(8) + 10 bytes = 30 bytes.
	// With limit 80, we can fit 2 entries (60 bytes); 3rd triggers rotation.
	w := openWALAt(t, dir, wal.WithMaxSegmentSize(80))

	const numEntries = 9
	for i := 1; i <= numEntries; i++ {
		mustAppend(t, w, uint64(i), fmt.Sprintf("data-%05d", i)) // 10 bytes
	}

	segs, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatalf("want ≥2 segment files after rotation, got %d", len(segs))
	}

	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != numEntries {
		t.Fatalf("want %d entries across segments, got %d", numEntries, len(entries))
	}
	for i, e := range entries {
		if e.Index != uint64(i+1) {
			t.Errorf("entries[%d].Index = %d, want %d", i, e.Index, i+1)
		}
	}
}

// TestWALSegmentRotationSurvivesReopen verifies that entries across multiple
// segments are still readable after a close/reopen cycle.
func TestWALSegmentRotationSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, wal.WithMaxSegmentSize(80))
	if err != nil {
		t.Fatal(err)
	}

	const numEntries = 6
	for i := 1; i <= numEntries; i++ {
		mustAppend(t, w, uint64(i), fmt.Sprintf("data-%05d", i))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2 := openWALAt(t, dir, wal.WithMaxSegmentSize(80))
	entries, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after reopen: %v", err)
	}
	if len(entries) != numEntries {
		t.Fatalf("want %d entries after reopen, got %d", numEntries, len(entries))
	}
	if w2.LastIndex() != numEntries {
		t.Errorf("LastIndex = %d, want %d", w2.LastIndex(), numEntries)
	}
}

// TestWALLastIndex verifies that LastIndex is updated correctly on each append.
func TestWALLastIndex(t *testing.T) {
	w := openWAL(t)

	if got := w.LastIndex(); got != 0 {
		t.Errorf("initial LastIndex = %d, want 0", got)
	}
	mustAppend(t, w, 5, "five")
	if got := w.LastIndex(); got != 5 {
		t.Errorf("LastIndex = %d, want 5", got)
	}
	mustAppend(t, w, 10, "ten")
	if got := w.LastIndex(); got != 10 {
		t.Errorf("LastIndex = %d, want 10", got)
	}
}

// TestWALLastIndexRestoredOnReopen verifies that LastIndex is correctly
// recovered from disk.
func TestWALLastIndexRestoredOnReopen(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, 7, "seven")
	mustAppend(t, w, 42, "forty-two")
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2 := openWALAt(t, dir)
	if got := w2.LastIndex(); got != 42 {
		t.Errorf("LastIndex after reopen = %d, want 42", got)
	}
}
