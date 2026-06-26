package metadata

import (
	"errors"
	"testing"
)

// openTestStore opens a BadgerDB store in a temporary directory that is
// automatically cleaned up when the test ends.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Store.Close: %v", err)
		}
	})
	return s
}

// -- Store --------------------------------------------------------------------

func TestStorePutGet(t *testing.T) {
	s := openTestStore(t)
	if err := s.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "value" {
		t.Errorf("Get = %q, want %q", got, "value")
	}
}

func TestStoreGetMissingKey(t *testing.T) {
	s := openTestStore(t)
	_, err := s.Get([]byte("missing"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get missing key: got %v, want ErrKeyNotFound", err)
	}
}

func TestStoreDelete(t *testing.T) {
	s := openTestStore(t)
	_ = s.Put([]byte("k"), []byte("v"))
	if err := s.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := s.Get([]byte("k"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("after Delete: got %v, want ErrKeyNotFound", err)
	}
}

func TestStoreDeleteMissingIsNoop(t *testing.T) {
	s := openTestStore(t)
	if err := s.Delete([]byte("ghost")); err != nil {
		t.Errorf("Delete missing key should be a no-op, got %v", err)
	}
}

func TestStoreScan(t *testing.T) {
	s := openTestStore(t)
	_ = s.Put([]byte("a/1"), []byte("v1"))
	_ = s.Put([]byte("a/2"), []byte("v2"))
	_ = s.Put([]byte("b/3"), []byte("v3"))

	var keys []string
	if err := s.Scan([]byte("a/"), func(k, _ []byte) error {
		keys = append(keys, string(k))
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("Scan returned %d keys, want 2: %v", len(keys), keys)
	}
}

func TestStoreTxnCommit(t *testing.T) {
	s := openTestStore(t)
	txn := s.NewTxn()
	defer txn.Discard()

	if err := txn.Set([]byte("tx-key"), []byte("tx-val")); err != nil {
		t.Fatalf("txn.Set: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("txn.Commit: %v", err)
	}
	got, err := s.Get([]byte("tx-key"))
	if err != nil {
		t.Fatalf("Get after commit: %v", err)
	}
	if string(got) != "tx-val" {
		t.Errorf("Get = %q, want %q", got, "tx-val")
	}
}

func TestStoreTxnDiscard(t *testing.T) {
	s := openTestStore(t)
	txn := s.NewTxn()
	_ = txn.Set([]byte("discard-key"), []byte("v"))
	txn.Discard() // roll back

	_, err := s.Get([]byte("discard-key"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("after Discard: got %v, want ErrKeyNotFound", err)
	}
}

// -- Namespace ----------------------------------------------------------------

func openTestNamespace(t *testing.T) *Namespace {
	t.Helper()
	return NewNamespace(openTestStore(t))
}

func TestNamespaceCreateAndStat(t *testing.T) {
	ns := openTestNamespace(t)
	fi := FileInfo{Path: "/foo.txt", Size: 42, ChunkIDs: []string{"c1"}}
	if err := ns.CreateFile(fi); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	got, err := ns.Stat("/foo.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got.Size != 42 {
		t.Errorf("Size = %d, want 42", got.Size)
	}
	if len(got.ChunkIDs) != 1 || got.ChunkIDs[0] != "c1" {
		t.Errorf("ChunkIDs = %v, want [c1]", got.ChunkIDs)
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt not set")
	}
}

func TestNamespaceStatNotFound(t *testing.T) {
	ns := openTestNamespace(t)
	_, err := ns.Stat("/ghost")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Stat missing: got %v, want ErrNotFound", err)
	}
}

func TestNamespaceCreateDuplicate(t *testing.T) {
	ns := openTestNamespace(t)
	fi := FileInfo{Path: "/dup.txt"}
	_ = ns.CreateFile(fi)
	if err := ns.CreateFile(fi); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("duplicate CreateFile: got %v, want ErrAlreadyExists", err)
	}
}

func TestNamespaceDeleteFile(t *testing.T) {
	ns := openTestNamespace(t)
	_ = ns.CreateFile(FileInfo{Path: "/del.txt"})
	if err := ns.DeleteFile("/del.txt"); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	_, err := ns.Stat("/del.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("after Delete: got %v, want ErrNotFound", err)
	}
}

func TestNamespaceDeleteNotFound(t *testing.T) {
	ns := openTestNamespace(t)
	if err := ns.DeleteFile("/ghost.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete missing: got %v, want ErrNotFound", err)
	}
}

func TestNamespaceListDir(t *testing.T) {
	ns := openTestNamespace(t)
	_ = ns.CreateDir("/mydir")
	_ = ns.CreateFile(FileInfo{Path: "/mydir/a.txt"})
	_ = ns.CreateFile(FileInfo{Path: "/mydir/b.txt"})
	// Nested file should NOT appear in ListDir("/mydir").
	_ = ns.CreateDir("/mydir/sub")
	_ = ns.CreateFile(FileInfo{Path: "/mydir/sub/c.txt"})

	entries, err := ns.ListDir("/mydir")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(entries) != 3 { // a.txt, b.txt, sub/
		t.Errorf("ListDir returned %d entries, want 3: %+v", len(entries), entries)
	}
}

func TestNamespaceRename(t *testing.T) {
	ns := openTestNamespace(t)
	_ = ns.CreateFile(FileInfo{Path: "/old.txt", Size: 7})

	if err := ns.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if _, err := ns.Stat("/old.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("old path should not exist after rename, got %v", err)
	}
	got, err := ns.Stat("/new.txt")
	if err != nil {
		t.Fatalf("Stat new path: %v", err)
	}
	if got.Size != 7 {
		t.Errorf("renamed file size = %d, want 7", got.Size)
	}
}

func TestNamespaceRenameSourceMissing(t *testing.T) {
	ns := openTestNamespace(t)
	if err := ns.Rename("/ghost.txt", "/new.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Rename missing source: got %v, want ErrNotFound", err)
	}
}

func TestNamespaceUpdateFile(t *testing.T) {
	ns := openTestNamespace(t)
	_ = ns.CreateFile(FileInfo{Path: "/update.txt", Size: 1})
	fi, _ := ns.Stat("/update.txt")
	fi.Size = 99
	if err := ns.UpdateFile(fi); err != nil {
		t.Fatalf("UpdateFile: %v", err)
	}
	got, _ := ns.Stat("/update.txt")
	if got.Size != 99 {
		t.Errorf("after UpdateFile: size = %d, want 99", got.Size)
	}
}

// -- ChunkMap ------------------------------------------------------------------

func TestChunkMapAddAndGet(t *testing.T) {
	cm := NewChunkMap()
	cm.AddLocation("chunk-1", Location{NodeID: "n1", Address: "host1:9000"})
	cm.AddLocation("chunk-1", Location{NodeID: "n2", Address: "host2:9000"})

	locs := cm.GetLocations("chunk-1")
	if len(locs) != 2 {
		t.Errorf("GetLocations = %d, want 2", len(locs))
	}
}

func TestChunkMapAddDuplicateNode(t *testing.T) {
	cm := NewChunkMap()
	cm.AddLocation("c1", Location{NodeID: "n1", Address: "old:9000"})
	cm.AddLocation("c1", Location{NodeID: "n1", Address: "new:9000"})

	locs := cm.GetLocations("c1")
	if len(locs) != 1 {
		t.Errorf("duplicate node should update in place, got %d locations", len(locs))
	}
	if locs[0].Address != "new:9000" {
		t.Errorf("address = %q, want %q", locs[0].Address, "new:9000")
	}
}

func TestChunkMapRemoveLocation(t *testing.T) {
	cm := NewChunkMap()
	cm.AddLocation("c1", Location{NodeID: "n1"})
	cm.AddLocation("c1", Location{NodeID: "n2"})
	cm.RemoveLocation("c1", "n1")

	locs := cm.GetLocations("c1")
	if len(locs) != 1 || locs[0].NodeID != "n2" {
		t.Errorf("after RemoveLocation: %+v", locs)
	}
}

func TestChunkMapRemoveLastLocation(t *testing.T) {
	cm := NewChunkMap()
	cm.AddLocation("c1", Location{NodeID: "n1"})
	cm.RemoveLocation("c1", "n1")

	if locs := cm.GetLocations("c1"); locs != nil {
		t.Errorf("after removing last location: got %v, want nil", locs)
	}
	if cm.ChunkCount() != 0 {
		t.Errorf("ChunkCount = %d after removing all, want 0", cm.ChunkCount())
	}
}

func TestChunkMapRemoveNode(t *testing.T) {
	cm := NewChunkMap()
	cm.AddLocation("c1", Location{NodeID: "dead"})
	cm.AddLocation("c1", Location{NodeID: "alive"})
	cm.AddLocation("c2", Location{NodeID: "dead"})
	cm.RemoveNode("dead")

	if locs := cm.GetLocations("c1"); len(locs) != 1 || locs[0].NodeID != "alive" {
		t.Errorf("c1 after RemoveNode: %+v", locs)
	}
	if locs := cm.GetLocations("c2"); locs != nil {
		t.Errorf("c2 should be gone after RemoveNode, got %+v", locs)
	}
}

func TestChunkMapGetUnknown(t *testing.T) {
	cm := NewChunkMap()
	if locs := cm.GetLocations("unknown"); locs != nil {
		t.Errorf("GetLocations unknown: got %v, want nil", locs)
	}
}

// -- ChunkVersion --------------------------------------------------------------

func TestChunkVersionHappensBefore(t *testing.T) {
	a := NewChunkVersion("c1")
	a = a.Increment("n1")

	b := a.Increment("n2")

	if !a.HappensBefore(b) {
		t.Error("a should happen before b")
	}
	if b.HappensBefore(a) {
		t.Error("b should not happen before a")
	}
}

func TestChunkVersionConcurrent(t *testing.T) {
	base := NewChunkVersion("c1").Increment("n1")
	a := base.Increment("n1") // n1 writes again
	b := base.Increment("n2") // n2 writes concurrently

	if !a.Concurrent(b) {
		t.Error("a and b should be concurrent (neither happens before the other)")
	}
}

func TestChunkVersionEqual(t *testing.T) {
	v := NewChunkVersion("c1").Increment("n1")
	w := NewChunkVersion("c1").Increment("n1")
	if !v.Equal(w) {
		t.Error("identical increments should be equal")
	}
}

func TestChunkVersionMerge(t *testing.T) {
	a := NewChunkVersion("c1").Increment("n1").Increment("n1") // n1=2
	b := NewChunkVersion("c1").Increment("n2")                 // n2=1

	merged := a.Merge(b)
	if !a.HappensBefore(merged) && !b.HappensBefore(merged) {
		t.Error("merged version should dominate both parents")
	}
}

func TestNamespaceStats(t *testing.T) {
	ns := openTestNamespace(t)
	if err := ns.CreateDir("/d"); err != nil {
		t.Fatal(err)
	}
	if err := ns.CreateFile(FileInfo{Path: "/d/a", ChunkIDs: []string{"c1", "c2"}}); err != nil {
		t.Fatal(err)
	}
	if err := ns.CreateFile(FileInfo{Path: "/d/b", ChunkIDs: []string{"c2", "c3"}}); err != nil {
		t.Fatal(err)
	}
	files, chunks, err := ns.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if files != 2 {
		t.Errorf("files = %d, want 2 (directories excluded)", files)
	}
	// Distinct chunks across both files: c1, c2, c3.
	if chunks != 3 {
		t.Errorf("chunks = %d, want 3 distinct", chunks)
	}
}
