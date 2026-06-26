package chunk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrChunkNotFound is returned when a requested chunk does not exist on disk.
var ErrChunkNotFound = errors.New("chunk: not found")

// fanoutLen is the number of leading hex characters used as a subdirectory.
// Sharding chunk files across 256 directories keeps any single directory small,
// which matters for filesystems whose lookups degrade with directory size.
const fanoutLen = 2

// Store is a disk-backed, content-addressed chunk store. It is safe for
// concurrent use.
//
// On-disk layout: <root>/<first 2 hex chars>/<full 64-char id>
type Store struct {
	root string

	mu      sync.Mutex           // protects writing, guarding concurrent writes of the same id
	writing map[ChunkID]struct{} // chunk IDs with an in-flight write
}

// NewStore opens (creating if necessary) a chunk store rooted at dir.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("chunk: create store dir %s: %w", dir, err)
	}
	return &Store{
		root:    dir,
		writing: make(map[ChunkID]struct{}),
	}, nil
}

// path returns the on-disk file path for id.
func (s *Store) path(id ChunkID) string {
	return filepath.Join(s.root, string(id[:fanoutLen]), string(id))
}

// WriteChunk stores data and returns its content-addressed ID. The write is
// atomic (temp file + fsync + rename) and idempotent: writing identical bytes
// twice is a no-op the second time. The data is verified against its hash
// before the rename, so a corrupt write never becomes visible.
func (s *Store) WriteChunk(ctx context.Context, data []byte) (ChunkID, error) {
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("chunk: write cancelled: %w", err)
	}

	id := Hash(data)
	dest := s.path(id)

	// Serialise concurrent writers of the same chunk; different chunks proceed
	// in parallel.
	s.mu.Lock()
	if _, inFlight := s.writing[id]; inFlight {
		s.mu.Unlock()
		return id, nil // another goroutine is already persisting identical bytes
	}
	s.writing[id] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.writing, id)
		s.mu.Unlock()
	}()

	// Already present (idempotent write).
	if _, err := os.Stat(dest); err == nil {
		return id, nil
	}

	if err := os.MkdirAll(filepath.Dir(dest), 0o750); err != nil {
		return "", fmt.Errorf("chunk: create fanout dir for %s: %w", id, err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dest), "."+string(id)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("chunk: create temp file for %s: %w", id, err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if we bail before the rename.
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("chunk: write data for %s: %w", id, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("chunk: fsync %s: %w", id, err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("chunk: close temp for %s: %w", id, err)
	}

	if err := os.Rename(tmpName, dest); err != nil {
		return "", fmt.Errorf("chunk: commit %s: %w", id, err)
	}
	return id, nil
}

// ReadChunk returns the bytes for id, verifying their integrity against the ID.
// It returns ErrChunkNotFound if the chunk is absent, or ErrChecksumFailed if
// the on-disk bytes have been corrupted.
func (s *Store) ReadChunk(ctx context.Context, id ChunkID) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("chunk: read cancelled: %w", err)
	}

	data, err := os.ReadFile(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrChunkNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("chunk: read %s: %w", id, err)
	}
	if err := Verify(id, data); err != nil {
		return nil, err
	}
	return data, nil
}

// DeleteChunk removes id from the store. Deleting a missing chunk is not an error.
func (s *Store) DeleteChunk(ctx context.Context, id ChunkID) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("chunk: delete cancelled: %w", err)
	}
	if err := os.Remove(s.path(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("chunk: delete %s: %w", id, err)
	}
	return nil
}

// Has reports whether id is present in the store.
func (s *Store) Has(id ChunkID) bool {
	_, err := os.Stat(s.path(id))
	return err == nil
}

// ListChunks returns the IDs of all chunks currently stored.
func (s *Store) ListChunks() ([]ChunkID, error) {
	var ids []ChunkID
	err := filepath.WalkDir(s.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		id := ChunkID(d.Name())
		if id.Valid() {
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("chunk: list chunks: %w", err)
	}
	return ids, nil
}

// Count returns the number of chunks currently stored.
func (s *Store) Count() (int, error) {
	ids, err := s.ListChunks()
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}
