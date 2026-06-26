package chunk

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// ReferenceChecker reports whether a chunk is still referenced by the namespace.
// The master implements this; a chunk server asks before reclaiming disk space.
// Defined on the consumer side, kept to a single method.
type ReferenceChecker interface {
	IsReferenced(id ChunkID) bool
}

// GC reclaims disk space by deleting chunks that are no longer referenced by any
// file. To avoid races with in-flight writes (a chunk written but not yet
// linked into the namespace), an unreferenced chunk is only deleted after it has
// been observed orphaned for at least the grace period.
type GC struct {
	store *Store
	refs  ReferenceChecker
	grace time.Duration

	mu         sync.Mutex            // protects orphanedAt
	orphanedAt map[ChunkID]time.Time // chunk -> first time it was seen orphaned
	now        func() time.Time      // injectable clock for tests
}

// NewGC returns a garbage collector for store. Chunks unreferenced by refs are
// deleted once they have been orphaned for longer than grace.
func NewGC(store *Store, refs ReferenceChecker, grace time.Duration) *GC {
	return &GC{
		store:      store,
		refs:       refs,
		grace:      grace,
		orphanedAt: make(map[ChunkID]time.Time),
		now:        time.Now,
	}
}

// Run executes a GC sweep every interval until ctx is cancelled.
func (g *GC) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := g.Sweep(ctx); err != nil {
				slog.Warn("chunk gc sweep failed", "err", err)
			}
		}
	}
}

// Sweep performs a single garbage-collection pass: it deletes chunks that have
// been orphaned longer than the grace period and returns the number deleted.
func (g *GC) Sweep(ctx context.Context) error {
	ids, err := g.store.ListChunks()
	if err != nil {
		return err
	}

	now := g.now()
	live := make(map[ChunkID]struct{}, len(ids))

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		live[id] = struct{}{}

		if g.refs.IsReferenced(id) {
			// Referenced again (or still): clear any orphan timer.
			g.mu.Lock()
			delete(g.orphanedAt, id)
			g.mu.Unlock()
			continue
		}

		g.mu.Lock()
		first, seen := g.orphanedAt[id]
		if !seen {
			first = now
			g.orphanedAt[id] = now
		}
		expired := now.Sub(first) >= g.grace
		g.mu.Unlock()

		if !expired {
			continue
		}
		if err := g.store.DeleteChunk(ctx, id); err != nil {
			slog.Warn("chunk gc delete failed", "chunk_id", id, "err", err)
			continue
		}
		g.mu.Lock()
		delete(g.orphanedAt, id)
		g.mu.Unlock()
		slog.Info("chunk garbage collected", "chunk_id", id)
	}

	// Forget orphan timers for chunks that no longer exist on disk.
	g.mu.Lock()
	for id := range g.orphanedAt {
		if _, ok := live[id]; !ok {
			delete(g.orphanedAt, id)
		}
	}
	g.mu.Unlock()
	return nil
}
