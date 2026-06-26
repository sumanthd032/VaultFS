package metadata

import "github.com/sumanthd032/vaultfs/internal/clock"

// ChunkVersion tracks the causal history of a chunk using a vector clock.
// Each write to the chunk increments the owning node's counter.
// Conflicts are detected by comparing clocks; concurrent writes produce
// clocks that are neither HappensBefore nor equal.
type ChunkVersion struct {
	ChunkID string
	Clock   clock.VectorClock
}

// NewChunkVersion returns a ChunkVersion with an empty vector clock.
func NewChunkVersion(chunkID string) ChunkVersion {
	return ChunkVersion{
		ChunkID: chunkID,
		Clock:   clock.New(),
	}
}

// Increment records a write event at nodeID, returning the updated version.
func (v ChunkVersion) Increment(nodeID string) ChunkVersion {
	return ChunkVersion{
		ChunkID: v.ChunkID,
		Clock:   v.Clock.Increment(clock.NodeID(nodeID)),
	}
}

// Merge produces a version that reflects all events in both v and other.
// Used when replicating a chunk to a new node.
func (v ChunkVersion) Merge(other ChunkVersion) ChunkVersion {
	return ChunkVersion{
		ChunkID: v.ChunkID,
		Clock:   v.Clock.Merge(other.Clock),
	}
}

// HappensBefore reports whether v causally precedes other (v -> other).
func (v ChunkVersion) HappensBefore(other ChunkVersion) bool {
	return v.Clock.HappensBefore(other.Clock)
}

// Concurrent reports whether v and other represent concurrent (conflicting) writes.
func (v ChunkVersion) Concurrent(other ChunkVersion) bool {
	return v.Clock.Concurrent(other.Clock)
}

// Equal reports whether both versions carry identical vector timestamps.
func (v ChunkVersion) Equal(other ChunkVersion) bool {
	return v.Clock.Equal(other.Clock)
}
