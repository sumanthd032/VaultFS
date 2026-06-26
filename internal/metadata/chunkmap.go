package metadata

import "sync"

// Location identifies a chunk replica on a specific node.
type Location struct {
	NodeID  string
	Address string
}

// ChunkMap is an in-memory map of chunk ID → replica locations.
// It is safe for concurrent use.
type ChunkMap struct {
	mu        sync.RWMutex       // protects locations
	locations map[string][]Location
}

// NewChunkMap returns an empty ChunkMap.
func NewChunkMap() *ChunkMap {
	return &ChunkMap{
		locations: make(map[string][]Location),
	}
}

// AddLocation records loc as a replica site for chunkID.
// If a location with the same NodeID already exists, it is updated in place.
func (cm *ChunkMap) AddLocation(chunkID string, loc Location) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	locs := cm.locations[chunkID]
	for i, l := range locs {
		if l.NodeID == loc.NodeID {
			locs[i] = loc
			cm.locations[chunkID] = locs
			return
		}
	}
	cm.locations[chunkID] = append(locs, loc)
}

// RemoveLocation removes the replica on nodeID from chunkID.
// If no replica exists on nodeID, this is a no-op.
func (cm *ChunkMap) RemoveLocation(chunkID, nodeID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	locs := cm.locations[chunkID]
	out := locs[:0]
	for _, l := range locs {
		if l.NodeID != nodeID {
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		delete(cm.locations, chunkID)
	} else {
		cm.locations[chunkID] = out
	}
}

// GetLocations returns a copy of all known replica locations for chunkID.
// Returns nil if the chunk is unknown.
func (cm *ChunkMap) GetLocations(chunkID string) []Location {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	locs := cm.locations[chunkID]
	if len(locs) == 0 {
		return nil
	}
	result := make([]Location, len(locs))
	copy(result, locs)
	return result
}

// RemoveNode removes all chunk replicas that were on nodeID.
// This is called when a node is declared dead.
func (cm *ChunkMap) RemoveNode(nodeID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for chunkID, locs := range cm.locations {
		out := locs[:0]
		for _, l := range locs {
			if l.NodeID != nodeID {
				out = append(out, l)
			}
		}
		if len(out) == 0 {
			delete(cm.locations, chunkID)
		} else {
			cm.locations[chunkID] = out
		}
	}
}

// ChunkCount returns the number of distinct chunks tracked.
func (cm *ChunkMap) ChunkCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.locations)
}

// UnderReplicated returns, for every chunk with fewer than factor replicas, a
// copy of its current locations keyed by chunk ID. Chunks with zero replicas are
// not tracked by the map (their entries are removed when the last replica is
// evicted), so every returned chunk has at least one live source to copy from.
func (cm *ChunkMap) UnderReplicated(factor int) map[string][]Location {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	out := make(map[string][]Location)
	for id, locs := range cm.locations {
		if len(locs) < factor {
			cp := make([]Location, len(locs))
			copy(cp, locs)
			out[id] = cp
		}
	}
	return out
}
