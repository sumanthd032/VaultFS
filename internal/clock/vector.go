package clock

// NodeID is the unique identifier for a node in the VaultFS cluster.
// It is used as the key in a VectorClock.
type NodeID string

// VectorClock maps each node to its logical timestamp.
//
// A VectorClock is value-like and immutable: all operations return a new copy,
// so callers can freely pass VectorClock values between goroutines without
// synchronisation.
//
// The zero value is a valid, empty clock (equivalent to New()).
type VectorClock map[NodeID]uint64

// New returns an empty VectorClock ready for use.
func New() VectorClock {
	return make(VectorClock)
}

// Increment returns a new VectorClock with id's counter incremented by one.
// Use this to record a local event at node id.
func (vc VectorClock) Increment(id NodeID) VectorClock {
	next := vc.clone()
	next[id]++
	return next
}

// Merge returns a new VectorClock that is the component-wise maximum of vc
// and other. Use this when receiving a message: merge the local clock with
// the sender's clock, then Increment the local node.
func (vc VectorClock) Merge(other VectorClock) VectorClock {
	result := vc.clone()
	for id, val := range other {
		if val > result[id] {
			result[id] = val
		}
	}
	return result
}

// HappensBefore reports whether vc causally precedes other (vc -> other).
//
// vc -> other iff:
//   - vc[i] <= other[i] for all nodes i, AND
//   - vc[j] < other[j] for at least one node j.
//
// Missing keys are treated as 0.
func (vc VectorClock) HappensBefore(other VectorClock) bool {
	atLeastOneStrict := false

	for id, val := range vc {
		if val > other[id] {
			return false
		}
		if val < other[id] {
			atLeastOneStrict = true
		}
	}
	// Any counter that exists only in other must be > 0 for strict ordering.
	for id, val := range other {
		if _, ok := vc[id]; !ok && val > 0 {
			atLeastOneStrict = true
		}
	}
	return atLeastOneStrict
}

// Concurrent reports whether vc and other are causally unrelated - neither
// happens before the other and they are not equal.
func (vc VectorClock) Concurrent(other VectorClock) bool {
	return !vc.HappensBefore(other) && !other.HappensBefore(vc) && !vc.Equal(other)
}

// Equal reports whether vc and other represent identical vector timestamps.
func (vc VectorClock) Equal(other VectorClock) bool {
	allIDs := make(map[NodeID]struct{}, len(vc)+len(other))
	for id := range vc {
		allIDs[id] = struct{}{}
	}
	for id := range other {
		allIDs[id] = struct{}{}
	}
	for id := range allIDs {
		if vc[id] != other[id] {
			return false
		}
	}
	return true
}

// clone returns a deep copy of vc.
func (vc VectorClock) clone() VectorClock {
	c := make(VectorClock, len(vc))
	for k, v := range vc {
		c[k] = v
	}
	return c
}
