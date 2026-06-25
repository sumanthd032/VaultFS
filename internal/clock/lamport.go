// Package clock provides distributed clock primitives for causal ordering of
// events across nodes in the VaultFS cluster.
//
// Two implementations are provided:
//   - LamportClock: a scalar logical clock that establishes a total order
//     consistent with causality.
//   - VectorClock: a per-node counter map that expresses partial causal order
//     and can detect concurrent events that a Lamport clock cannot.
//
// Both are safe for use by multiple goroutines.
package clock

import "sync/atomic"

// LamportClock is a scalar logical clock that advances monotonically.
//
// Rules (Lamport 1978):
//   - On a local event: Tick() increments the counter.
//   - On message receive: Update(received) sets counter to max(local, received)+1.
//
// All methods are safe for concurrent use via atomic operations.
type LamportClock struct {
	counter atomic.Uint64
}

// Tick increments the clock by one (local event) and returns the new value.
func (c *LamportClock) Tick() uint64 {
	return c.counter.Add(1)
}

// Update advances the clock to be greater than received (message-receive rule).
// It sets the clock to max(current, received) + 1 and returns the new value.
func (c *LamportClock) Update(received uint64) uint64 {
	for {
		cur := c.counter.Load()
		var next uint64
		if received >= cur {
			next = received + 1
		} else {
			next = cur + 1
		}
		if c.counter.CompareAndSwap(cur, next) {
			return next
		}
	}
}

// Now returns the current clock value without advancing it.
func (c *LamportClock) Now() uint64 {
	return c.counter.Load()
}
