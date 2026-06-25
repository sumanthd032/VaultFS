package clock_test

import (
	"sync"
	"testing"

	"github.com/sumanthd032/vaultfs/internal/clock"
)

// ── LamportClock ─────────────────────────────────────────────────────────────

// TestLamportClock_Tick verifies that Tick advances the clock monotonically
// and returns the new value after each increment.
func TestLamportClock_Tick(t *testing.T) {
	tests := []struct {
		name   string
		ticks  int
		wantFn func(uint64) bool
	}{
		{"single tick returns 1", 1, func(v uint64) bool { return v == 1 }},
		{"five ticks returns 5", 5, func(v uint64) bool { return v == 5 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c clock.LamportClock
			var last uint64
			for range tt.ticks {
				last = c.Tick()
			}
			if !tt.wantFn(last) {
				t.Errorf("after %d ticks got %d", tt.ticks, last)
			}
		})
	}
}

// TestLamportClock_Monotonic verifies that successive Tick values are strictly
// increasing regardless of interleaving.
func TestLamportClock_Monotonic(t *testing.T) {
	var c clock.LamportClock
	prev := c.Now()
	for range 1000 {
		next := c.Tick()
		if next <= prev {
			t.Fatalf("clock went backwards: %d → %d", prev, next)
		}
		prev = next
	}
}

// TestLamportClock_Update_HigherReceived verifies that when the received
// timestamp exceeds the local clock, the local clock jumps to received+1.
func TestLamportClock_Update_HigherReceived(t *testing.T) {
	tests := []struct {
		name     string
		local    uint64 // ticks before Update
		received uint64
		wantMin  uint64
	}{
		{"received much higher", 1, 100, 101},
		{"received equal", 5, 5, 6},
		{"received one above", 3, 4, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c clock.LamportClock
			for range tt.local {
				c.Tick()
			}
			got := c.Update(tt.received)
			if got < tt.wantMin {
				t.Errorf("Update(%d) = %d, want ≥ %d (local was %d)",
					tt.received, got, tt.wantMin, tt.local)
			}
		})
	}
}

// TestLamportClock_Update_LowerReceived verifies that when the received
// timestamp is lower than the local clock, the local clock still advances.
func TestLamportClock_Update_LowerReceived(t *testing.T) {
	var c clock.LamportClock
	for range 10 {
		c.Tick()
	}
	localBefore := c.Now()
	got := c.Update(3) // received < local
	if got <= localBefore {
		t.Errorf("Update with lower received: got %d, want > %d", got, localBefore)
	}
}

// TestLamportClock_Now verifies that Now returns the current value without
// advancing the clock.
func TestLamportClock_Now(t *testing.T) {
	var c clock.LamportClock
	c.Tick()
	c.Tick()
	before := c.Now()
	after := c.Now()
	if before != after {
		t.Errorf("Now changed clock: %d → %d", before, after)
	}
	if before != 2 {
		t.Errorf("Now = %d, want 2", before)
	}
}

// TestLamportClock_Concurrent verifies there are no data races when multiple
// goroutines call Tick and Update concurrently.
func TestLamportClock_Concurrent(t *testing.T) {
	var c clock.LamportClock
	var wg sync.WaitGroup
	const n = 100

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Tick()
		}()
	}
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Update(50)
		}()
	}
	wg.Wait()

	if got := c.Now(); got == 0 {
		t.Error("clock should not be 0 after concurrent operations")
	}
}

// ── VectorClock ───────────────────────────────────────────────────────────────

// TestVectorClock_Increment verifies that Increment updates only the specified
// node and returns a new copy without modifying the original.
func TestVectorClock_Increment(t *testing.T) {
	tests := []struct {
		name      string
		start     clock.VectorClock
		id        clock.NodeID
		wantValue uint64
	}{
		{"increment from empty", clock.New(), "n1", 1},
		{"increment existing", clock.VectorClock{"n1": 3}, "n1", 4},
		{"increment different node", clock.VectorClock{"n1": 5}, "n2", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := tt.start.Increment(clock.NodeID("placeholder")) // ensure clone
			_ = original
			next := tt.start.Increment(tt.id)
			if next[tt.id] != tt.wantValue {
				t.Errorf("after Increment(%q) got %d, want %d", tt.id, next[tt.id], tt.wantValue)
			}
			// Original must be unchanged.
			if tt.start[tt.id] == next[tt.id] && tt.wantValue != tt.start[tt.id] {
				t.Error("Increment mutated the original clock")
			}
		})
	}
}

// TestVectorClock_IncrementImmutability verifies the copy semantics of Increment.
func TestVectorClock_IncrementImmutability(t *testing.T) {
	a := clock.VectorClock{"n1": 2, "n2": 1}
	b := a.Increment("n1")
	if a["n1"] != 2 {
		t.Errorf("Increment mutated original: a[n1] = %d, want 2", a["n1"])
	}
	if b["n1"] != 3 {
		t.Errorf("b[n1] = %d, want 3", b["n1"])
	}
}

// TestVectorClock_Merge verifies component-wise maximum semantics.
func TestVectorClock_Merge(t *testing.T) {
	tests := []struct {
		name  string
		a, b  clock.VectorClock
		wantA map[clock.NodeID]uint64
	}{
		{
			name:  "disjoint nodes",
			a:     clock.VectorClock{"n1": 3},
			b:     clock.VectorClock{"n2": 5},
			wantA: map[clock.NodeID]uint64{"n1": 3, "n2": 5},
		},
		{
			name:  "overlapping nodes b dominates",
			a:     clock.VectorClock{"n1": 1, "n2": 2},
			b:     clock.VectorClock{"n1": 3, "n2": 1},
			wantA: map[clock.NodeID]uint64{"n1": 3, "n2": 2},
		},
		{
			name:  "merge with empty",
			a:     clock.VectorClock{"n1": 5},
			b:     clock.New(),
			wantA: map[clock.NodeID]uint64{"n1": 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := tt.a.Merge(tt.b)
			for id, want := range tt.wantA {
				if got := merged[id]; got != want {
					t.Errorf("merged[%q] = %d, want %d", id, got, want)
				}
			}
		})
	}
}

// TestVectorClock_HappensBefore verifies the strict causal ordering relation.
func TestVectorClock_HappensBefore(t *testing.T) {
	tests := []struct {
		name string
		a, b clock.VectorClock
		want bool
	}{
		{
			name: "a strictly before b",
			a:    clock.VectorClock{"n1": 1, "n2": 0},
			b:    clock.VectorClock{"n1": 2, "n2": 1},
			want: true,
		},
		{
			name: "equal clocks not before",
			a:    clock.VectorClock{"n1": 2},
			b:    clock.VectorClock{"n1": 2},
			want: false,
		},
		{
			name: "concurrent not before",
			a:    clock.VectorClock{"n1": 2, "n2": 0},
			b:    clock.VectorClock{"n1": 0, "n2": 2},
			want: false,
		},
		{
			name: "b before a returns false",
			a:    clock.VectorClock{"n1": 5},
			b:    clock.VectorClock{"n1": 1},
			want: false,
		},
		{
			name: "single node increment",
			a:    clock.VectorClock{"n1": 3},
			b:    clock.VectorClock{"n1": 4},
			want: true,
		},
		{
			name: "key only in b counts as strict",
			a:    clock.New(),
			b:    clock.VectorClock{"n1": 1},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.HappensBefore(tt.b)
			if got != tt.want {
				t.Errorf("HappensBefore = %v, want %v (a=%v, b=%v)", got, tt.want, tt.a, tt.b)
			}
		})
	}
}

// TestVectorClock_Concurrent verifies that unrelated events are identified as
// concurrent and causally ordered events are not.
func TestVectorClock_Concurrent(t *testing.T) {
	tests := []struct {
		name string
		a, b clock.VectorClock
		want bool
	}{
		{
			name: "truly concurrent",
			a:    clock.VectorClock{"n1": 2, "n2": 0},
			b:    clock.VectorClock{"n1": 0, "n2": 2},
			want: true,
		},
		{
			name: "a before b — not concurrent",
			a:    clock.VectorClock{"n1": 1},
			b:    clock.VectorClock{"n1": 2},
			want: false,
		},
		{
			name: "equal — not concurrent",
			a:    clock.VectorClock{"n1": 3},
			b:    clock.VectorClock{"n1": 3},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Concurrent(tt.b)
			if got != tt.want {
				t.Errorf("Concurrent = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestVectorClock_Equal verifies equality comparison across symmetric and
// asymmetric cases.
func TestVectorClock_Equal(t *testing.T) {
	tests := []struct {
		name string
		a, b clock.VectorClock
		want bool
	}{
		{"both empty", clock.New(), clock.New(), true},
		{"same entries", clock.VectorClock{"n1": 3, "n2": 1}, clock.VectorClock{"n1": 3, "n2": 1}, true},
		{"different values", clock.VectorClock{"n1": 3}, clock.VectorClock{"n1": 4}, false},
		{"different keys", clock.VectorClock{"n1": 1}, clock.VectorClock{"n2": 1}, false},
		{"missing key treated as zero", clock.VectorClock{"n1": 0}, clock.New(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Equal(tt.b); got != tt.want {
				t.Errorf("Equal = %v, want %v", got, tt.want)
			}
			// Equal must be symmetric.
			if got := tt.b.Equal(tt.a); got != tt.want {
				t.Errorf("Equal (symmetric) = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestVectorClock_HappensBefore_Transitivity verifies that happens-before is
// transitive: if a→b and b→c then a→c.
func TestVectorClock_HappensBefore_Transitivity(t *testing.T) {
	a := clock.VectorClock{"n1": 1, "n2": 0}
	b := clock.VectorClock{"n1": 2, "n2": 1}
	c := clock.VectorClock{"n1": 3, "n2": 2}

	if !a.HappensBefore(b) {
		t.Error("a should happen before b")
	}
	if !b.HappensBefore(c) {
		t.Error("b should happen before c")
	}
	if !a.HappensBefore(c) {
		t.Error("a → b → c but a does not happen before c (transitivity violated)")
	}
}
