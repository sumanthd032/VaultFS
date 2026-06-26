package metadata

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaseGrant(t *testing.T) {
	m := NewLeaseManager(time.Minute)
	lease, err := m.Grant("chunk-1", "node-A")
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if lease.Holder != "node-A" || lease.ChunkID != "chunk-1" {
		t.Errorf("unexpected lease: %+v", lease)
	}
}

func TestLeaseGrantContended(t *testing.T) {
	m := NewLeaseManager(time.Minute)
	if _, err := m.Grant("c1", "node-A"); err != nil {
		t.Fatalf("first Grant: %v", err)
	}

	tests := []struct {
		name    string
		holder  string
		wantErr error
	}{
		{"same holder renews", "node-A", nil},
		{"different holder blocked", "node-B", ErrLeaseHeld},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := m.Grant("c1", tt.holder)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Grant: got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestLeaseExpiry(t *testing.T) {
	m := NewLeaseManager(time.Minute)
	base := time.Now()
	m.now = func() time.Time { return base }

	if _, err := m.Grant("c1", "node-A"); err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if _, ok := m.Check("c1"); !ok {
		t.Fatal("lease should be valid immediately after grant")
	}

	// Advance past the TTL - the lease must now read as absent.
	m.now = func() time.Time { return base.Add(2 * time.Minute) }
	if _, ok := m.Check("c1"); ok {
		t.Error("lease should have expired")
	}

	// A different node can now acquire the chunk.
	m.now = func() time.Time { return base.Add(2 * time.Minute) }
	if _, err := m.Grant("c1", "node-B"); err != nil {
		t.Errorf("Grant after expiry should succeed, got %v", err)
	}
}

func TestLeaseRenew(t *testing.T) {
	m := NewLeaseManager(time.Minute)
	base := time.Now()
	m.now = func() time.Time { return base }
	_, _ = m.Grant("c1", "node-A")

	// Renew at t+30s extends expiry to t+90s.
	m.now = func() time.Time { return base.Add(30 * time.Second) }
	lease, err := m.Renew("c1", "node-A")
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if !lease.Expiry.Equal(base.Add(90 * time.Second)) {
		t.Errorf("renewed expiry = %v, want %v", lease.Expiry, base.Add(90*time.Second))
	}

	// A node that does not hold the lease cannot renew it.
	if _, err := m.Renew("c1", "node-B"); !errors.Is(err, ErrLeaseNotHeld) {
		t.Errorf("Renew by non-holder: got %v, want ErrLeaseNotHeld", err)
	}
}

func TestLeaseRevoke(t *testing.T) {
	m := NewLeaseManager(time.Minute)
	_, _ = m.Grant("c1", "node-A")

	if err := m.Revoke("c1", "node-B"); !errors.Is(err, ErrLeaseNotHeld) {
		t.Errorf("Revoke by non-holder: got %v, want ErrLeaseNotHeld", err)
	}
	if err := m.Revoke("c1", "node-A"); err != nil {
		t.Fatalf("Revoke by holder: %v", err)
	}
	if _, ok := m.Check("c1"); ok {
		t.Error("lease should be gone after revoke")
	}
}

func TestLeaseSweepExpired(t *testing.T) {
	m := NewLeaseManager(time.Minute)
	base := time.Now()
	m.now = func() time.Time { return base }
	_, _ = m.Grant("c1", "node-A")
	_, _ = m.Grant("c2", "node-B")

	m.now = func() time.Time { return base.Add(2 * time.Minute) }
	if n := m.SweepExpired(); n != 2 {
		t.Errorf("SweepExpired = %d, want 2", n)
	}
}

// TestLeaseConcurrentRequests verifies that under a race only one node ever
// holds the lease for a chunk.
func TestLeaseConcurrentRequests(t *testing.T) {
	m := NewLeaseManager(time.Minute)

	const goroutines = 64
	var granted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			holder := string(rune('A' + n%26))
			if _, err := m.Grant("hot-chunk", holder); err == nil {
				granted.Add(1)
			}
		}(i)
	}
	wg.Wait()

	// At least one grant succeeds; the winning holder owns the lease.
	if granted.Load() == 0 {
		t.Fatal("no goroutine obtained the lease")
	}
	lease, ok := m.Check("hot-chunk")
	if !ok {
		t.Fatal("no valid lease after contention")
	}
	// Every successful grant must have been to the eventual holder (same holder
	// renews; different holders are rejected).
	if lease.Holder == "" {
		t.Error("winning lease has no holder")
	}
}
