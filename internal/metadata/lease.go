package metadata

import (
	"errors"
	"sync"
	"time"
)

// DefaultLeaseTTL is the lifetime of a chunk write lease (GFS uses 60 s).
const DefaultLeaseTTL = 60 * time.Second

// ErrLeaseHeld is returned when a lease is requested for a chunk that already
// has a valid lease held by a different node.
var ErrLeaseHeld = errors.New("metadata: lease already held by another node")

// ErrLeaseNotHeld is returned when renewing or revoking a lease that the caller
// does not currently hold.
var ErrLeaseNotHeld = errors.New("metadata: lease not held by this node")

// Lease grants a single node the exclusive right to coordinate writes to a
// chunk for a bounded period. The master grants the lease to a chunk's primary
// replica; the primary serialises mutations until the lease expires or is renewed.
type Lease struct {
	ChunkID string
	Holder  string // node ID of the primary replica
	Expiry  time.Time
}

// expired reports whether the lease is no longer valid at now.
func (l Lease) expired(now time.Time) bool {
	return !now.Before(l.Expiry)
}

// LeaseManager grants, renews, and revokes chunk write leases with automatic
// time-based expiry. It is safe for concurrent use.
type LeaseManager struct {
	mu     sync.Mutex        // protects leases
	leases map[string]*Lease // chunkID -> active lease

	ttl time.Duration
	now func() time.Time // injectable clock for tests
}

// NewLeaseManager returns a LeaseManager using ttl as the lease lifetime.
// If ttl is non-positive, DefaultLeaseTTL is used.
func NewLeaseManager(ttl time.Duration) *LeaseManager {
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	return &LeaseManager{
		leases: make(map[string]*Lease),
		ttl:    ttl,
		now:    time.Now,
	}
}

// Grant issues a lease on chunkID to holder. If a valid lease is already held by
// a different node, it returns ErrLeaseHeld. Re-granting to the current holder
// renews the lease. Expired leases are silently reclaimed.
func (m *LeaseManager) Grant(chunkID, holder string) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	if existing, ok := m.leases[chunkID]; ok && !existing.expired(now) {
		if existing.Holder != holder {
			return Lease{}, ErrLeaseHeld
		}
	}
	lease := &Lease{
		ChunkID: chunkID,
		Holder:  holder,
		Expiry:  now.Add(m.ttl),
	}
	m.leases[chunkID] = lease
	return *lease, nil
}

// Renew extends an existing lease held by holder. It returns ErrLeaseNotHeld if
// the lease is absent, expired, or held by a different node.
func (m *LeaseManager) Renew(chunkID, holder string) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	existing, ok := m.leases[chunkID]
	if !ok || existing.expired(now) || existing.Holder != holder {
		return Lease{}, ErrLeaseNotHeld
	}
	existing.Expiry = now.Add(m.ttl)
	return *existing, nil
}

// Revoke releases the lease on chunkID held by holder. It returns
// ErrLeaseNotHeld if holder does not hold a valid lease on the chunk.
func (m *LeaseManager) Revoke(chunkID, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.leases[chunkID]
	if !ok || existing.expired(m.now()) || existing.Holder != holder {
		return ErrLeaseNotHeld
	}
	delete(m.leases, chunkID)
	return nil
}

// Check returns the current valid lease for chunkID, and whether one exists.
// An expired lease is reported as absent.
func (m *LeaseManager) Check(chunkID string) (Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.leases[chunkID]
	if !ok || existing.expired(m.now()) {
		return Lease{}, false
	}
	return *existing, true
}

// Count returns the number of currently valid (unexpired) leases.
func (m *LeaseManager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	var n int
	for _, lease := range m.leases {
		if !lease.expired(now) {
			n++
		}
	}
	return n
}

// SweepExpired removes all expired leases and returns how many were reclaimed.
// A master may call this periodically; expiry is also enforced lazily on access.
func (m *LeaseManager) SweepExpired() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	var n int
	for id, lease := range m.leases {
		if lease.expired(now) {
			delete(m.leases, id)
			n++
		}
	}
	return n
}
