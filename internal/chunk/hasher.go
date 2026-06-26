// Package chunk implements the VaultFS data plane: content-addressed,
// disk-backed chunk storage with SHA-256 integrity, pipeline replication to
// secondary chunk servers, orphaned-chunk garbage collection, and periodic
// heartbeat reporting to the master.
//
// Chunks are content-addressed: a chunk's ID is the hex-encoded SHA-256 of its
// bytes. This makes writes idempotent and lets any reader detect corruption by
// re-hashing the data it read back.
package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// ChunkID is the hex-encoded SHA-256 digest of a chunk's contents.
// It is always 64 lowercase hex characters.
type ChunkID string

// ErrChecksumFailed indicates that a chunk's bytes do not hash to its ID,
// meaning the data was corrupted in storage or transit.
var ErrChecksumFailed = errors.New("chunk: SHA-256 mismatch, data corrupted")

// idLen is the length of a valid ChunkID (SHA-256 -> 32 bytes -> 64 hex chars).
const idLen = sha256.Size * 2

// Hash computes the ChunkID for data.
func Hash(data []byte) ChunkID {
	sum := sha256.Sum256(data)
	return ChunkID(hex.EncodeToString(sum[:]))
}

// Verify reports whether data hashes to id. It returns ErrChecksumFailed
// (wrapped with the offending id) when the digest does not match.
func Verify(id ChunkID, data []byte) error {
	got := Hash(data)
	if got != id {
		return fmt.Errorf("%w: want %s, got %s", ErrChecksumFailed, id, got)
	}
	return nil
}

// Valid reports whether id is well-formed: exactly 64 lowercase hex characters.
func (id ChunkID) Valid() bool {
	if len(id) != idLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		isDigit := c >= '0' && c <= '9'
		isLowerHex := c >= 'a' && c <= 'f'
		if !isDigit && !isLowerHex {
			return false
		}
	}
	return true
}

// String returns the chunk ID as a string.
func (id ChunkID) String() string { return string(id) }
