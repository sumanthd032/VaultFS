// Package metadata provides durable metadata storage for VaultFS, backed by
// BadgerDB. It exposes three logical layers:
//
//   - Store: raw key/value access with transactions.
//   - Namespace: a POSIX-like inode tree (files, directories, rename).
//   - ChunkMap: an in-memory map of chunk ID -> replica node locations.
//   - ChunkVersion: per-chunk vector clocks for write-conflict detection.
package metadata

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// ErrKeyNotFound is returned by Store.Get when the key does not exist.
var ErrKeyNotFound = errors.New("metadata: key not found")

// Store is a transactional key/value store backed by BadgerDB.
// It is safe for concurrent use.
type Store struct {
	db *badger.DB
}

// Open opens (or creates) the BadgerDB store at path.
func Open(path string) (*Store, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // silence BadgerDB internal log; VaultFS uses slog
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("metadata: open store at %s: %w", path, err)
	}
	return &Store{db: db}, nil
}

// Close flushes and closes the store. Callers must not use the Store after this.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("metadata: close store: %w", err)
	}
	return nil
}

// Get returns the value for key. Returns ErrKeyNotFound if absent.
func (s *Store) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("metadata: get %q: %w", key, err)
	}
	return val, nil
}

// Put writes key -> val, overwriting any existing value.
func (s *Store) Put(key, val []byte) error {
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	}); err != nil {
		return fmt.Errorf("metadata: put %q: %w", key, err)
	}
	return nil
}

// Delete removes key. It is not an error if the key is absent.
func (s *Store) Delete(key []byte) error {
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	}); err != nil {
		return fmt.Errorf("metadata: delete %q: %w", key, err)
	}
	return nil
}

// Scan calls fn for every key/value pair whose key starts with prefix.
// Iteration stops if fn returns an error; that error is returned by Scan.
func (s *Store) Scan(prefix []byte, fn func(key, val []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 32
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("metadata: scan value for %q: %w", key, err)
			}
			if err := fn(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

// Txn is a read-write transaction on the Store.
type Txn struct {
	txn *badger.Txn
}

// NewTxn begins a read-write transaction. Call Commit or Discard when done.
func (s *Store) NewTxn() *Txn {
	return &Txn{txn: s.db.NewTransaction(true)}
}

// Get returns the value for key within the transaction.
func (t *Txn) Get(key []byte) ([]byte, error) {
	item, err := t.txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("metadata: txn get %q: %w", key, err)
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, fmt.Errorf("metadata: txn read value %q: %w", key, err)
	}
	return val, nil
}

// Set writes key -> val within the transaction.
func (t *Txn) Set(key, val []byte) error {
	if err := t.txn.Set(key, val); err != nil {
		return fmt.Errorf("metadata: txn set %q: %w", key, err)
	}
	return nil
}

// Delete removes key within the transaction.
func (t *Txn) Delete(key []byte) error {
	if err := t.txn.Delete(key); err != nil {
		return fmt.Errorf("metadata: txn delete %q: %w", key, err)
	}
	return nil
}

// Commit commits the transaction. Returns an error on conflict.
func (t *Txn) Commit() error {
	if err := t.txn.Commit(); err != nil {
		return fmt.Errorf("metadata: txn commit: %w", err)
	}
	return nil
}

// Discard rolls back the transaction.
func (t *Txn) Discard() { t.txn.Discard() }
