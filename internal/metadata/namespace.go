package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrNotFound is returned when a path does not exist in the namespace.
var ErrNotFound = errors.New("metadata: path not found")

// ErrAlreadyExists is returned when a path already exists.
var ErrAlreadyExists = errors.New("metadata: path already exists")

// FileInfo is the inode for a file or directory in the namespace.
type FileInfo struct {
	Path      string    `json:"path"`
	IsDir     bool      `json:"is_dir"`
	Size      int64     `json:"size"`
	ChunkIDs  []string  `json:"chunk_ids"`
	Mode      uint32    `json:"mode"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

const nsPrefix = "ns:"

// Namespace is a POSIX-like inode tree persisted in a Store.
type Namespace struct {
	store *Store
}

// NewNamespace wraps store with namespace operations.
func NewNamespace(store *Store) *Namespace {
	return &Namespace{store: store}
}

// CreateFile creates a new file at path. Returns ErrAlreadyExists if the path
// is already occupied.
func (ns *Namespace) CreateFile(info FileInfo) error {
	if _, err := ns.Stat(info.Path); err == nil {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, info.Path)
	}
	now := time.Now()
	info.CreatedAt = now
	info.UpdatedAt = now
	return ns.write(info)
}

// CreateDir creates a directory at path. Returns ErrAlreadyExists if occupied.
func (ns *Namespace) CreateDir(path string) error {
	if _, err := ns.Stat(path); err == nil {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, path)
	}
	now := time.Now()
	return ns.write(FileInfo{
		Path:      path,
		IsDir:     true,
		Mode:      0o755,
		CreatedAt: now,
		UpdatedAt: now,
	})
}

// Stat returns the FileInfo for path. Returns ErrNotFound if absent.
func (ns *Namespace) Stat(path string) (FileInfo, error) {
	val, err := ns.store.Get([]byte(nsPrefix + path))
	if errors.Is(err, ErrKeyNotFound) {
		return FileInfo{}, fmt.Errorf("%w: %s", ErrNotFound, path)
	}
	if err != nil {
		return FileInfo{}, err
	}
	var fi FileInfo
	if err := json.Unmarshal(val, &fi); err != nil {
		return FileInfo{}, fmt.Errorf("metadata: namespace unmarshal %s: %w", path, err)
	}
	return fi, nil
}

// UpdateFile overwrites the metadata for an existing file. Returns ErrNotFound
// if path does not exist.
func (ns *Namespace) UpdateFile(info FileInfo) error {
	if _, err := ns.Stat(info.Path); err != nil {
		return err
	}
	info.UpdatedAt = time.Now()
	return ns.write(info)
}

// DeleteFile removes path from the namespace. Returns ErrNotFound if absent.
func (ns *Namespace) DeleteFile(path string) error {
	if _, err := ns.Stat(path); err != nil {
		return err
	}
	if err := ns.store.Delete([]byte(nsPrefix + path)); err != nil {
		return fmt.Errorf("metadata: namespace delete %s: %w", path, err)
	}
	return nil
}

// ListDir returns all direct children of dir (files and directories).
// Returns ErrNotFound if dir does not exist as a directory.
func (ns *Namespace) ListDir(dir string) ([]FileInfo, error) {
	if dir != "/" {
		fi, err := ns.Stat(dir)
		if err != nil {
			return nil, err
		}
		if !fi.IsDir {
			return nil, fmt.Errorf("metadata: %s is not a directory", dir)
		}
	}

	// Prefix to scan: "ns:" + dir + "/"
	// We want direct children only — no nested slashes after the prefix.
	scanPrefix := nsPrefix + strings.TrimSuffix(dir, "/") + "/"
	prefix := []byte(scanPrefix)

	var results []FileInfo
	err := ns.store.Scan(prefix, func(key, val []byte) error {
		// Only include direct children (rest of the path has no "/").
		rest := string(key)[len(scanPrefix):]
		if strings.Contains(rest, "/") {
			return nil
		}
		var fi FileInfo
		if err := json.Unmarshal(val, &fi); err != nil {
			return fmt.Errorf("metadata: namespace list unmarshal: %w", err)
		}
		results = append(results, fi)
		return nil
	})
	return results, err
}

// Rename moves the file or directory at from to to atomically.
func (ns *Namespace) Rename(from, to string) error {
	fi, err := ns.Stat(from)
	if err != nil {
		return fmt.Errorf("metadata: rename source: %w", err)
	}
	if _, err := ns.Stat(to); err == nil {
		return fmt.Errorf("metadata: rename destination exists: %w", ErrAlreadyExists)
	}

	txn := ns.store.NewTxn()
	defer txn.Discard()

	fi.Path = to
	fi.UpdatedAt = time.Now()
	newVal, err := json.Marshal(fi)
	if err != nil {
		return fmt.Errorf("metadata: rename marshal: %w", err)
	}
	if err := txn.Set([]byte(nsPrefix+to), newVal); err != nil {
		return err
	}
	if err := txn.Delete([]byte(nsPrefix + from)); err != nil {
		return err
	}
	return txn.Commit()
}

// write serialises info and stores it under its path.
func (ns *Namespace) write(info FileInfo) error {
	val, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("metadata: namespace marshal %s: %w", info.Path, err)
	}
	return ns.store.Put([]byte(nsPrefix+info.Path), val)
}
