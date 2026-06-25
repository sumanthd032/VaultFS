// Package wal implements a write-ahead log for durable, ordered entry storage.
//
// Entries are written to disk and fsynced before the caller receives an ack,
// ensuring no committed entry is lost on sudden process termination.
//
// On-disk format per entry:
//
//	[8 bytes: payload length][4 bytes: CRC32 of payload][payload bytes]
//
// where payload = [8 bytes: entry index][N bytes: entry data].
//
// On recovery, entries are replayed until the first truncated or corrupt record.
// Any partial entry at the tail is discarded and the file truncated to the last
// known-good boundary, making crash recovery both safe and automatic.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// ErrChecksumMismatch is returned when a decoded entry's CRC32 does not match
// the stored checksum, indicating data corruption.
var ErrChecksumMismatch = errors.New("wal: CRC32 checksum mismatch")

const (
	lenFieldSize = 8  // bytes for the payload-length prefix
	crcFieldSize = 4  // bytes for the CRC32 field
	idxFieldSize = 8  // bytes for the entry index inside the payload
	headerSize   = lenFieldSize + crcFieldSize
	// maxPayloadSize guards against allocating huge buffers on corrupted length fields.
	maxPayloadSize = 128 * 1024 * 1024 // 128 MiB
)

// Entry is a single WAL record consisting of a monotonically increasing index
// and an opaque data blob.
type Entry struct {
	Index uint64
	Data  []byte
}

// Encode serialises the entry and writes it to w.
// The format is: [8:payloadLen][4:CRC32][8:index][N:data].
func (e Entry) Encode(w io.Writer) error {
	payload := make([]byte, idxFieldSize+len(e.Data))
	binary.LittleEndian.PutUint64(payload[:idxFieldSize], e.Index)
	copy(payload[idxFieldSize:], e.Data)

	checksum := crc32.ChecksumIEEE(payload)

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint64(header[:lenFieldSize], uint64(len(payload)))
	binary.LittleEndian.PutUint32(header[lenFieldSize:], checksum)

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("wal: encode header: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("wal: encode payload: %w", err)
	}
	return nil
}

// Decode reads one entry from r.
//
// Returns io.EOF only when no bytes remain at the start of a record (clean end
// of log). Returns io.ErrUnexpectedEOF (wrapped) when a record is partially
// present (crash-truncated tail). Returns ErrChecksumMismatch for bit-flipped data.
func Decode(r io.Reader) (Entry, error) {
	header := make([]byte, headerSize)
	_, err := io.ReadFull(r, header)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Entry{}, io.EOF
		}
		return Entry{}, fmt.Errorf("wal: decode header: %w", io.ErrUnexpectedEOF)
	}

	payloadLen := binary.LittleEndian.Uint64(header[:lenFieldSize])
	storedCRC := binary.LittleEndian.Uint32(header[lenFieldSize:])

	if payloadLen > maxPayloadSize {
		return Entry{}, fmt.Errorf("wal: decode: payload length %d exceeds maximum", payloadLen)
	}
	if payloadLen < idxFieldSize {
		return Entry{}, fmt.Errorf("wal: decode: payload length %d too small to contain index", payloadLen)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Entry{}, fmt.Errorf("wal: decode payload: %w", io.ErrUnexpectedEOF)
	}

	if crc32.ChecksumIEEE(payload) != storedCRC {
		return Entry{}, ErrChecksumMismatch
	}

	return Entry{
		Index: binary.LittleEndian.Uint64(payload[:idxFieldSize]),
		Data:  append([]byte(nil), payload[idxFieldSize:]...),
	}, nil
}

// encodedSize returns the total on-disk byte count for an entry with dataLen bytes.
func encodedSize(dataLen int) int64 {
	return int64(headerSize + idxFieldSize + dataLen)
}
