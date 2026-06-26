package client

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

// DefaultChunkSize is the size at which files are split into chunks (4 MiB).
const DefaultChunkSize = 4 << 20

// chunk is one piece of a file together with its content-addressed ID.
type chunk struct {
	id   string
	data []byte
}

// hashChunk returns the hex-encoded SHA-256 of data, which is the chunk's ID.
func hashChunk(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// splitChunks reads r fully and splits it into fixed-size chunks. The final
// chunk may be smaller. An empty input yields no chunks.
func splitChunks(r io.Reader, size int) ([]chunk, error) {
	if size <= 0 {
		size = DefaultChunkSize
	}
	var chunks []chunk
	buf := make([]byte, size)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			chunks = append(chunks, chunk{id: hashChunk(data), data: data})
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("client: read input: %w", err)
		}
	}
	return chunks, nil
}

// verifyChunk reports an error if data does not hash to the expected ID,
// catching corruption introduced in transit or storage.
func verifyChunk(expectedID string, data []byte) error {
	if got := hashChunk(data); got != expectedID {
		return fmt.Errorf("client: chunk integrity check failed: want %s, got %s", expectedID, got)
	}
	return nil
}
