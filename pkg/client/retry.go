package client

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryPolicy controls exponential backoff for transient RPC failures.
type retryPolicy struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

// defaultRetryPolicy is a sensible default: up to 4 attempts with backoff
// growing from 50ms to a 1s cap.
func defaultRetryPolicy() retryPolicy {
	return retryPolicy{
		maxAttempts: 4,
		baseDelay:   50 * time.Millisecond,
		maxDelay:    time.Second,
	}
}

// isTransient reports whether err is worth retrying. Unavailable (server down,
// connection refused) and DeadlineExceeded are the retryable gRPC codes.
func isTransient(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
}

// do runs op with exponential backoff, retrying only on transient errors and
// honouring context cancellation between attempts. The last error is returned.
func (rp retryPolicy) do(ctx context.Context, op func() error) error {
	delay := rp.baseDelay
	var err error
	for attempt := 0; attempt < rp.maxAttempts; attempt++ {
		if err = op(); err == nil {
			return nil
		}
		if !isTransient(err) {
			return err
		}
		if attempt == rp.maxAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay *= 2; delay > rp.maxDelay {
			delay = rp.maxDelay
		}
	}
	return err
}
