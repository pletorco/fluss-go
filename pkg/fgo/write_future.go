package fgo

import (
	"context"
	"fmt"
	"sync"
)

// WriteResult is the terminal outcome of one queued mutation.
type WriteResult struct {
	// Bucket is the target bucket, or zero when routing did not complete.
	Bucket int32
	// BaseOffset is the first assigned offset when OffsetKnown is true.
	BaseOffset int64
	// OffsetKnown reports whether the successful response included an offset.
	// A recovered duplicate-sequence acknowledgement can succeed without one.
	OffsetKnown bool
	// Records is the number of records completed by this result.
	Records int
	// Pressure is the successful KV response's normalized storage pressure when PressureKnown is true.
	Pressure float32
	// PressureKnown reports whether Fluss supplied a KV storage-pressure signal.
	PressureKnown bool
	// Err is the terminal mutation error.
	Err error
}

// WriteFuture represents one queued mutation.
// Await may be called concurrently and does not consume the result.
type WriteFuture struct {
	done    chan struct{}
	once    sync.Once
	data    WriteResult
	release func()
}

func newWriteFuture() *WriteFuture { return &WriteFuture{done: make(chan struct{})} }

func (f *WriteFuture) complete(result WriteResult) {
	f.once.Do(func() {
		f.data = result
		if f.release != nil {
			f.release()
		}
		close(f.done)
	})
}

// Await waits for the mutation result or ctx cancellation.
// Cancellation stops waiting but does not cancel a mutation already in flight.
func (f *WriteFuture) Await(ctx context.Context) WriteResult {
	if f == nil {
		return WriteResult{Err: fmt.Errorf("%w: nil write future", ErrInvalidConfig)}
	}
	select {
	case <-f.done:
		return f.data
	case <-ctx.Done():
		return WriteResult{Err: ctx.Err()}
	}
}
