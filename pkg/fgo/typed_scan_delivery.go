package fgo

import (
	"context"
	"fmt"
	"sync"
)

var errTypedScanArrow = fmt.Errorf("%w: typed row scanner does not decode Arrow batches", ErrUnsupportedAPI)

// typedScanDelivery owns at most one row result after the underlying scanner
// advances. It serializes codecs with later polls, without holding a lock
// while I/O or the application codec runs.
type typedScanDelivery[R any] struct {
	gate chan struct{}
	mu   sync.Mutex

	active   bool
	pending  R
	retained bool
	failure  error
	complete bool
}

func newTypedScanDelivery[R any]() *typedScanDelivery[R] {
	return &typedScanDelivery[R]{gate: make(chan struct{}, 1)}
}

func pollTypedScan[R, T any](
	ctx, life context.Context,
	state *typedScanDelivery[R],
	read func(context.Context) (R, error),
	decode func(R) (T, error),
	sourceDone func() bool,
) (T, error) {
	var zero T
	if ctx == nil {
		return zero, fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	if err := acquirePoll(ctx, life, state.gate); err != nil {
		return zero, err
	}
	raw, retained, err := state.begin(ctx, life)
	defer state.end()
	if err != nil {
		return zero, err
	}
	if !retained {
		raw, err = read(ctx)
		state.capture(life, raw, err)
		if err != nil {
			return zero, err
		}
	}
	if err := typedPollContextErr(ctx, life); err != nil {
		return zero, err
	}
	out, decodeErr := decode(raw)
	if err := state.commit(ctx, life, decodeErr, sourceDone); err != nil {
		return zero, err
	}
	return out, nil
}

func (d *typedScanDelivery[R]) begin(ctx, life context.Context) (R, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active = true
	err := typedPollContextErr(ctx, life)
	if err == nil {
		err = d.failure
	}
	return d.pending, d.retained, err
}

func (d *typedScanDelivery[R]) capture(life context.Context, raw R, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if life.Err() != nil {
		return
	}
	if err == errTypedScanArrow {
		// A fixed row codec can never consume an Arrow batch. Stop explicitly
		// rather than retaining Arrow resources or skipping those results.
		d.failure = err
	}
	if err == nil {
		d.pending, d.retained = raw, true
	}
}

func typedPollContextErr(ctx, life context.Context) error {
	if life.Err() != nil {
		return ErrClosed
	}
	return ctx.Err()
}

func (d *typedScanDelivery[R]) commit(ctx, life context.Context, decodeErr error, sourceDone func() bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := typedPollContextErr(ctx, life); err != nil {
		return err
	}
	if decodeErr == nil {
		d.clearPending()
		d.complete = sourceDone()
	}
	return decodeErr
}

func (d *typedScanDelivery[R]) end() {
	d.mu.Lock()
	d.active = false
	d.mu.Unlock()
	<-d.gate
}

func (d *typedScanDelivery[R]) done(sourceDone func() bool) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.complete || (!d.active && !d.retained && d.failure == nil && sourceDone())
}

func (d *typedScanDelivery[R]) clearPending() {
	var zero R
	d.pending, d.retained = zero, false
}

func (d *typedScanDelivery[R]) discard() {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Keep Done false if a closed wrapper still had undelivered data.
	if d.retained || d.active {
		d.failure = ErrClosed
	}
	d.clearPending()
}
