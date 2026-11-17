package fgo

import (
	"context"
	"fmt"
)

type writerAdmission struct {
	gate   chan struct{}
	closed bool // Guarded by gate.
}

func newWriterAdmission() writerAdmission {
	return writerAdmission{gate: make(chan struct{}, 1)}
}

// acquire serializes command submission and the closed flag.
// Unlike a mutex, the channel gate lets queued callers stop waiting without
// interrupting commands or writes that have already been accepted.
func (a *writerAdmission) acquire(ctx context.Context) error {
	select {
	case a.gate <- struct{}{}:
		// The gate and cancellation may become ready together. Never accept
		// an already canceled call just because the gate case won the select.
		if err := ctx.Err(); err != nil {
			a.release()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *writerAdmission) release() { <-a.gate }

// beginCommand leaves admission held only for a live caller on an open writer.
func (a *writerAdmission) beginCommand(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	if err := a.acquire(ctx); err != nil {
		return err
	}
	if a.closed {
		a.release()
		return ErrClosed
	}
	return nil
}

// beginSubmission acquires a buffer slot and command admission. On failure the
// caller completes future to return any acquired slot; on success it must release
// admission after submitting the command or abandoning the queue wait.
func (a *writerAdmission) beginSubmission(ctx context.Context, slots chan struct{}, done <-chan struct{}, future *WriteFuture) error {
	select {
	case slots <- struct{}{}:
		future.release = func() { <-slots }
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return ErrClosed
	}
	return a.beginCommand(ctx)
}

// beginClose either returns the published terminal result, waits for an existing
// Close, or leaves admission held for the caller to submit the first close command.
// terminal is scheduler-owned and is read only after receiving from done.
func (a *writerAdmission) beginClose(ctx context.Context, done <-chan struct{}, terminal *error) (bool, error) {
	if ctx == nil {
		return false, fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	select {
	case <-done:
		return false, *terminal
	default:
	}
	if err := a.acquire(ctx); err != nil {
		return false, err
	}
	if a.closed {
		a.release()
		select {
		case <-done:
			return false, *terminal
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	a.closed = true
	return true, nil
}
