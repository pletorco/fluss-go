package fgo

import "context"

// acquirePoll serializes polls while allowing queued calls to stop waiting.
// The scanner checks its state and caller context after taking the slot.
func acquirePoll(ctx, life context.Context, gate chan struct{}) error {
	select {
	case gate <- struct{}{}:
		return nil
	default:
	}
	select {
	case gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-life.Done():
		return ErrClosed
	}
}
