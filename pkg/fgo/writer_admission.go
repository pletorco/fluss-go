package fgo

import "context"

// acquireWriterAdmission serializes command submission and the closed flag.
// Unlike a mutex, the channel gate lets queued callers stop waiting without
// interrupting commands or writes that have already been accepted.
func acquireWriterAdmission(ctx context.Context, gate chan struct{}) error {
	select {
	case gate <- struct{}{}:
		// The gate and cancellation may become ready together. Never accept
		// an already canceled call just because the gate case won the select.
		if err := ctx.Err(); err != nil {
			<-gate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
