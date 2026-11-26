package fgo

import "context"

// contextForLifetime keeps the caller's deadline and also cancels when the
// owning resource closes. Cancel it at the end of the operation to unregister
// the lifetime callback.
func contextForLifetime(caller, life context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(caller)
	stop := context.AfterFunc(life, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}
