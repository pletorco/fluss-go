package fgo

import (
	"context"
	"fmt"
	"sync"
)

type sharedCall[V any] struct {
	done      chan struct{}
	cancel    context.CancelFunc
	value     V
	err       error
	waiters   int
	completed bool
	claimed   bool
	succeeded bool
	dispose   func(V)
}

// coalescer runs at most one operation for a comparable key. The operation
// context belongs to the shared call, not its first waiter.
type coalescer[K comparable, V any] struct {
	mu       sync.Mutex
	calls    map[K]*sharedCall[V]
	closed   bool
	closeErr error
}

func newCoalescer[K comparable, V any]() *coalescer[K, V] {
	return &coalescer[K, V]{calls: make(map[K]*sharedCall[V])}
}

func (c *coalescer[K, V]) Do(
	ctx context.Context,
	key K,
	work func(context.Context) (V, error),
	dispose func(V),
) (V, error) {
	var zero V
	if ctx == nil {
		return zero, fmt.Errorf("%w: nil coalescer context", ErrInvalidConfig)
	}
	if work == nil {
		return zero, fmt.Errorf("%w: nil coalesced operation", ErrInvalidConfig)
	}
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		return zero, err
	}
	call := c.calls[key]
	if call == nil {
		callCtx, cancel := context.WithCancel(context.Background())
		call = &sharedCall[V]{
			done: make(chan struct{}), cancel: cancel, dispose: dispose,
		}
		c.calls[key] = call
		go c.run(callCtx, key, call, work)
	}
	call.waiters++
	c.mu.Unlock()

	select {
	case <-call.done:
		c.mu.Lock()
		call.claimed = true
		value, err := call.value, call.err
		if err != nil {
			value = zero
		}
		c.releaseLocked(key, call)
		c.mu.Unlock()
		return value, err
	case <-ctx.Done():
		c.release(key, call)
		return zero, ctx.Err()
	}
}

func (c *coalescer[K, V]) run(
	ctx context.Context,
	key K,
	call *sharedCall[V],
	work func(context.Context) (V, error),
) {
	value, err := work(ctx)
	call.cancel()
	c.mu.Lock()
	call.value, call.err, call.completed, call.succeeded = value, err, true, err == nil
	current := c.calls[key] == call
	if c.closed {
		call.err = c.closeErr
	}
	if current {
		delete(c.calls, key)
	}
	close(call.done)
	dispose := !call.claimed && call.succeeded && call.dispose != nil &&
		(call.waiters == 0 || c.closed)
	c.mu.Unlock()
	if dispose {
		call.dispose(value)
	}
}

func (c *coalescer[K, V]) release(key K, call *sharedCall[V]) {
	c.mu.Lock()
	dispose := c.releaseLocked(key, call)
	value := call.value
	c.mu.Unlock()
	if dispose != nil {
		dispose(value)
	}
}

func (c *coalescer[K, V]) releaseLocked(key K, call *sharedCall[V]) func(V) {
	call.waiters--
	if call.waiters != 0 {
		return nil
	}
	if !call.completed {
		if c.calls[key] == call {
			delete(c.calls, key)
		}
		call.cancel()
		return nil
	}
	if !call.claimed && call.succeeded {
		return call.dispose
	}
	return nil
}

func (c *coalescer[K, V]) Close(err error) {
	if err == nil {
		err = ErrClosed
	}
	c.mu.Lock()
	if !c.closed {
		c.closed, c.closeErr = true, err
		for _, call := range c.calls {
			call.cancel()
		}
	}
	c.mu.Unlock()
}
