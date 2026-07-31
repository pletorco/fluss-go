package fgo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCoalescerSharesResultAndSeparatesCancellation(t *testing.T) {
	group := newCoalescer[string, *int]()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	work := func(ctx context.Context) (*int, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			value := 7
			return &value, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerResult := make(chan error, 1)
	go func() {
		_, err := group.Do(ownerCtx, "key", work, nil)
		ownerResult <- err
	}()
	<-started
	results := make(chan *int, 4)
	for range 4 {
		go func() {
			value, err := group.Do(context.Background(), "key", work, nil)
			if err != nil {
				t.Errorf("waiter result: %v", err)
				return
			}
			results <- value
		}()
	}
	waitForCoalescerWaiters(t, group, "key", 5)
	cancelOwner()
	if err := <-ownerResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner cancellation = %v", err)
	}
	close(release)
	var first *int
	for range 4 {
		value := <-results
		if first == nil {
			first = value
		}
		if value != first || *value != 7 {
			t.Fatalf("shared result = %p/%d, want %p/7", value, *value, first)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("work calls = %d", calls.Load())
	}
}

func TestCoalescerCancelsWhenAllWaitersLeave(t *testing.T) {
	group := newCoalescer[string, struct{}]()
	canceled := make(chan struct{})
	work := func(ctx context.Context) (struct{}, error) {
		<-ctx.Done()
		close(canceled)
		return struct{}{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := group.Do(ctx, "key", work, nil)
		result <- err
	}()
	waitForCoalescerWaiters(t, group, "key", 1)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter result = %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("shared operation was not canceled")
	}
}

func TestCoalescerCloseRaceAndUnclaimedResultDisposal(t *testing.T) {
	group := newCoalescer[string, *atomic.Bool]()
	workStarted := make(chan struct{})
	workRelease := make(chan struct{})
	resource := &atomic.Bool{}
	work := func(context.Context) (*atomic.Bool, error) {
		close(workStarted)
		<-workRelease
		return resource, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := group.Do(ctx, "key", work, func(value *atomic.Bool) {
			value.Store(true)
		})
		result <- err
	}()
	<-workStarted
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled result = %v", err)
	}
	close(workRelease)
	deadline := time.Now().Add(time.Second)
	for !resource.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !resource.Load() {
		t.Fatal("unclaimed result was not disposed")
	}

	closed := newCoalescer[string, struct{}]()
	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := closed.Do(context.Background(), "key", func(ctx context.Context) (struct{}, error) {
			close(started)
			<-ctx.Done()
			return struct{}{}, ctx.Err()
		}, nil)
		finished <- err
	}()
	<-started
	sentinel := errors.New("closed for test")
	closed.Close(sentinel)
	if err := <-finished; !errors.Is(err, sentinel) {
		t.Fatalf("close-race result = %v", err)
	}
	if _, err := closed.Do(context.Background(), "new", func(context.Context) (struct{}, error) {
		return struct{}{}, nil
	}, nil); !errors.Is(err, sentinel) {
		t.Fatalf("post-close result = %v", err)
	}
}

func TestCoalescerRejectsInvalidCalls(t *testing.T) {
	group := newCoalescer[string, struct{}]()
	if _, err := group.Do(nil, "key", func(context.Context) (struct{}, error) {
		return struct{}{}, nil
	}, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := group.Do(context.Background(), "key", nil, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil work error = %v", err)
	}
}

func waitForCoalescerWaiters[K comparable, V any](
	t *testing.T,
	group *coalescer[K, V],
	key K,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		group.mu.Lock()
		call := group.calls[key]
		waiters := 0
		if call != nil {
			waiters = call.waiters
		}
		group.mu.Unlock()
		if waiters == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("coalescer waiters = %d, want %d", waiters, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCoalescerDifferentKeysRunConcurrently(t *testing.T) {
	group := newCoalescer[string, string]()
	var wait sync.WaitGroup
	wait.Add(2)
	started := make(chan string, 2)
	release := make(chan struct{})
	for _, key := range []string{"a", "b"} {
		go func() {
			defer wait.Done()
			value, err := group.Do(context.Background(), key, func(context.Context) (string, error) {
				started <- key
				<-release
				return key, nil
			}, nil)
			if err != nil || value != key {
				t.Errorf("Do(%q) = %q, %v", key, value, err)
			}
		}()
	}
	<-started
	<-started
	close(release)
	wait.Wait()
}

func TestCoalescerAllCancelAllowsSafeReplacement(t *testing.T) {
	group := newCoalescer[string, int]()
	var calls atomic.Int32
	firstCanceled := make(chan struct{})
	finishFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	finishSecond := make(chan struct{})
	work := func(ctx context.Context) (int, error) {
		if calls.Add(1) == 1 {
			<-ctx.Done()
			close(firstCanceled)
			<-finishFirst
			return 1, nil
		}
		close(secondStarted)
		<-finishSecond
		return 2, nil
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, err := group.Do(firstCtx, "key", work, nil)
		firstResult <- err
	}()
	waitForCoalescerWaiters(t, group, "key", 1)
	cancelFirst()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first result = %v", err)
	}
	<-firstCanceled

	results := make(chan int, 2)
	for range 2 {
		go func() {
			value, err := group.Do(context.Background(), "key", work, nil)
			if err != nil {
				t.Errorf("replacement result: %v", err)
				return
			}
			results <- value
		}()
	}
	<-secondStarted
	waitForCoalescerWaiters(t, group, "key", 2)
	close(finishFirst)
	close(finishSecond)
	if first, second := <-results, <-results; first != 2 || second != 2 {
		t.Fatalf("replacement values = %d, %d", first, second)
	}
	if calls.Load() != 2 {
		t.Fatalf("replacement work calls = %d", calls.Load())
	}
}
