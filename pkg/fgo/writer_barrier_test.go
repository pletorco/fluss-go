package fgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type barrierWriter interface {
	Flush(context.Context) error
	Close(context.Context) error
}

type barrierRequest struct {
	bucket int32
	result chan error
}

type barrierBackend struct {
	requests chan barrierRequest
	stop     chan struct{}
}

func (b *barrierBackend) attempt(ctx context.Context, bucket int32) error {
	request := barrierRequest{bucket: bucket, result: make(chan error, 1)}
	select {
	case b.requests <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stop:
		return context.Canceled
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stop:
		return context.Canceled
	}
}

type barrierAppendBackend struct {
	*fakeAppendWriterBackend
	*barrierBackend
}

func (b barrierAppendBackend) produce(ctx context.Context, input logProduceRequest) (int64, error) {
	return 0, b.attempt(ctx, input.bucket)
}

type barrierUpsertBackend struct {
	*fakeUpsertWriterBackend
	*barrierBackend
}

func (b barrierUpsertBackend) put(ctx context.Context, input kvPutRequest) (kvPutResult, error) {
	return kvPutResult{}, b.attempt(ctx, input.bucket)
}

type barrierFixture struct {
	writer  barrierWriter
	submit  func(int32) *WriteFuture
	backend *barrierBackend
	ctx     context.Context
}

func newBarrierFixture(t *testing.T, kind string, timeout time.Duration) barrierFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	backend := &barrierBackend{requests: make(chan barrierRequest, 2), stop: make(chan struct{})}
	fixture := barrierFixture{backend: backend, ctx: ctx}
	if kind == "append" {
		table := appendWriterTable()
		table.BucketCount = 2
		writer, err := newAppendWriter(ctx, barrierAppendBackend{logBackend(0, 1), backend}, table,
			WithAppendBatchTimeout(timeout), WithAppendNoKeyAssigner(NoKeyAssignerRoundRobin))
		if err != nil {
			t.Fatal(err)
		}
		fixture.writer = writer
		fixture.submit = func(int32) *WriteFuture { return writer.Append(ctx, Row{int32(1), "value"}) }
	} else {
		table := upsertWriterTable()
		table.BucketCount = 2
		writer, err := newUpsertWriter(ctx, barrierUpsertBackend{kvBackend(0, 1), backend}, table,
			WithUpsertBatchTimeout(timeout))
		if err != nil {
			t.Fatal(err)
		}
		fixture.writer = writer
		keys := make(map[int32]int32)
		for id := int32(0); len(keys) < 2; id++ {
			key, err := EncodePrimaryKey(table.Schema, PrimaryKey{id})
			if err != nil {
				t.Fatal(err)
			}
			bucket, err := flussBucket(key, 2)
			if err != nil {
				t.Fatal(err)
			}
			keys[bucket] = id
		}
		fixture.submit = func(bucket int32) *WriteFuture {
			return writer.Upsert(ctx, Row{keys[bucket], "value", int64(1)})
		}
	}
	t.Cleanup(func() {
		close(backend.stop)
		_ = fixture.writer.Close(context.Background())
	})
	return fixture
}

func (f barrierFixture) request(t *testing.T, bucket int32) barrierRequest {
	t.Helper()
	select {
	case request := <-f.backend.requests:
		if request.bucket != bucket {
			t.Fatalf("request bucket = %d, want %d", request.bucket, bucket)
		}
		return request
	case <-f.ctx.Done():
		t.Fatal("writer did not dispatch request")
		return barrierRequest{}
	}
}

func TestWriterBarriersRetainBackgroundFailure(t *testing.T) {
	for _, kind := range []string{"append", "upsert"} {
		t.Run(kind, func(t *testing.T) {
			f := newBarrierFixture(t, kind, 0)
			cause := &ServerError{Code: 123, Message: "terminal batch failure"}
			failure := fmt.Errorf("request failed: %w", cause)
			future := f.submit(0)
			f.request(t, 0).result <- failure
			if result := future.Await(f.ctx); !errors.Is(result.Err, cause) {
				t.Fatalf("future error = %v", result.Err)
			}
			for range 2 {
				err := f.writer.Flush(f.ctx)
				var server *ServerError
				if !errors.Is(err, cause) || !errors.As(err, &server) || server != cause {
					t.Errorf("Flush lost background cause: %v", err)
				}
			}
			// A successful write on the healthy bucket must not clear the failure.
			healthy := f.submit(1)
			f.request(t, 1).result <- nil
			if result := healthy.Await(f.ctx); result.Err != nil {
				t.Fatalf("healthy bucket failed: %v", result.Err)
			}
			poisoned := f.submit(0).Await(f.ctx)
			if !errors.Is(poisoned.Err, ErrWriterState) || !errors.Is(poisoned.Err, cause) {
				t.Errorf("poisoned bucket lost state/cause: %v", poisoned.Err)
			}
			if err := f.writer.Flush(f.ctx); !errors.Is(err, cause) {
				t.Errorf("Flush after healthy/poisoned writes = %v", err)
			}
			const callers = 8
			results := make(chan error, callers)
			var callersStarted sync.WaitGroup
			callersStarted.Add(callers)
			for range callers {
				go func() {
					callersStarted.Done()
					callersStarted.Wait()
					results <- f.writer.Close(f.ctx)
				}()
			}
			var terminal error
			for index := range callers {
				err := <-results
				if !errors.Is(err, cause) {
					t.Errorf("concurrent Close lost background cause: %v", err)
				}
				if index == 0 {
					terminal = err
				} else if err != terminal {
					t.Errorf("concurrent Close did not retain the same terminal result")
				}
			}
			if err := f.writer.Close(f.ctx); err != terminal {
				t.Errorf("repeated Close = %v", err)
			}
		})
	}
}

func TestWriterFlushRetainsFailureDuringBarrier(t *testing.T) {
	for _, kind := range []string{"append", "upsert"} {
		t.Run(kind, func(t *testing.T) {
			for _, cancelWait := range []bool{false, true} {
				t.Run(fmt.Sprintf("cancel_wait_%t", cancelWait), func(t *testing.T) {
					f := newBarrierFixture(t, kind, time.Hour)
					future := f.submit(0)
					ctx, cancel := context.WithCancel(f.ctx)
					defer cancel()
					flushed := make(chan error, 1)
					go func() { flushed <- f.writer.Flush(ctx) }()
					// The only way this buffered batch can dispatch is the Flush barrier.
					request := f.request(t, 0)
					if cancelWait {
						cancel()
						if err := <-flushed; !errors.Is(err, context.Canceled) {
							t.Fatalf("canceled Flush = %v", err)
						}
					}
					request.result <- errWriterTerminal
					if !cancelWait {
						if err := <-flushed; !errors.Is(err, errWriterTerminal) {
							t.Errorf("Flush during failure = %v", err)
						}
					}
					if result := future.Await(f.ctx); !errors.Is(result.Err, errWriterTerminal) {
						t.Fatalf("future = %#v", result)
					}
					if err := f.writer.Flush(f.ctx); !errors.Is(err, errWriterTerminal) || errors.Is(err, context.Canceled) {
						t.Errorf("eventual Flush = %v", err)
					}
					if err := f.writer.Close(f.ctx); !errors.Is(err, errWriterTerminal) || errors.Is(err, context.Canceled) {
						t.Errorf("eventual Close = %v", err)
					}
				})
			}
		})
	}
}

func TestWriterBarriersRetainEachBucketFailure(t *testing.T) {
	for _, kind := range []string{"append", "upsert"} {
		t.Run(kind, func(t *testing.T) {
			f := newBarrierFixture(t, kind, 0)
			causes := []error{errors.New("bucket zero failed"), errors.New("bucket one failed")}
			first := f.submit(0)
			firstRequest := f.request(t, 0)
			second := f.submit(1)
			requests := []barrierRequest{firstRequest, f.request(t, 1)}
			futures := []*WriteFuture{first, second}
			// Process completions in reverse bucket order; barrier ordering is stable.
			for bucket := len(causes) - 1; bucket >= 0; bucket-- {
				requests[bucket].result <- causes[bucket]
				if result := futures[bucket].Await(f.ctx); !errors.Is(result.Err, causes[bucket]) {
					t.Fatalf("bucket %d error = %v", bucket, result.Err)
				}
			}
			for _, err := range []error{f.writer.Flush(f.ctx), f.writer.Flush(f.ctx), f.writer.Close(f.ctx)} {
				for _, cause := range causes {
					if !errors.Is(err, cause) {
						t.Errorf("barrier lost %v: %v", cause, err)
					}
				}
				if err != nil && err.Error() != "bucket zero failed\nbucket one failed" {
					t.Errorf("unstable barrier error order: %v", err)
				}
			}
		})
	}
}

func TestWriterCloseRetainsFailureBeforeFirstBarrier(t *testing.T) {
	for _, kind := range []string{"append", "upsert"} {
		t.Run(kind, func(t *testing.T) {
			f := newBarrierFixture(t, kind, 0)
			future := f.submit(0)
			f.request(t, 0).result <- errWriterTerminal
			if result := future.Await(f.ctx); !errors.Is(result.Err, errWriterTerminal) {
				t.Fatalf("future = %#v", result)
			}
			for range 2 {
				if err := f.writer.Close(f.ctx); !errors.Is(err, errWriterTerminal) {
					t.Errorf("Close after background failure = %v", err)
				}
			}
		})
	}
}
