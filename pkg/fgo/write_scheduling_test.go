package fgo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type schedulingBackendControl struct {
	block   <-chan struct{}
	started chan struct{}
	once    sync.Once
	err     error
}

type schedulingLogBackend struct {
	mu           sync.Mutex
	locations    map[int32]Node
	nextWriterID int64
	controls     map[int64]*schedulingBackendControl
	delay        time.Duration
	err          error
	calls        atomic.Int64
}

func newSchedulingLogBackend(bucketCount int) *schedulingLogBackend {
	locations := make(map[int32]Node, bucketCount)
	for bucket := range bucketCount {
		locations[int32(bucket)] = Node{
			ID: int32(bucket + 1), Address: "shared-tablet", Role: TabletServer,
		}
	}
	return &schedulingLogBackend{
		locations: locations,
		controls:  make(map[int64]*schedulingBackendControl),
	}
}

func (b *schedulingLogBackend) metadata(
	context.Context,
	PhysicalTablePath,
) (int64, map[int32]Node, error) {
	return logWriterTable().ID, b.locations, nil
}

func (b *schedulingLogBackend) initWriter(
	context.Context,
	PhysicalTablePath,
	int32,
) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextWriterID++
	return b.nextWriterID, nil
}

func (b *schedulingLogBackend) produce(
	ctx context.Context,
	request logProduceRequest,
) (int64, error) {
	b.calls.Add(1)
	control, err := b.controlFor(request.records)
	if err != nil {
		return 0, err
	}
	if control != nil && control.started != nil {
		control.once.Do(func() { close(control.started) })
	}
	if control != nil && control.block != nil {
		select {
		case <-control.block:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	delay := b.delay
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if control != nil && control.err != nil {
		return 0, control.err
	}
	if b.err != nil {
		return 0, b.err
	}
	return b.calls.Load(), nil
}

func (b *schedulingLogBackend) controlFor(records []byte) (*schedulingBackendControl, error) {
	b.mu.Lock()
	hasControls := len(b.controls) != 0
	b.mu.Unlock()
	if !hasControls {
		return nil, nil
	}
	batch, err := DecodeLogBatchRows(logWriterTable().Schema, records, true)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.controls[batch.WriterID], nil
}

func (b *schedulingLogBackend) setControl(writerID int64, control *schedulingBackendControl) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.controls[writerID] = control
}

type schedulingKVBackend struct {
	schedule *schedulingLogBackend
}

func (b schedulingKVBackend) metadata(
	context.Context,
	PhysicalTablePath,
) (int64, map[int32]Node, error) {
	return kvWriterTable().ID, b.schedule.locations, nil
}

func (b schedulingKVBackend) initWriter(
	ctx context.Context,
	path PhysicalTablePath,
	bucket int32,
) (int64, error) {
	return b.schedule.initWriter(ctx, path, bucket)
}

func (b schedulingKVBackend) put(ctx context.Context, request kvPutRequest) (int64, error) {
	b.schedule.calls.Add(1)
	batch, err := DecodeKVBatch(request.records)
	if err != nil {
		return 0, err
	}
	b.schedule.mu.Lock()
	control := b.schedule.controls[batch.WriterID]
	b.schedule.mu.Unlock()
	if control != nil && control.started != nil {
		control.once.Do(func() { close(control.started) })
	}
	if control != nil && control.block != nil {
		select {
		case <-control.block:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if control != nil && control.err != nil {
		return 0, control.err
	}
	return b.schedule.calls.Load(), nil
}

func newSchedulingLogWriter(
	t testing.TB,
	backend logWriterBackend,
	bucketCount int,
	options ...LogWriterOption,
) *LogWriter {
	t.Helper()
	table := logWriterTable()
	table.BucketCount = bucketCount
	writer, err := newLogWriter(context.Background(), backend, table, options...)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func TestWriterSchedulingIsolatesSaturationAndShutdown(t *testing.T) {
	backend := newSchedulingLogBackend(1)
	release := make(chan struct{})
	started := make(chan struct{})
	backend.setControl(1, &schedulingBackendControl{block: release, started: started})

	slow := newSchedulingLogWriter(
		t, backend, 1, WithLogLinger(0), WithLogBuffer(1),
	)
	fast := newSchedulingLogWriter(t, backend, 1, WithLogLinger(0))

	first := slow.Append(context.Background(), Row{int32(1), "slow"})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow writer did not reach backend")
	}
	second := slow.Append(context.Background(), Row{int32(2), "queued"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	saturated := slow.Append(ctx, Row{int32(3), "rejected"})
	if result := saturated.Await(context.Background()); !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf("saturated append = %#v", result)
	}

	fastCtx, fastCancel := context.WithTimeout(context.Background(), time.Second)
	defer fastCancel()
	if result := fast.Append(fastCtx, Row{int32(4), "fast"}).Await(fastCtx); result.Err != nil {
		t.Fatalf("fast writer was blocked by slow writer: %v", result.Err)
	}
	if err := fast.Close(fastCtx); err != nil {
		t.Fatalf("fast Close() = %v", err)
	}
	select {
	case <-fast.done:
	case <-time.After(time.Second):
		t.Fatal("fast writer goroutine did not stop")
	}

	close(release)
	for index, future := range []*WriteFuture{first, second} {
		if result := future.Await(context.Background()); result.Err != nil {
			t.Fatalf("slow result %d = %#v", index, result)
		}
	}
	if err := slow.Close(context.Background()); err != nil {
		t.Fatalf("slow Close() = %v", err)
	}
	select {
	case <-slow.done:
	case <-time.After(time.Second):
		t.Fatal("slow writer goroutine did not stop")
	}
}

func TestWriterSchedulingIsolatesAmbiguousFailure(t *testing.T) {
	backend := newSchedulingLogBackend(1)
	failure := errors.New("ambiguous server failure")
	backend.setControl(1, &schedulingBackendControl{err: failure})
	failing := newSchedulingLogWriter(t, backend, 1, WithLogLinger(0))
	healthy := newSchedulingLogWriter(t, backend, 1, WithLogLinger(0))

	if result := failing.Append(
		context.Background(), Row{int32(1), "failing"},
	).Await(context.Background()); !errors.Is(result.Err, failure) {
		t.Fatalf("failing writer result = %#v", result)
	}
	if result := failing.Append(
		context.Background(), Row{int32(2), "poisoned"},
	).Await(context.Background()); !errors.Is(result.Err, ErrWriterState) {
		t.Fatalf("poisoned writer result = %#v", result)
	}
	if result := healthy.Append(
		context.Background(), Row{int32(3), "healthy"},
	).Await(context.Background()); result.Err != nil {
		t.Fatalf("healthy writer inherited failure: %v", result.Err)
	}

	if err := failing.Close(context.Background()); err != nil {
		t.Fatalf("failing Close() = %v", err)
	}
	if err := healthy.Close(context.Background()); err != nil {
		t.Fatalf("healthy Close() = %v", err)
	}
}

func TestKVWriterSchedulingIsolatesSlowWriter(t *testing.T) {
	schedule := newSchedulingLogBackend(1)
	release := make(chan struct{})
	started := make(chan struct{})
	schedule.setControl(1, &schedulingBackendControl{block: release, started: started})
	backend := schedulingKVBackend{schedule: schedule}
	slow, err := newKVWriter(
		context.Background(), backend, kvWriterTable(), WithKVLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := newKVWriter(
		context.Background(), backend, kvWriterTable(), WithKVLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}

	slowResult := slow.Upsert(context.Background(), Row{int32(1), "slow", nil})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow KV writer did not reach backend")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if result := healthy.Upsert(
		ctx, Row{int32(2), "healthy", nil},
	).Await(ctx); result.Err != nil {
		t.Fatalf("healthy KV writer was blocked: %v", result.Err)
	}
	close(release)
	if result := slowResult.Await(context.Background()); result.Err != nil {
		t.Fatalf("slow KV result = %#v", result)
	}
	if err := slow.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := healthy.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkWriterScheduling(b *testing.B) {
	b.Run("16_writers_8_buckets", func(b *testing.B) {
		benchmarkParallelWriters(b, 16, 8, 0)
	})
	b.Run("16_writers_slow_server", func(b *testing.B) {
		benchmarkParallelWriters(b, 16, 8, 100*time.Microsecond)
	})
	b.Run("failing_server_lifecycle", func(b *testing.B) {
		failure := errors.New("server failure")
		b.ReportAllocs()
		for index := range b.N {
			backend := newSchedulingLogBackend(1)
			backend.err = failure
			writer := newSchedulingLogWriter(b, backend, 1, WithLogLinger(0))
			result := writer.Append(
				context.Background(), Row{int32(index), "failure"},
			).Await(context.Background())
			if !errors.Is(result.Err, failure) {
				b.Fatalf("write result = %#v", result)
			}
			if err := writer.Close(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkParallelWriters(
	b *testing.B,
	writerCount int,
	bucketCount int,
	delay time.Duration,
) {
	backend := newSchedulingLogBackend(bucketCount)
	backend.delay = delay
	writers := make([]*LogWriter, writerCount)
	for index := range writers {
		writers[index] = newSchedulingLogWriter(
			b, backend, bucketCount,
			WithLogLinger(0),
			WithLogBuffer(64),
			WithLogBucketAssignment(AssignmentRoundRobin),
		)
	}
	b.Cleanup(func() {
		for _, writer := range writers {
			if err := writer.Close(context.Background()); err != nil {
				b.Error(err)
			}
		}
	})

	var sequence atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := sequence.Add(1)
			writer := writers[id%uint64(len(writers))]
			result := writer.Append(
				context.Background(), Row{int32(id), "value"},
			).Await(context.Background())
			if result.Err != nil {
				b.Error(result.Err)
				return
			}
		}
	})
}
