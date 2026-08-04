package fgo

import (
	"context"
	"errors"
	"fmt"
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
	locations    map[int32]ServerNode
	nextWriterID int64
	controls     map[int64]*schedulingBackendControl
	delay        time.Duration
	err          error
	calls        atomic.Int64
}

func newSchedulingLogBackend(bucketCount int) *schedulingLogBackend {
	locations := make(map[int32]ServerNode, bucketCount)
	for bucket := range bucketCount {
		locations[int32(bucket)] = ServerNode{
			ID: int32(bucket + 1), Address: "shared-tablet", ServerType: TabletServer,
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
) (int64, map[int32]ServerNode, error) {
	return appendWriterTable().ID, b.locations, nil
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
	batch, err := DecodeLogBatchRows(appendWriterTable().Schema, records, true)
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
) (int64, map[int32]ServerNode, error) {
	return upsertWriterTable().ID, b.schedule.locations, nil
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

type bucketExecutionProbe struct {
	blockedBucket int32
	release       <-chan struct{}
	started       chan struct{}
	startOnce     sync.Once

	mu        sync.Mutex
	sequences map[int32][]int32
}

func newBucketExecutionProbe(blockedBucket int32, release <-chan struct{}) *bucketExecutionProbe {
	return &bucketExecutionProbe{
		blockedBucket: blockedBucket, release: release, started: make(chan struct{}),
		sequences: make(map[int32][]int32),
	}
}

func (p *bucketExecutionProbe) execute(
	ctx context.Context,
	bucket int32,
	sequence int32,
) (int64, error) {
	p.mu.Lock()
	p.sequences[bucket] = append(p.sequences[bucket], sequence)
	p.mu.Unlock()
	if bucket == p.blockedBucket {
		p.startOnce.Do(func() { close(p.started) })
		select {
		case <-p.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return int64(sequence + 1), nil
}

func (p *bucketExecutionProbe) bucketSequences(bucket int32) []int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int32(nil), p.sequences[bucket]...)
}

type bucketIsolationLogBackend struct {
	probe     *bucketExecutionProbe
	locations map[int32]ServerNode
}

func (b bucketIsolationLogBackend) metadata(
	context.Context,
	PhysicalTablePath,
) (int64, map[int32]ServerNode, error) {
	return appendWriterTable().ID, b.locations, nil
}

func (bucketIsolationLogBackend) initWriter(
	context.Context,
	PhysicalTablePath,
	int32,
) (int64, error) {
	return 1, nil
}

func (b bucketIsolationLogBackend) produce(
	ctx context.Context,
	request logProduceRequest,
) (int64, error) {
	batch, err := DecodeLogBatchRows(appendWriterTable().Schema, request.records, true)
	if err != nil {
		return 0, err
	}
	return b.probe.execute(ctx, request.bucket, batch.BatchSequence)
}

type bucketIsolationKVBackend struct {
	probe     *bucketExecutionProbe
	locations map[int32]ServerNode
}

func (b bucketIsolationKVBackend) metadata(
	context.Context,
	PhysicalTablePath,
) (int64, map[int32]ServerNode, error) {
	return upsertWriterTable().ID, b.locations, nil
}

func (bucketIsolationKVBackend) initWriter(
	context.Context,
	PhysicalTablePath,
	int32,
) (int64, error) {
	return 1, nil
}

func (b bucketIsolationKVBackend) put(
	ctx context.Context,
	request kvPutRequest,
) (int64, error) {
	batch, err := DecodeKVBatch(request.records)
	if err != nil {
		return 0, err
	}
	return b.probe.execute(ctx, request.bucket, batch.BatchSequence)
}

func twoBucketLocations() map[int32]ServerNode {
	return map[int32]ServerNode{
		0: {ID: 1, Address: "tablet-0", ServerType: TabletServer},
		1: {ID: 2, Address: "tablet-1", ServerType: TabletServer},
	}
}

func newSchedulingAppendWriter(
	t testing.TB,
	backend appendWriterBackend,
	bucketCount int,
	options ...AppendWriterOption,
) *AppendWriter {
	t.Helper()
	table := appendWriterTable()
	table.BucketCount = bucketCount
	writer, err := newAppendWriter(context.Background(), backend, table, options...)
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

	slow := newSchedulingAppendWriter(
		t, backend, 1, WithAppendLinger(0), WithAppendBuffer(2),
	)
	fast := newSchedulingAppendWriter(t, backend, 1, WithAppendLinger(0))

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
	failing := newSchedulingAppendWriter(t, backend, 1, WithAppendLinger(0))
	healthy := newSchedulingAppendWriter(t, backend, 1, WithAppendLinger(0))

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

func TestUpsertWriterSchedulingIsolatesSlowWriter(t *testing.T) {
	schedule := newSchedulingLogBackend(1)
	release := make(chan struct{})
	started := make(chan struct{})
	schedule.setControl(1, &schedulingBackendControl{block: release, started: started})
	backend := schedulingKVBackend{schedule: schedule}
	slow, err := newUpsertWriter(
		context.Background(), backend, upsertWriterTable(), WithUpsertLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := newUpsertWriter(
		context.Background(), backend, upsertWriterTable(), WithUpsertLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}

	slowResult := slow.Upsert(context.Background(), Row{int32(1), "slow", nil})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow upsert writer did not reach backend")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if result := healthy.Upsert(
		ctx, Row{int32(2), "healthy", nil},
	).Await(ctx); result.Err != nil {
		t.Fatalf("healthy upsert writer was blocked: %v", result.Err)
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

func TestAppendWriterIsolatesSlowBucketAndPreservesBucketOrder(t *testing.T) {
	release := make(chan struct{})
	probe := newBucketExecutionProbe(0, release)
	backend := bucketIsolationLogBackend{probe: probe, locations: twoBucketLocations()}
	writer := newSchedulingAppendWriter(
		t, backend, 2, WithAppendLinger(0), WithAppendConcurrency(2),
		WithAppendBucketAssignment(AssignmentRoundRobin),
	)
	first := writer.Append(context.Background(), Row{int32(1), "blocked"})
	select {
	case <-probe.started:
	case <-time.After(time.Second):
		t.Fatal("blocked bucket did not reach backend")
	}
	second := writer.Append(context.Background(), Row{int32(2), "independent"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if result := second.Await(ctx); result.Err != nil || result.Bucket != 1 {
		t.Fatalf("independent bucket result = %#v", result)
	}
	third := writer.Append(context.Background(), Row{int32(3), "ordered"})
	fourth := writer.Append(context.Background(), Row{int32(4), "barrier"})
	if result := fourth.Await(ctx); result.Err != nil || result.Bucket != 1 {
		t.Fatalf("barrier bucket result = %#v", result)
	}
	if sequences := probe.bucketSequences(0); len(sequences) != 1 ||
		sequences[0] != 0 {
		t.Fatalf("blocked bucket sequences before release = %v", sequences)
	}
	close(release)
	for _, future := range []*WriteFuture{first, third} {
		if result := future.Await(context.Background()); result.Err != nil || result.Bucket != 0 {
			t.Fatalf("blocked bucket result = %#v", result)
		}
	}
	if sequences := probe.bucketSequences(0); len(sequences) != 2 ||
		sequences[0] != 0 || sequences[1] != 1 {
		t.Fatalf("blocked bucket sequences = %v", sequences)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUpsertWriterIsolatesSlowBucketAndPreservesBucketOrder(t *testing.T) {
	release := make(chan struct{})
	probe := newBucketExecutionProbe(0, release)
	table := upsertWriterTable()
	table.BucketCount = 2
	backend := bucketIsolationKVBackend{probe: probe, locations: twoBucketLocations()}
	writer, err := newUpsertWriter(
		context.Background(), backend, table,
		WithUpsertLinger(0), WithUpsertConcurrency(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	blockedID := kvIDForBucket(t, table.Schema, 0)
	independentID := kvIDForBucket(t, table.Schema, 1)
	first := writer.Upsert(context.Background(), Row{blockedID, "blocked", nil})
	select {
	case <-probe.started:
	case <-time.After(time.Second):
		t.Fatal("blocked bucket did not reach backend")
	}
	second := writer.Upsert(context.Background(), Row{independentID, "independent", nil})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if result := second.Await(ctx); result.Err != nil || result.Bucket != 1 {
		t.Fatalf("independent bucket result = %#v", result)
	}
	third := writer.Upsert(context.Background(), Row{blockedID, "ordered", nil})
	fourth := writer.Upsert(context.Background(), Row{independentID, "barrier", nil})
	if result := fourth.Await(ctx); result.Err != nil || result.Bucket != 1 {
		t.Fatalf("barrier bucket result = %#v", result)
	}
	if sequences := probe.bucketSequences(0); len(sequences) != 1 ||
		sequences[0] != 0 {
		t.Fatalf("blocked bucket sequences before release = %v", sequences)
	}
	close(release)
	for _, future := range []*WriteFuture{first, third} {
		if result := future.Await(context.Background()); result.Err != nil || result.Bucket != 0 {
			t.Fatalf("blocked bucket result = %#v", result)
		}
	}
	if sequences := probe.bucketSequences(0); len(sequences) != 2 ||
		sequences[0] != 0 || sequences[1] != 1 {
		t.Fatalf("blocked bucket sequences = %v", sequences)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func kvIDForBucket(t *testing.T, schema Schema, bucket int32) int32 {
	t.Helper()
	names := schema.BucketKey
	if len(names) == 0 {
		names = schema.PrimaryKey
	}
	for id := int32(0); id < 10_000; id++ {
		encoded, err := encodeKeyColumns(schema, names, PrimaryKey{id})
		if err != nil {
			t.Fatal(err)
		}
		hashed, err := flussBucket(encoded, 2)
		if err != nil {
			t.Fatal(err)
		}
		if hashed == bucket {
			return id
		}
	}
	t.Fatalf("no key found for bucket %d", bucket)
	return 0
}

func BenchmarkWriterScheduling(b *testing.B) {
	for _, test := range []struct {
		name        string
		writers     int
		buckets     int
		serverDelay time.Duration
	}{
		{"single_writer_single_bucket_saturation", 1, 1, 100 * time.Microsecond},
		{"single_writer_8_buckets_saturation", 1, 8, 100 * time.Microsecond},
		{"16_writers_8_buckets", 16, 8, 0},
		{"16_writers_slow_server", 16, 8, 100 * time.Microsecond},
	} {
		b.Run(test.name, func(b *testing.B) {
			benchmarkParallelWriters(
				b, test.writers, test.buckets, test.serverDelay,
			)
		})
	}
	b.Run("failing_server_lifecycle", func(b *testing.B) {
		benchmarkFailingWriterLifecycle(b)
	})
	b.Run("64_writer_lifecycle", func(b *testing.B) {
		benchmarkWriterLifecycle(b, 64)
	})
}

func benchmarkFailingWriterLifecycle(b *testing.B) {
	failure := errors.New("server failure")
	b.ReportAllocs()
	for index := range b.N {
		backend := newSchedulingLogBackend(1)
		backend.err = failure
		writer := newSchedulingAppendWriter(b, backend, 1, WithAppendLinger(0))
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
}

func benchmarkWriterLifecycle(b *testing.B, count int) {
	b.ReportAllocs()
	for range b.N {
		backend := newSchedulingLogBackend(8)
		writers := make([]*AppendWriter, count)
		for index := range writers {
			writers[index] = newSchedulingAppendWriter(
				b, backend, 8, WithAppendLinger(0), WithAppendBuffer(64),
			)
		}
		for _, writer := range writers {
			if err := writer.Close(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkWriterBatching(b *testing.B) {
	for _, records := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("%d_records", records), func(b *testing.B) {
			benchmarkWriterBatchSize(b, records)
		})
	}
}

func benchmarkWriterBatchSize(b *testing.B, records int) {
	backend := newSchedulingLogBackend(1)
	writer := newSchedulingAppendWriter(
		b, backend, 1,
		WithAppendBatchLimits(1<<20, records),
		WithAppendBuffer(records),
		WithAppendLinger(time.Hour),
	)
	b.Cleanup(func() {
		if err := writer.Close(context.Background()); err != nil {
			b.Error(err)
		}
	})

	b.ReportAllocs()
	b.SetBytes(int64(len("value")))
	b.ResetTimer()
	for written := 0; written < b.N; {
		count := records
		if remaining := b.N - written; remaining < count {
			count = remaining
		}
		futures := make([]*WriteFuture, count)
		for index := range count {
			futures[index] = writer.Append(
				context.Background(), Row{int32(written + index), "value"},
			)
		}
		if err := writer.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
		for _, future := range futures {
			if result := future.Await(context.Background()); result.Err != nil {
				b.Fatal(result.Err)
			}
		}
		written += count
	}
}

func benchmarkParallelWriters(
	b *testing.B,
	writerCount int,
	bucketCount int,
	delay time.Duration,
) {
	backend := newSchedulingLogBackend(bucketCount)
	backend.delay = delay
	writers := make([]*AppendWriter, writerCount)
	for index := range writers {
		writers[index] = newSchedulingAppendWriter(
			b, backend, bucketCount,
			WithAppendLinger(0),
			WithAppendBuffer(64),
			WithAppendBucketAssignment(AssignmentRoundRobin),
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
