package fgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// UpsertWriter batches primary-key upserts and deletes.
// It owns one background scheduler and must be closed after use.
type UpsertWriter struct {
	table         Table
	path          PhysicalTablePath
	backend       upsertWriterBackend
	config        UpsertWriterConfig
	tableID       int64
	partitionID   int64
	writerID      int64
	bucketCount   int32
	keys          kvKeyLayout
	valueEncoding rowEncoding
	commands      chan upsertWriterCommand
	slots         chan struct{}
	done          chan struct{}
	admission     writerAdmission
	closeErr      error // Published by closing done after the scheduler exits.
	observer      MetricsObserver
}

type upsertWriterCommand struct {
	item  *pendingKVWrite
	flush chan error
	close chan error
}

type pendingKVWrite struct {
	ctx      context.Context
	record   KVRecord
	bucket   int32
	targets  []int32
	size     int
	queuedAt time.Time
	future   *WriteFuture
}

type kvPendingBatch struct {
	items   []*pendingKVWrite
	records []KVRecord
	targets []int32
	bytes   int
}

type upsertWriterLoop struct {
	writer        *UpsertWriter
	buckets       []int32 // Sorted buckets with admitted writes, including ones without a leader at open.
	batches       map[int32]*kvPendingBatch
	pending       map[int32][]*kvPendingBatch
	active        map[int32]bool
	sequences     map[int32]int32
	poisoned      map[int32]error
	throttle      map[int32]time.Time
	completions   chan kvBatchCompletion
	inFlight      int
	timer         *time.Timer
	timerC        <-chan time.Time
	pressureTimer *time.Timer
	pressureC     <-chan time.Time
	closing       bool
}

type kvBatchCompletion struct {
	bucket        int32
	batch         *kvPendingBatch
	pressure      float32
	pressureKnown bool
	bytes         int
	started       time.Time
	err           error
}

// NewUpsertWriter creates a primary-key writer for table.
func (c *Client) NewUpsertWriter(ctx context.Context, table Table, options ...UpsertWriterOption) (*UpsertWriter, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	writer, err := newUpsertWriter(ctx, clientUpsertWriterBackend{client: c}, table, options...)
	if err == nil {
		writer.observer = c.observer
	}
	return writer, err
}

func newUpsertWriter(ctx context.Context, backend upsertWriterBackend, table Table, options ...UpsertWriterOption) (*UpsertWriter, error) {
	if err := validateUpsertWriterTable(table); err != nil {
		return nil, err
	}
	valueEncoding, err := kvValueEncoding(table)
	if err != nil {
		return nil, err
	}
	config, err := upsertWriterConfig(options)
	if err != nil {
		return nil, err
	}
	if config.MergeMode == MergeModeOverwrite && table.Properties != nil &&
		strings.TrimSpace(table.Properties["table.merge-engine"]) == "" {
		return nil, fmt.Errorf(
			"%w: KV overwrite requires table %s to configure table.merge-engine",
			ErrInvalidConfig, table.Path,
		)
	}
	path := PhysicalTablePath{TablePath: table.Path, Partition: config.Partition}
	keys, err := newKVKeyLayout(table.Schema, path.Partition)
	if err != nil {
		return nil, err
	}
	if backend, ok := backend.(interface {
		ensurePartition(context.Context, PhysicalTablePath, []string) error
	}); ok {
		if err := backend.ensurePartition(ctx, path, table.Schema.PartitionKey); err != nil {
			return nil, err
		}
	}
	layout, err := backend.metadata(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := layout.validateTableID(path, table.ID); err != nil {
		return nil, err
	}
	if layout.count <= 0 {
		return nil, fmt.Errorf("%w: %s has no routing bucket count", ErrMetadata, path)
	}
	if len(layout.leaders) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoBucketLeader, path)
	}
	leaders, err := sortedBuckets(layout.leaders)
	if err != nil {
		return nil, err
	}
	writerID, err := backend.initWriter(ctx, path, leaders[0])
	if err != nil {
		return nil, err
	}
	table = snapshotTable(table)
	writer := &UpsertWriter{
		table: table, path: path, backend: backend, config: config, tableID: table.ID,
		partitionID: layout.partitionID, writerID: writerID, bucketCount: layout.count, keys: keys,
		valueEncoding: valueEncoding,
		commands:      make(chan upsertWriterCommand, config.MaxBuffered),
		slots:         make(chan struct{}, config.MaxBuffered), done: make(chan struct{}),
		admission: newWriterAdmission(),
	}
	go writer.run()
	return writer, nil
}

func validateUpsertWriterTable(table Table) error {
	if err := table.RequirePrimaryKey(); err != nil {
		return err
	}
	if err := table.Schema.Validate(); err != nil {
		return err
	}
	if table.SchemaID < 0 || table.SchemaID > math.MaxInt16 {
		return fmt.Errorf("%w: schema ID exceeds KV batch range", ErrInvalidSchema)
	}
	return nil
}

func upsertWriterConfig(options []UpsertWriterOption) (UpsertWriterConfig, error) {
	config := UpsertWriterConfig{
		MaxBatchBytes: 1 << 20, MaxBatchRecords: 1000, MaxBuffered: 10_000,
		MaxConcurrentRequests: 4,
		BatchTimeout:          5 * time.Millisecond, RequestTimeout: 30 * time.Second, Acks: -1,
		RetryPolicy: defaultWriterRetryPolicy(), BackpressureMaxThrottle: time.Second,
	}
	for _, option := range options {
		if option == nil {
			return UpsertWriterConfig{}, fmt.Errorf("%w: nil upsert writer option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return UpsertWriterConfig{}, err
		}
	}
	if err := validateWriterRetryPolicy(config.RetryPolicy, config.Acks); err != nil {
		return UpsertWriterConfig{}, err
	}
	return config, nil
}

// Upsert queues a complete primary-key row for insertion or update.
func (w *UpsertWriter) Upsert(ctx context.Context, row Row) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: nil context", ErrInvalidConfig)})
		return future
	}
	if len(w.table.Schema.AutoIncrement) != 0 {
		future.complete(WriteResult{Err: fmt.Errorf("%w: full upsert includes auto-increment columns", ErrInvalidRow)})
		return future
	}
	if err := w.table.Schema.ValidateRow(row, nil); err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	value, err := encodeRow(w.table.Schema, row, w.valueEncoding)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	w.enqueueMutation(ctx, rowValues(w.table.Schema, row, nil), value, nil, future)
	return future
}

// PartialUpsert writes only columns named by columns. The columns must contain the complete
// primary key and must be in the same order as values.
func (w *UpsertWriter) PartialUpsert(ctx context.Context, columns []string, values Row) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: nil context", ErrInvalidConfig)})
		return future
	}
	targets, err := w.validatePartial(columns, values)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	full := make(Row, len(w.table.Schema.Columns))
	positions := make(map[string]int, len(w.table.Schema.Columns))
	for index, column := range w.table.Schema.Columns {
		positions[column.Name] = index
	}
	for index, column := range columns {
		full[positions[column]] = values[index]
	}
	value, err := encodeRow(w.table.Schema, full, w.valueEncoding)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	w.enqueueMutation(ctx, rowValues(w.table.Schema, values, columns), value, targets, future)
	return future
}

// Delete queues removal of the row identified by key.
func (w *UpsertWriter) Delete(ctx context.Context, key PrimaryKey) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: nil context", ErrInvalidConfig)})
		return future
	}
	if err := w.table.Schema.ValidatePrimaryKey(key); err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	w.enqueueMutation(ctx, rowValues(w.table.Schema, Row(key), w.table.Schema.PrimaryKey), nil, nil, future)
	return future
}

func (w *UpsertWriter) enqueueMutation(
	ctx context.Context,
	values map[string]any,
	value []byte,
	targets []int32,
	future *WriteFuture,
) {
	key, bucket, err := w.keys.encodePoint(w.table.Schema, values, w.bucketCount)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return
	}
	item := &pendingKVWrite{
		ctx: ctx, record: KVRecord{Key: key, Value: value}, bucket: bucket,
		targets: append([]int32(nil), targets...), size: len(key) + len(value) + 8, future: future,
	}
	if w.observer != nil {
		item.queuedAt = time.Now()
	}
	w.enqueue(ctx, item)
}

func (w *UpsertWriter) validatePartial(columns []string, values Row) ([]int32, error) {
	if len(columns) == 0 || len(columns) != len(values) {
		return nil, fmt.Errorf("%w: partial update columns and values must be non-empty and aligned", ErrInvalidRow)
	}
	if err := w.table.Schema.ValidateRow(values, columns); err != nil {
		return nil, err
	}
	positions := make(map[string]int32, len(w.table.Schema.Columns))
	for index, column := range w.table.Schema.Columns {
		positions[column.Name] = int32(index)
	}
	selected := make(map[string]bool, len(columns))
	targets := make([]int32, len(columns))
	for index, name := range columns {
		if selected[name] {
			return nil, fmt.Errorf("%w: duplicate target column %q", ErrInvalidRow, name)
		}
		selected[name] = true
		targets[index] = positions[name]
	}
	if err := w.validatePartialSelection(selected); err != nil {
		return nil, err
	}
	return targets, nil
}

func (w *UpsertWriter) validatePartialSelection(selected map[string]bool) error {
	for _, name := range w.table.Schema.PrimaryKey {
		if !selected[name] {
			return fmt.Errorf("%w: partial update omits primary key %q", ErrInvalidRow, name)
		}
	}
	for _, name := range w.table.Schema.AutoIncrement {
		if selected[name] {
			return fmt.Errorf("%w: partial update includes auto-increment column %q", ErrInvalidRow, name)
		}
	}
	for _, column := range w.table.Schema.Columns {
		if !selected[column.Name] && !column.Nullable && !slices.Contains(w.table.Schema.PrimaryKey, column.Name) &&
			!slices.Contains(w.table.Schema.AutoIncrement, column.Name) {
			return fmt.Errorf("%w: omitted column %q is not nullable", ErrInvalidRow, column.Name)
		}
	}
	return nil
}

func (w *UpsertWriter) enqueue(ctx context.Context, item *pendingKVWrite) {
	if err := w.admission.beginSubmission(ctx, w.slots, w.done, item.future); err != nil {
		item.future.complete(WriteResult{Err: err})
		return
	}
	defer w.admission.release()
	select {
	case w.commands <- upsertWriterCommand{item: item}:
	case <-ctx.Done():
		item.future.complete(WriteResult{Err: ctx.Err()})
	case <-w.done:
		item.future.complete(WriteResult{Err: ErrClosed})
	}
}

// Flush waits until every previously accepted mutation reaches a terminal
// result. It returns all unrecoverable batch failures observed by this writer,
// including failures reported by earlier Flush calls. A caller's context only
// bounds its wait.
func (w *UpsertWriter) Flush(ctx context.Context) error {
	if err := w.admission.beginCommand(ctx); err != nil {
		return err
	}
	barrier := make(chan error, 1)
	select {
	case w.commands <- upsertWriterCommand{flush: barrier}:
		w.admission.release()
	case <-ctx.Done():
		w.admission.release()
		return ctx.Err()
	}
	select {
	case err := <-barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes accepted mutations and stops the writer.
// Close is idempotent and retains the terminal result, including batch failures
// already reported by Flush. A caller's context only bounds its wait.
func (w *UpsertWriter) Close(ctx context.Context) error {
	started, err := w.admission.beginClose(ctx, w.done, &w.closeErr)
	if !started {
		return err
	}
	barrier := make(chan error, 1)
	select {
	case w.commands <- upsertWriterCommand{close: barrier}:
		w.admission.release()
	case <-ctx.Done():
		w.admission.closed = false
		w.admission.release()
		return ctx.Err()
	}
	select {
	case err := <-barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *UpsertWriter) run() {
	defer close(w.done)
	timer := time.NewTimer(w.config.BatchTimeout)
	if !timer.Stop() {
		<-timer.C
	}
	loop := &upsertWriterLoop{
		writer: w, batches: make(map[int32]*kvPendingBatch), pending: make(map[int32][]*kvPendingBatch),
		active: make(map[int32]bool), sequences: make(map[int32]int32),
		poisoned: make(map[int32]error), throttle: make(map[int32]time.Time),
		completions: make(chan kvBatchCompletion, w.config.MaxConcurrentRequests),
		timer:       timer,
	}
	loop.run()
}

func (l *upsertWriterLoop) run() {
	for {
		select {
		case command := <-l.writer.commands:
			switch {
			case command.item != nil:
				l.add(command.item)
			case command.flush != nil:
				command.flush <- l.flushAll()
			case command.close != nil:
				l.closing = true
				err := l.flushAll()
				l.writer.closeErr = err
				command.close <- err
				l.timer.Stop()
				return
			}
		case <-l.timerC:
			l.timerC = nil
			l.queueAll()
		case <-l.pressureC:
			l.pressureC = nil
			l.dispatch()
		case completion := <-l.completions:
			l.handleCompletion(completion)
		}
	}
}

func (l *upsertWriterLoop) add(item *pendingKVWrite) {
	if err := item.ctx.Err(); err != nil {
		item.future.complete(WriteResult{Err: err})
		return
	}
	if index, found := slices.BinarySearch(l.buckets, item.bucket); !found {
		l.buckets = slices.Insert(l.buckets, index, item.bucket)
	}
	batch := l.compatibleBatch(item)
	if len(batch.items) > 0 && l.batchFullWith(batch, item) {
		l.flushBucket(item.bucket)
		batch = l.newBatch(item)
	}
	batch.items = append(batch.items, item)
	batch.records = append(batch.records, item.record)
	batch.bytes += item.size
	if l.batchFull(batch) || l.writer.config.BatchTimeout == 0 {
		l.flushBucket(item.bucket)
		return
	}
	l.armTimer()
}

func (l *upsertWriterLoop) compatibleBatch(item *pendingKVWrite) *kvPendingBatch {
	batch := l.batches[item.bucket]
	if batch != nil && targetKey(batch.targets) != targetKey(item.targets) {
		l.flushBucket(item.bucket)
		batch = nil
	}
	if batch == nil {
		batch = l.newBatch(item)
	}
	return batch
}

func (l *upsertWriterLoop) newBatch(item *pendingKVWrite) *kvPendingBatch {
	batch := &kvPendingBatch{targets: append([]int32(nil), item.targets...)}
	l.batches[item.bucket] = batch
	return batch
}

func (l *upsertWriterLoop) batchFull(batch *kvPendingBatch) bool {
	return len(batch.items) >= l.writer.config.MaxBatchRecords || batch.bytes >= l.writer.config.MaxBatchBytes
}

func (l *upsertWriterLoop) batchFullWith(batch *kvPendingBatch, item *pendingKVWrite) bool {
	return len(batch.items) >= l.writer.config.MaxBatchRecords ||
		batch.bytes+item.size > l.writer.config.MaxBatchBytes
}

func (l *upsertWriterLoop) flushBucket(bucket int32) {
	batch := l.batches[bucket]
	if batch == nil || len(batch.items) == 0 {
		return
	}
	delete(l.batches, bucket)
	if err := l.poisoned[bucket]; err != nil {
		poisoned := fmt.Errorf("%w: bucket %d: %w", ErrWriterState, bucket, err)
		l.writer.completeKVBatch(batch, bucket, 0, false, poisoned)
		return
	}
	l.pending[bucket] = append(l.pending[bucket], batch)
	l.dispatch()
}

func (l *upsertWriterLoop) dispatch() {
	var next time.Time
	now := time.Now()
	for _, bucket := range l.buckets {
		if l.inFlight == l.writer.config.MaxConcurrentRequests {
			break
		}
		if l.active[bucket] || len(l.pending[bucket]) == 0 {
			continue
		}
		if deadline := l.throttle[bucket]; !l.closing && deadline.After(now) {
			if next.IsZero() || deadline.Before(next) {
				next = deadline
			}
			continue
		}
		delete(l.throttle, bucket)
		batch := popWriterBatch(l.pending, bucket)
		l.active[bucket] = true
		l.inFlight++
		go l.executeBatch(bucket, batch, l.sequences[bucket])
	}
	if l.inFlight == l.writer.config.MaxConcurrentRequests {
		// Completion will dispatch again once an RPC slot is free.
		next = time.Time{}
	}
	l.schedulePressureWakeup(next)
}

func (l *upsertWriterLoop) schedulePressureWakeup(next time.Time) {
	if next.IsZero() {
		if l.pressureC != nil {
			l.pressureTimer.Stop()
			l.pressureC = nil
		}
		return
	}
	delay := time.Until(next)
	if l.pressureTimer == nil {
		l.pressureTimer = time.NewTimer(delay)
	} else {
		l.pressureTimer.Reset(delay)
	}
	l.pressureC = l.pressureTimer.C
}

func (l *upsertWriterLoop) executeBatch(bucket int32, batch *kvPendingBatch, sequence int32) {
	encoded, err := (KVBatch{
		SchemaID: int16(l.writer.table.SchemaID), WriterID: l.writer.writerID,
		BatchSequence: sequence, Records: batch.records,
	}).Encode()
	var result writerAttemptResult
	var pressure float32
	var pressureKnown bool
	started := metricStart(l.writer.observer)
	if err == nil {
		result = executeWriterRequest(
			l.writer.config.RequestTimeout, l.writer.config.RetryPolicy, l.writer.observer, MetricOperationKVWrite,
			func(ctx context.Context) (int64, bool, error) {
				put, err := l.writer.backend.put(ctx, kvPutRequest{
					path: l.writer.path, bucket: bucket, bucketCount: l.writer.bucketCount, tableID: l.writer.tableID, partitionID: l.writer.partitionID,
					targets: batch.targets, records: encoded, timeout: l.writer.config.RequestTimeout, acks: l.writer.config.Acks,
					mergeMode: l.writer.config.MergeMode,
				})
				if err == nil {
					pressure, pressureKnown = put.pressure, put.pressureKnown
				}
				// PutKv reports a bucket-wide log end, not the start offset
				// of any mutation. A mutation can emit zero, one, or two events.
				return 0, false, err
			},
		)
	} else {
		result.err = err
	}
	l.completions <- kvBatchCompletion{
		bucket: bucket, batch: batch,
		pressure: pressure, pressureKnown: pressureKnown,
		bytes: len(encoded), started: started, err: result.err,
	}
}

func (l *upsertWriterLoop) handleCompletion(completion kvBatchCompletion) {
	delete(l.active, completion.bucket)
	l.inFlight--
	queueTime := time.Duration(0)
	if len(completion.batch.items) != 0 && !completion.batch.items[0].queuedAt.IsZero() {
		queueTime = time.Since(completion.batch.items[0].queuedAt)
	}
	observeMetric(l.writer.observer, MetricEvent{
		Kind: MetricWriteBatch, Operation: MetricOperationKVWrite,
		Duration: metricDuration(completion.started), QueueTime: queueTime,
		QueueSize: len(l.writer.commands), Records: int64(len(completion.batch.records)), Bytes: int64(completion.bytes),
		Failed: completion.err != nil, ErrorClass: metricErrorClass(completion.err),
	})
	if completion.err != nil {
		l.poisoned[completion.bucket] = completion.err
		l.writer.completeKVBatch(completion.batch, completion.bucket, 0, false, completion.err)
		for _, batch := range l.pending[completion.bucket] {
			poisoned := fmt.Errorf(
				"%w: bucket %d: %w", ErrWriterState, completion.bucket, completion.err,
			)
			l.writer.completeKVBatch(batch, completion.bucket, 0, false, poisoned)
		}
		delete(l.pending, completion.bucket)
	} else {
		if completion.pressureKnown && completion.pressure > 0 && l.writer.config.BackpressureMaxThrottle > 0 {
			delay := time.Duration(float64(l.writer.config.BackpressureMaxThrottle) * float64(completion.pressure) * float64(completion.pressure))
			l.throttle[completion.bucket] = time.Now().Add(delay)
		} else if completion.pressureKnown {
			delete(l.throttle, completion.bucket)
		}
		l.sequences[completion.bucket]++
		l.writer.completeKVBatch(
			completion.batch, completion.bucket, completion.pressure, completion.pressureKnown, nil,
		)
	}
	l.dispatch()
}

func (l *upsertWriterLoop) flushAll() error {
	l.stopTimer()
	l.queueAll()
	l.dispatch()
	for l.inFlight > 0 || l.pressureC != nil {
		select {
		case completion := <-l.completions:
			l.handleCompletion(completion)
		case <-l.pressureC:
			l.pressureC = nil
			l.dispatch()
		}
	}
	// Retain one cause per failed bucket for all barriers, independent of when
	// the completion was processed. Use bucket order for deterministic results.
	var failures []error
	for _, bucket := range l.buckets {
		if err := l.poisoned[bucket]; err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (l *upsertWriterLoop) queueAll() {
	l.stopTimer()
	for _, bucket := range l.buckets {
		l.flushBucket(bucket)
	}
}

func (l *upsertWriterLoop) armTimer() {
	if l.timerC == nil && l.writer.config.BatchTimeout > 0 {
		l.timer.Reset(l.writer.config.BatchTimeout)
		l.timerC = l.timer.C
	}
}

func (l *upsertWriterLoop) stopTimer() {
	if l.timerC != nil && !l.timer.Stop() {
		select {
		case <-l.timer.C:
		default:
		}
	}
	l.timerC = nil
}

func (w *UpsertWriter) completeKVBatch(
	batch *kvPendingBatch,
	bucket int32,
	pressure float32,
	pressureKnown bool,
	err error,
) {
	for _, item := range batch.items {
		item.future.complete(WriteResult{
			Bucket:  bucket,
			Records: 1, Pressure: pressure, PressureKnown: pressureKnown, Err: err,
		})
	}
}

func targetKey(targets []int32) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(strconv.FormatInt(int64(target), 10))
		builder.WriteByte(',')
	}
	return builder.String()
}
