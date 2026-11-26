package fgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ErrWriterState reports that cancellation or transport failure left the
// server outcome of a mutation unknown.
var ErrWriterState = errors.New("fgo: writer state is uncertain")

// NoKeyAssigner selects how rows without a bucket key are routed to buckets.
type NoKeyAssigner string

// No-key assigners supported by [AppendWriter].
const (
	NoKeyAssignerSticky     NoKeyAssigner = "sticky"
	NoKeyAssignerRoundRobin NoKeyAssigner = "round-robin"
)

// LogFormat selects the row or Arrow encoding used for log batches.
type LogFormat string

// Log write formats supported by Apache Fluss 1.0.
const (
	LogFormatAuto      LogFormat = "auto"
	LogFormatArrow     LogFormat = "arrow"
	LogFormatIndexed   LogFormat = "indexed"
	LogFormatCompacted LogFormat = "compacted"
)

// AppendWriter batches append operations for a log table.
// It owns one background scheduler and must be closed after use.
type AppendWriter struct {
	table              Table
	bucketKeyPositions []int
	partitionColumns   []physicalPartitionColumn
	path               PhysicalTablePath
	backend            appendWriterBackend
	config             AppendWriterConfig
	tableID            int64
	partitionID        int64
	writerID           int64
	buckets            []int32
	commands           chan writerCommand
	slots              chan struct{}
	done               chan struct{}
	admission          writerAdmission
	closeErr           error // Published by closing done after the scheduler exits.
	roundRobin         int
	stickyIndex        int
	observer           MetricsObserver
}

type writerCommand struct {
	item  *pendingWrite
	flush chan error
	close chan error
}

type pendingWrite struct {
	ctx       context.Context
	encoded   []byte
	bucketKey []byte
	arrow     arrow.RecordBatch
	changes   []ChangeType
	bucket    *int32
	size      int
	queuedAt  time.Time
	future    *WriteFuture
}

type bucketBatch struct {
	items []*pendingWrite
	bytes int
}

type appendWriterLoop struct {
	writer      *AppendWriter
	batches     map[int32]*bucketBatch
	pending     map[int32][]*bucketBatch
	active      map[int32]bool
	sequences   map[int32]int32
	poisoned    map[int32]error
	completions chan logBatchCompletion
	inFlight    int
	timer       *time.Timer
	timerC      <-chan time.Time
}

type logBatchCompletion struct {
	bucket      int32
	batch       *bucketBatch
	baseOffset  int64
	offsetKnown bool
	records     int
	bytes       int
	started     time.Time
	err         error
}

// NewAppendWriter creates an append writer for a log table. It snapshots table
// schema metadata; later changes to the caller's table do not affect this writer.
func (c *Client) NewAppendWriter(ctx context.Context, table Table, options ...AppendWriterOption) (*AppendWriter, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	writer, err := newAppendWriter(ctx, clientAppendWriterBackend{client: c}, table, options...)
	if err == nil {
		writer.observer = c.observer
	}
	return writer, err
}

func newAppendWriter(ctx context.Context, backend appendWriterBackend, table Table, options ...AppendWriterOption) (*AppendWriter, error) {
	if err := validateAppendWriterTable(table); err != nil {
		return nil, err
	}
	table = snapshotAppendTable(table)
	config, err := appendWriterConfig(options)
	if err != nil {
		return nil, err
	}
	if err := resolveAppendWriterFormat(table, &config); err != nil {
		return nil, err
	}
	path := PhysicalTablePath{TablePath: table.Path, Partition: config.Partition}
	partitionColumns, err := physicalPartitionColumns(table.Schema, path.Partition)
	if err != nil {
		return nil, err
	}
	if err := ensureAppendWriterPartition(ctx, backend, path, table.Schema.PartitionKey); err != nil {
		return nil, err
	}
	layout, err := backend.metadata(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := layout.validateTableID(path, table.ID); err != nil {
		return nil, err
	}
	buckets, err := completeBucketIDs(layout.count, layout.leaders)
	if err != nil {
		return nil, fmt.Errorf("fgo: open append writer for %s: %w", path, err)
	}
	writerID, err := backend.initWriter(ctx, path, buckets[0])
	if err != nil {
		return nil, err
	}
	writer := &AppendWriter{
		table: table, path: path, backend: backend, config: config, tableID: table.ID, partitionID: layout.partitionID,
		bucketKeyPositions: appendBucketKeyPositions(table.Schema),
		partitionColumns:   partitionColumns,
		writerID:           writerID, buckets: buckets, commands: make(chan writerCommand, config.MaxBuffered),
		slots: make(chan struct{}, config.MaxBuffered), done: make(chan struct{}),
		admission: newWriterAdmission(), stickyIndex: -1,
	}
	go writer.run()
	return writer, nil
}

func resolveAppendWriterFormat(table Table, config *AppendWriterConfig) error {
	format := LogFormat(strings.ToLower(strings.TrimSpace(table.Properties["table.log.format"])))
	switch format {
	case "":
		// If metadata did not advertise a format, Auto still accepts either
		// append method and uses compacted encoding for row appends.
	case LogFormatCompacted, LogFormatIndexed, LogFormatArrow:
		if config.Format == LogFormatAuto {
			config.Format = format
		} else if config.Format != format {
			return fmt.Errorf("%w: writer format %s does not match table.log.format %s", ErrInvalidConfig, config.Format, format)
		}
	default:
		return fmt.Errorf("%w: unsupported table.log.format %q", ErrInvalidConfig, format)
	}
	if config.arrowCompressionSet &&
		config.Format != LogFormatAuto && config.Format != LogFormatArrow {
		return fmt.Errorf("%w: Arrow compression requires Arrow-capable table and writer formats", ErrInvalidConfig)
	}
	return nil
}

func ensureAppendWriterPartition(
	ctx context.Context,
	backend appendWriterBackend,
	path PhysicalTablePath,
	partitionKeys []string,
) error {
	ensurer, ok := backend.(interface {
		ensurePartition(context.Context, PhysicalTablePath, []string) error
	})
	if !ok {
		return nil
	}
	return ensurer.ensurePartition(ctx, path, partitionKeys)
}

func validateAppendWriterTable(table Table) error {
	if err := table.RequireLog(); err != nil {
		return err
	}
	if err := table.Schema.Validate(); err != nil {
		return err
	}
	if table.SchemaID < 0 || table.SchemaID > math.MaxInt16 {
		return fmt.Errorf("%w: schema ID exceeds log batch range", ErrInvalidSchema)
	}
	return nil
}

func appendWriterConfig(options []AppendWriterOption) (AppendWriterConfig, error) {
	config := AppendWriterConfig{
		MaxBatchBytes: 1 << 20, MaxBatchRecords: 1000, MaxBuffered: 10_000,
		MaxConcurrentRequests: 4,
		BatchTimeout:          5 * time.Millisecond, RequestTimeout: 30 * time.Second, Acks: -1,
		RetryPolicy: defaultWriterRetryPolicy(),
		Format:      LogFormatAuto,
	}
	for _, option := range options {
		if option == nil {
			return AppendWriterConfig{}, fmt.Errorf("%w: nil append writer option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return AppendWriterConfig{}, err
		}
	}
	if err := validateWriterRetryPolicy(config.RetryPolicy, config.Acks); err != nil {
		return AppendWriterConfig{}, err
	}
	return config, nil
}

// Append queues one row. The returned future completes exactly once after acknowledgment or
// failure. Before Append returns, the writer encodes its own snapshot of the row
// and bucket key, so the caller may reuse the row and its mutable nested values.
func (w *AppendWriter) Append(ctx context.Context, row Row) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: nil context", ErrInvalidConfig)})
		return future
	}
	if w.config.Format == LogFormatArrow {
		future.complete(WriteResult{
			Err: fmt.Errorf("%w: Arrow format requires AppendArrow", ErrInvalidConfig),
		})
		return future
	}
	if err := validateRowValues(w.table.Schema.Columns, row); err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	for _, partition := range w.partitionColumns {
		if !partition.matches(row[partition.position]) {
			future.complete(WriteResult{Err: fmt.Errorf("%w: row column %q differs from the selected partition", ErrInvalidRow, partition.name)})
			return future
		}
	}
	encoding := compactedEncoding
	if w.config.Format == LogFormatIndexed {
		encoding = indexedEncoding
	}
	encoded, err := encodeValidatedRow(w.table.Schema, row, encoding)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	key, err := w.encodeBucketKey(row)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	item := &pendingWrite{
		ctx: ctx, encoded: encoded, bucketKey: key, size: len(encoded) + 5, future: future,
	}
	if w.observer != nil {
		item.queuedAt = time.Now()
	}
	w.enqueue(ctx, item)
	return future
}

// AppendArrow queues one Arrow record batch for an explicit bucket. The caller must retain the
// record batch until the returned future completes.
func (w *AppendWriter) AppendArrow(ctx context.Context, bucket int32, batch arrow.RecordBatch, changes []ChangeType) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil || batch == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: context and Arrow batch are required", ErrInvalidConfig)})
		return future
	}
	if w.config.Format == LogFormatIndexed || w.config.Format == LogFormatCompacted {
		future.complete(WriteResult{
			Err: fmt.Errorf("%w: %s format does not accept Arrow batches", ErrInvalidConfig, w.config.Format),
		})
		return future
	}
	expected, err := w.table.Schema.ArrowSchema()
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	if !expected.Equal(batch.Schema()) {
		future.complete(WriteResult{Err: fmt.Errorf("%w: Arrow batch schema does not match table", ErrInvalidSchema)})
		return future
	}
	if !w.hasBucket(bucket) || int64(len(changes)) != batch.NumRows() {
		future.complete(WriteResult{Err: fmt.Errorf("%w: invalid Arrow bucket or change count", ErrInvalidRow)})
		return future
	}
	if err := validateArrowBatchNullability(expected, batch); err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	for _, change := range changes {
		if err := change.Validate(); err != nil {
			future.complete(WriteResult{Err: err})
			return future
		}
	}
	item := &pendingWrite{
		ctx: ctx, arrow: batch, changes: append([]ChangeType(nil), changes...),
		bucket: &bucket, size: int(batch.NumRows()), future: future,
	}
	if w.observer != nil {
		item.queuedAt = time.Now()
	}
	w.enqueue(ctx, item)
	return future
}

func (w *AppendWriter) enqueue(ctx context.Context, item *pendingWrite) {
	if err := w.admission.beginSubmission(ctx, w.slots, w.done, item.future); err != nil {
		item.future.complete(WriteResult{Err: err})
		return
	}
	defer w.admission.release()
	select {
	case w.commands <- writerCommand{item: item}:
	case <-ctx.Done():
		item.future.complete(WriteResult{Err: ctx.Err()})
	case <-w.done:
		item.future.complete(WriteResult{Err: ErrClosed})
	}
}

// Flush waits until every previously accepted append reaches a terminal result.
// It returns all unrecoverable batch failures observed by this writer, including
// failures reported by earlier Flush calls. A caller's context only bounds its wait.
func (w *AppendWriter) Flush(ctx context.Context) error {
	if err := w.admission.beginCommand(ctx); err != nil {
		return err
	}
	barrier := make(chan error, 1)
	select {
	case w.commands <- writerCommand{flush: barrier}:
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

// Close flushes accepted appends and stops the writer.
// Close is idempotent and retains the terminal result, including batch failures
// already reported by Flush. A caller's context only bounds its wait.
func (w *AppendWriter) Close(ctx context.Context) error {
	started, err := w.admission.beginClose(ctx, w.done, &w.closeErr)
	if !started {
		return err
	}
	barrier := make(chan error, 1)
	select {
	case w.commands <- writerCommand{close: barrier}:
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

func (w *AppendWriter) run() {
	defer close(w.done)
	timer := time.NewTimer(w.config.BatchTimeout)
	if !timer.Stop() {
		<-timer.C
	}
	loop := &appendWriterLoop{
		writer: w, batches: make(map[int32]*bucketBatch), pending: make(map[int32][]*bucketBatch),
		active: make(map[int32]bool), sequences: make(map[int32]int32),
		poisoned:    make(map[int32]error),
		completions: make(chan logBatchCompletion, w.config.MaxConcurrentRequests),
		timer:       timer,
	}
	loop.run()
}

func (l *appendWriterLoop) run() {
	for {
		select {
		case command := <-l.writer.commands:
			switch {
			case command.item != nil:
				l.add(command.item)
			case command.flush != nil:
				command.flush <- l.flushAll()
			case command.close != nil:
				err := l.flushAll()
				l.writer.closeErr = err
				command.close <- err
				l.timer.Stop()
				return
			}
		case <-l.timerC:
			l.timerC = nil
			l.queueAll()
		case completion := <-l.completions:
			l.handleCompletion(completion)
		}
	}
}

func (l *appendWriterLoop) add(item *pendingWrite) {
	if err := item.ctx.Err(); err != nil {
		item.future.complete(WriteResult{Err: err})
		return
	}
	bucket, err := l.writer.assignBucket(item)
	if err != nil {
		item.future.complete(WriteResult{Err: err})
		return
	}
	if item.arrow != nil {
		l.flushBucket(bucket)
		l.batches[bucket] = &bucketBatch{items: []*pendingWrite{item}}
		l.flushBucket(bucket)
		return
	}
	bucket, batch, ok := l.availableBatch(bucket, item)
	if !ok {
		return
	}
	batch.items = append(batch.items, item)
	batch.bytes += item.size
	if l.batchFull(batch) || l.writer.config.BatchTimeout == 0 {
		l.flushBucket(bucket)
		if l.writer.effectiveNoKeyAssigner() == NoKeyAssignerSticky {
			l.writer.advanceSticky()
		}
		return
	}
	l.armTimer()
}

func (l *appendWriterLoop) availableBatch(bucket int32, item *pendingWrite) (int32, *bucketBatch, bool) {
	batch := l.batch(bucket)
	if len(batch.items) == 0 || !l.batchFullWith(batch, item) {
		return bucket, batch, true
	}
	l.flushBucket(bucket)
	if l.writer.effectiveNoKeyAssigner() != NoKeyAssignerSticky {
		return bucket, l.batch(bucket), true
	}
	l.writer.advanceSticky()
	next, err := l.writer.assignBucket(item)
	if err != nil {
		item.future.complete(WriteResult{Err: err})
		return 0, nil, false
	}
	return next, l.batch(next), true
}

func (l *appendWriterLoop) batch(bucket int32) *bucketBatch {
	batch := l.batches[bucket]
	if batch == nil {
		batch = &bucketBatch{}
		l.batches[bucket] = batch
	}
	return batch
}

func (l *appendWriterLoop) batchFull(batch *bucketBatch) bool {
	return len(batch.items) >= l.writer.config.MaxBatchRecords || batch.bytes >= l.writer.config.MaxBatchBytes
}

func (l *appendWriterLoop) batchFullWith(batch *bucketBatch, item *pendingWrite) bool {
	return len(batch.items) >= l.writer.config.MaxBatchRecords ||
		batch.bytes+item.size > l.writer.config.MaxBatchBytes
}

func (l *appendWriterLoop) flushBucket(bucket int32) {
	batch := l.batches[bucket]
	if batch == nil || len(batch.items) == 0 {
		return
	}
	delete(l.batches, bucket)
	if err := l.poisoned[bucket]; err != nil {
		poisoned := fmt.Errorf("%w: bucket %d: %w", ErrWriterState, bucket, err)
		l.writer.completeBatch(batch, bucket, 0, false, poisoned)
		return
	}
	l.pending[bucket] = append(l.pending[bucket], batch)
	l.dispatch()
}

func (l *appendWriterLoop) dispatch() {
	for l.inFlight < l.writer.config.MaxConcurrentRequests {
		dispatched := false
		for _, bucket := range l.writer.buckets {
			if l.active[bucket] || len(l.pending[bucket]) == 0 {
				continue
			}
			batch := popWriterBatch(l.pending, bucket)
			l.active[bucket] = true
			l.inFlight++
			sequence := l.sequences[bucket]
			go l.executeBatch(bucket, batch, sequence)
			dispatched = true
			if l.inFlight == l.writer.config.MaxConcurrentRequests {
				break
			}
		}
		if !dispatched {
			return
		}
	}
}

func (l *appendWriterLoop) executeBatch(bucket int32, batch *bucketBatch, sequence int32) {
	encoded, records, err := l.writer.encodeBatch(batch, sequence)
	var result writerAttemptResult
	started := metricStart(l.writer.observer)
	if err == nil {
		result = executeWriterRequest(
			l.writer.config.RequestTimeout, l.writer.config.RetryPolicy, l.writer.observer, MetricOperationLogWrite,
			func(ctx context.Context) (int64, bool, error) {
				return l.writer.backend.produce(ctx, logProduceRequest{
					path: l.writer.path, bucket: bucket, bucketCount: int32(len(l.writer.buckets)),
					tableID: l.writer.tableID, partitionID: l.writer.partitionID,
					records: encoded, timeout: l.writer.config.RequestTimeout, acks: l.writer.config.Acks,
				})
			},
		)
	} else {
		result.err = err
	}
	l.completions <- logBatchCompletion{
		bucket: bucket, batch: batch, baseOffset: result.offset, offsetKnown: result.offsetKnown,
		records: records, bytes: len(encoded), started: started, err: result.err,
	}
}

func (l *appendWriterLoop) handleCompletion(completion logBatchCompletion) {
	delete(l.active, completion.bucket)
	l.inFlight--
	queueTime := time.Duration(0)
	if len(completion.batch.items) != 0 && !completion.batch.items[0].queuedAt.IsZero() {
		queueTime = time.Since(completion.batch.items[0].queuedAt)
	}
	observeMetric(l.writer.observer, MetricEvent{
		Kind: MetricWriteBatch, Operation: MetricOperationLogWrite,
		Duration: metricDuration(completion.started), QueueTime: queueTime,
		QueueSize: len(l.writer.commands), Records: int64(completion.records), Bytes: int64(completion.bytes),
		Failed: completion.err != nil, ErrorClass: metricErrorClass(completion.err),
	})
	if completion.err != nil {
		l.poisoned[completion.bucket] = completion.err
		l.writer.completeBatch(completion.batch, completion.bucket, 0, false, completion.err)
		for _, batch := range l.pending[completion.bucket] {
			poisoned := fmt.Errorf(
				"%w: bucket %d: %w", ErrWriterState, completion.bucket, completion.err,
			)
			l.writer.completeBatch(batch, completion.bucket, 0, false, poisoned)
		}
		delete(l.pending, completion.bucket)
	} else {
		l.sequences[completion.bucket]++
		l.writer.completeBatch(
			completion.batch, completion.bucket, completion.baseOffset, completion.offsetKnown, nil,
		)
	}
	l.dispatch()
}

func (l *appendWriterLoop) flushAll() error {
	l.stopTimer()
	l.queueAll()
	for l.inFlight > 0 {
		l.handleCompletion(<-l.completions)
	}
	// One retained cause per failed bucket bounds history and preserves errors
	// processed before the barrier. Bucket order makes repeated results stable.
	var failures []error
	for _, bucket := range l.writer.buckets {
		if err := l.poisoned[bucket]; err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (l *appendWriterLoop) queueAll() {
	l.stopTimer()
	for _, bucket := range l.writer.buckets {
		l.flushBucket(bucket)
	}
}

func (l *appendWriterLoop) armTimer() {
	if l.timerC == nil && l.writer.config.BatchTimeout > 0 {
		l.timer.Reset(l.writer.config.BatchTimeout)
		l.timerC = l.timer.C
	}
}

func (l *appendWriterLoop) stopTimer() {
	if l.timerC != nil && !l.timer.Stop() {
		select {
		case <-l.timer.C:
		default:
		}
	}
	l.timerC = nil
}

func (w *AppendWriter) encodeBatch(batch *bucketBatch, sequence int32) ([]byte, int, error) {
	if len(batch.items) == 1 && batch.items[0].arrow != nil {
		item := batch.items[0]
		encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
			Magic: 0, BaseOffset: -1, SchemaID: int16(w.table.SchemaID), WriterID: w.writerID,
			BatchSequence: sequence, Record: item.arrow, Changes: item.changes,
		}, w.config.ArrowCompressionType, memory.DefaultAllocator)
		return encoded, len(item.changes), err
	}
	encoded, err := (LogBatch{
		Magic: 0, BaseOffset: -1, SchemaID: int16(w.table.SchemaID), AppendOnly: true,
		WriterID: w.writerID, BatchSequence: sequence,
	}).encodeRowRecords(len(batch.items), func(index int) ([]byte, ChangeType, error) {
		return batch.items[index].encoded, Append, nil
	})
	return encoded, len(batch.items), err
}

func (w *AppendWriter) completeBatch(
	batch *bucketBatch,
	bucket int32,
	baseOffset int64,
	offsetKnown bool,
	err error,
) {
	offset := baseOffset
	for _, item := range batch.items {
		count := 1
		if item.arrow != nil {
			count = len(item.changes)
		}
		item.future.complete(WriteResult{
			Bucket: bucket, BaseOffset: offset, OffsetKnown: offsetKnown,
			Records: count, Err: err,
		})
		if offsetKnown {
			offset += int64(count)
		}
	}
}

func (w *AppendWriter) effectiveNoKeyAssigner() NoKeyAssigner {
	if len(w.table.Schema.BucketKey) != 0 {
		return ""
	}
	if w.config.NoKeyAssigner == "" {
		return NoKeyAssignerSticky
	}
	return w.config.NoKeyAssigner
}

func (w *AppendWriter) assignBucket(item *pendingWrite) (int32, error) {
	if item.bucket != nil {
		return *item.bucket, nil
	}
	if len(w.table.Schema.BucketKey) != 0 {
		return flussBucket(item.bucketKey, len(w.buckets))
	}
	if w.effectiveNoKeyAssigner() == NoKeyAssignerRoundRobin {
		bucket := w.buckets[w.roundRobin%len(w.buckets)]
		w.roundRobin++
		return bucket, nil
	}
	if w.stickyIndex < 0 {
		w.stickyIndex = 0
	}
	return w.buckets[w.stickyIndex], nil
}

func (w *AppendWriter) encodeBucketKey(row Row) ([]byte, error) {
	if len(w.table.Schema.BucketKey) == 0 {
		return nil, nil
	}
	encoded := make([]byte, 0, 32)
	for _, position := range w.bucketKeyPositions {
		column := w.table.Schema.Columns[position]
		if row[position] == nil {
			return nil, fmt.Errorf("%w: invalid bucket key column %q", ErrInvalidRow, column.Name)
		}
		var err error
		encoded, err = appendKeyColumn(encoded, column, row[position])
		if err != nil {
			return nil, err
		}
	}
	if len(encoded) > maxRowBytes {
		return nil, fmt.Errorf("%w: key exceeds %d bytes", ErrMalformedRow, maxRowBytes)
	}
	return encoded, nil
}

func (w *AppendWriter) advanceSticky() {
	if len(w.buckets) > 1 {
		w.stickyIndex = (w.stickyIndex + 1) % len(w.buckets)
	}
}

func (w *AppendWriter) hasBucket(bucket int32) bool {
	for _, candidate := range w.buckets {
		if candidate == bucket {
			return true
		}
	}
	return false
}
