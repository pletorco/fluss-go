package fgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
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
	table       Table
	path        PhysicalTablePath
	backend     appendWriterBackend
	config      AppendWriterConfig
	tableID     int64
	partitionID int64
	writerID    int64
	buckets     []int32
	commands    chan writerCommand
	slots       chan struct{}
	done        chan struct{}
	appendMu    sync.Mutex
	closed      bool
	closeErr    error
	roundRobin  int
	stickyIndex int
	observer    MetricsObserver
}

type writerCommand struct {
	item  *pendingWrite
	flush chan error
	close chan error
}

type pendingWrite struct {
	ctx      context.Context
	row      Row
	arrow    arrow.RecordBatch
	changes  []ChangeType
	bucket   *int32
	size     int
	queuedAt time.Time
	future   *WriteFuture
}

type bucketBatch struct {
	items   []*pendingWrite
	records []Record
	bytes   int
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

// NewAppendWriter creates an append writer for a log table.
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
	config, err := appendWriterConfig(options)
	if err != nil {
		return nil, err
	}
	if err := validateAppendWriterFormat(table, config); err != nil {
		return nil, err
	}
	path := PhysicalTablePath{TablePath: table.Path, Partition: config.Partition}
	if err := ensureAppendWriterPartition(ctx, backend, path, table.Schema.PartitionKey); err != nil {
		return nil, err
	}
	physicalID, locations, err := backend.metadata(ctx, path)
	if err != nil {
		return nil, err
	}
	if path.Partition == "" && physicalID != table.ID {
		return nil, fmt.Errorf("%w: metadata table ID %d does not match opened table %d", ErrMetadata, physicalID, table.ID)
	}
	buckets, err := sortedBuckets(locations)
	if err != nil {
		return nil, err
	}
	if table.BucketCount != 0 && table.BucketCount != len(buckets) {
		return nil, fmt.Errorf("%w: metadata has %d of %d buckets", ErrMetadata, len(buckets), table.BucketCount)
	}
	writerID, err := backend.initWriter(ctx, path, buckets[0])
	if err != nil {
		return nil, err
	}
	writer := &AppendWriter{
		table: table, path: path, backend: backend, config: config, tableID: table.ID, partitionID: -1,
		writerID: writerID, buckets: buckets, commands: make(chan writerCommand, config.MaxBuffered),
		slots: make(chan struct{}, config.MaxBuffered), done: make(chan struct{}), stickyIndex: -1,
	}
	if path.Partition != "" {
		writer.partitionID = physicalID
	}
	go writer.run()
	return writer, nil
}

func validateAppendWriterFormat(table Table, config AppendWriterConfig) error {
	if config.arrowCompressionSet &&
		config.Format != LogFormatAuto && config.Format != LogFormatArrow {
		return fmt.Errorf("%w: Arrow compression requires Arrow or auto format", ErrInvalidConfig)
	}
	configured := strings.TrimSpace(table.Properties["table.log.format"])
	if configured != "" && config.Format != LogFormatAuto &&
		!strings.EqualFold(configured, string(config.Format)) {
		return fmt.Errorf(
			"%w: writer format %s does not match table.log.format %s",
			ErrInvalidConfig, config.Format, configured,
		)
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
// failure. The writer copies row bytes while encoding, so the caller may reuse the row afterward.
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
	if err := w.table.Schema.ValidateRow(row, nil); err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	encoded, err := EncodeCompactedRow(w.table.Schema, row)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	item := &pendingWrite{
		ctx: ctx, row: append(Row(nil), row...), size: len(encoded) + 5, future: future,
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
	select {
	case w.slots <- struct{}{}:
		item.future.release = func() { <-w.slots }
	case <-ctx.Done():
		item.future.complete(WriteResult{Err: ctx.Err()})
		return
	case <-w.done:
		item.future.complete(WriteResult{Err: ErrClosed})
		return
	}
	w.appendMu.Lock()
	defer w.appendMu.Unlock()
	if w.closed {
		item.future.complete(WriteResult{Err: ErrClosed})
		return
	}
	select {
	case w.commands <- writerCommand{item: item}:
	case <-ctx.Done():
		item.future.complete(WriteResult{Err: ctx.Err()})
	case <-w.done:
		item.future.complete(WriteResult{Err: ErrClosed})
	}
}

// Flush waits until every previously accepted append reaches a terminal result.
func (w *AppendWriter) Flush(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	barrier := make(chan error, 1)
	w.appendMu.Lock()
	if w.closed {
		w.appendMu.Unlock()
		return ErrClosed
	}
	select {
	case w.commands <- writerCommand{flush: barrier}:
		w.appendMu.Unlock()
	case <-ctx.Done():
		w.appendMu.Unlock()
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
// Close is idempotent.
func (w *AppendWriter) Close(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	w.appendMu.Lock()
	if w.closed {
		w.appendMu.Unlock()
		select {
		case <-w.done:
			w.appendMu.Lock()
			err := w.closeErr
			w.appendMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.closed = true
	barrier := make(chan error, 1)
	select {
	case w.commands <- writerCommand{close: barrier}:
		w.appendMu.Unlock()
	case <-ctx.Done():
		w.closed = false
		w.appendMu.Unlock()
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
				l.writer.appendMu.Lock()
				l.writer.closeErr = err
				l.writer.appendMu.Unlock()
				command.close <- err
				l.timer.Stop()
				return
			}
		case <-l.timerC:
			l.timerC = nil
			_ = l.queueAll()
		case completion := <-l.completions:
			_ = l.handleCompletion(completion)
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
		_ = l.flushBucket(bucket)
		l.batches[bucket] = &bucketBatch{items: []*pendingWrite{item}}
		_ = l.flushBucket(bucket)
		return
	}
	bucket, batch, ok := l.availableBatch(bucket, item)
	if !ok {
		return
	}
	batch.items = append(batch.items, item)
	batch.records = append(batch.records, Record{Value: item.row, Change: Append, Offset: -1})
	batch.bytes += item.size
	if l.batchFull(batch) || l.writer.config.BatchTimeout == 0 {
		_ = l.flushBucket(bucket)
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
	_ = l.flushBucket(bucket)
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

func (l *appendWriterLoop) flushBucket(bucket int32) error {
	batch := l.batches[bucket]
	if batch == nil || len(batch.items) == 0 {
		return nil
	}
	delete(l.batches, bucket)
	if err := l.poisoned[bucket]; err != nil {
		poisoned := fmt.Errorf("%w: bucket %d: %v", ErrWriterState, bucket, err)
		l.writer.completeBatch(batch, bucket, 0, false, poisoned)
		return poisoned
	}
	l.pending[bucket] = append(l.pending[bucket], batch)
	l.dispatch()
	return nil
}

func (l *appendWriterLoop) dispatch() {
	for l.inFlight < l.writer.config.MaxConcurrentRequests {
		dispatched := false
		for _, bucket := range l.writer.buckets {
			if l.active[bucket] || len(l.pending[bucket]) == 0 {
				continue
			}
			batch := l.pending[bucket][0]
			l.pending[bucket] = l.pending[bucket][1:]
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
		requestCtx, cancel := context.WithTimeout(context.Background(), l.writer.config.RequestTimeout)
		result = executeWriterAttempts(
			requestCtx, l.writer.config.RetryPolicy, l.writer.observer, MetricOperationLogWrite,
			func(ctx context.Context) (int64, bool, error) {
				offset, err := l.writer.backend.produce(ctx, logProduceRequest{
					path: l.writer.path, bucket: bucket, tableID: l.writer.tableID, partitionID: l.writer.partitionID,
					records: encoded, timeout: l.writer.config.RequestTimeout, acks: l.writer.config.Acks,
				})
				return offset, err == nil, err
			},
		)
		if result.err != nil && requestCtx.Err() != nil {
			result.err = requestCtx.Err()
		}
		cancel()
	} else {
		result.err = err
	}
	l.completions <- logBatchCompletion{
		bucket: bucket, batch: batch, baseOffset: result.offset, offsetKnown: result.offsetKnown,
		records: records, bytes: len(encoded), started: started, err: result.err,
	}
}

func (l *appendWriterLoop) handleCompletion(completion logBatchCompletion) error {
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
				"%w: bucket %d: %v", ErrWriterState, completion.bucket, completion.err,
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
	return completion.err
}

func (l *appendWriterLoop) flushAll() error {
	l.stopTimer()
	err := l.queueAll()
	for l.inFlight > 0 {
		err = errors.Join(err, l.handleCompletion(<-l.completions))
	}
	return err
}

func (l *appendWriterLoop) queueAll() error {
	l.stopTimer()
	var joined error
	for _, bucket := range l.writer.buckets {
		joined = errors.Join(joined, l.flushBucket(bucket))
	}
	return joined
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
		WriterID: w.writerID, BatchSequence: sequence, Records: batch.records,
	}).EncodeRows(w.table.Schema, w.config.Format != LogFormatIndexed)
	return encoded, len(batch.records), err
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
		values, err := w.bucketKeyValues(item.row)
		if err != nil {
			return 0, err
		}
		key, err := encodeKeyColumns(w.table.Schema, w.table.Schema.BucketKey, values)
		if err != nil {
			return 0, err
		}
		bucket, err := flussBucket(key, len(w.buckets))
		if err != nil {
			return 0, err
		}
		return w.buckets[int(bucket)], nil
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

func (w *AppendWriter) bucketKeyValues(row Row) (PrimaryKey, error) {
	values := make(PrimaryKey, len(w.table.Schema.BucketKey))
	positions := make(map[string]int, len(w.table.Schema.Columns))
	for index, column := range w.table.Schema.Columns {
		positions[column.Name] = index
	}
	for index, name := range w.table.Schema.BucketKey {
		position, found := positions[name]
		if !found || row[position] == nil {
			return nil, fmt.Errorf("%w: invalid bucket key column %q", ErrInvalidRow, name)
		}
		values[index] = row[position]
	}
	return values, nil
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
