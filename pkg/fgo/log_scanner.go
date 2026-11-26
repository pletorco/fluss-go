package fgo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ScanOffsetKind identifies how a scanner resolves its initial position.
type ScanOffsetKind uint8

// Supported initial-position strategies for log scans.
const (
	ScanFromOffset ScanOffsetKind = iota
	ScanFromEarliest
	ScanFromLatest
	ScanFromTimestamp
)

// ScanOffset describes an explicit, symbolic, or timestamp-based start.
type ScanOffset struct {
	// Kind selects which of Offset or Timestamp is meaningful.
	Kind ScanOffsetKind
	// Offset is an inclusive non-negative start for [ScanFromOffset].
	Offset int64
	// Timestamp is used only for [ScanFromTimestamp].
	Timestamp time.Time
}

// AtOffset starts at an explicit inclusive log offset.
func AtOffset(offset int64) ScanOffset { return ScanOffset{Kind: ScanFromOffset, Offset: offset} }

// Earliest starts at the oldest available log offset.
func Earliest() ScanOffset { return ScanOffset{Kind: ScanFromEarliest} }

// Latest starts at the current log end.
func Latest() ScanOffset { return ScanOffset{Kind: ScanFromLatest} }

// AtTimestamp starts at the first offset whose timestamp is not before timestamp.
func AtTimestamp(timestamp time.Time) ScanOffset {
	return ScanOffset{Kind: ScanFromTimestamp, Timestamp: timestamp}
}

// Validate checks that exactly the fields required by Kind are set.
func (s ScanOffset) Validate() error {
	switch s.Kind {
	case ScanFromOffset:
		if s.Offset < 0 || !s.Timestamp.IsZero() {
			return fmt.Errorf("%w: invalid explicit scan offset", ErrInvalidConfig)
		}
	case ScanFromEarliest, ScanFromLatest:
		if s.Offset != 0 || !s.Timestamp.IsZero() {
			return fmt.Errorf("%w: symbolic scan offset has a value", ErrInvalidConfig)
		}
	case ScanFromTimestamp:
		if s.Timestamp.IsZero() || s.Offset != 0 {
			return fmt.Errorf("%w: scan timestamp is required", ErrInvalidConfig)
		}
	default:
		return fmt.Errorf("%w: unknown scan offset kind %d", ErrInvalidConfig, s.Kind)
	}
	return nil
}

// ErrWakeup reports that [LogScanner.Wakeup] interrupted a poll.
var ErrWakeup = errors.New("fgo: log scanner wakeup")

// ScanRecord associates one decoded row record with its source bucket.
type ScanRecord struct {
	// Bucket identifies the source table bucket.
	Bucket int32
	// Record contains the decoded row and log metadata.
	Record Record
}

// ScanArrowBatch associates one owned Arrow batch with its source bucket.
type ScanArrowBatch struct {
	// Bucket identifies the source table bucket.
	Bucket int32
	// Batch is owned by the enclosing [ScanResult].
	Batch ArrowLogBatch
}

// BucketScanError reports a bucket-local failure in a partial scan result.
type BucketScanError struct {
	// Bucket identifies the failed table bucket.
	Bucket int32
	// Err is the bucket-local fetch or decode failure.
	Err error
}

// ScanResult contains rows, owned Arrow batches, and per-bucket outcomes from
// one poll.
type ScanResult struct {
	// Records contains successfully decoded row records.
	Records []ScanRecord
	// ArrowBatches contains owned batches released by [ScanResult.Release].
	ArrowBatches []ScanArrowBatch
	// BucketErrors contains failures that did not invalidate other buckets.
	BucketErrors []BucketScanError
	// HighWatermark maps bucket IDs to the observed log end offset.
	HighWatermark map[int32]int64
	// Done reports that configured row or stopping-offset bounds were reached.
	Done bool
}

// Release frees Arrow records owned by the result.
// Release is safe to call more than once.
func (r *ScanResult) Release() {
	if r == nil {
		return
	}
	for index := range r.ArrowBatches {
		r.ArrowBatches[index].Batch.Release()
	}
	r.ArrowBatches = nil
}

// LogScanner polls ordered records from subscribed log buckets.
// Poll calls are serialized; Wakeup and Close may be called concurrently.
type LogScanner struct {
	table       Table
	path        PhysicalTablePath
	backend     logScannerBackend
	config      LogScannerConfig
	filter      *scanFilter
	tableID     int64
	partitionID int64
	buckets     []int32
	schema      Schema
	projection  []int32
	compacted   bool
	observer    MetricsObserver
	resolver    schemaResolver
	dynamic     bool

	pollGate    chan struct{}
	mu          sync.RWMutex
	offset      map[int32]int64
	subEpoch    map[int32]uint64
	delivered   int64
	done        bool
	closed      bool
	life        context.Context
	cancel      context.CancelFunc
	pollCancel  context.CancelFunc
	wakePending bool
}

// NewLogScanner creates a scanner subscribed to all current buckets at start.
// It returns [ErrNoBucketLeader] if the bucket layout has unavailable leaders.
func (c *Client) NewLogScanner(ctx context.Context, table Table, start ScanOffset, options ...LogScannerOption) (*LogScanner, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	scanner, err := newLogScanner(ctx, clientLogScannerBackend{client: c}, table, start, options...)
	if err == nil {
		scanner.observer = c.observer
	}
	return scanner, err
}

func newLogScanner(
	ctx context.Context,
	backend logScannerBackend,
	table Table,
	start ScanOffset,
	options ...LogScannerOption,
) (*LogScanner, error) {
	if err := table.Schema.Validate(); err != nil {
		return nil, err
	}
	if err := start.Validate(); err != nil {
		return nil, err
	}
	config, err := scannerConfig(options)
	if err != nil {
		return nil, err
	}
	filter, err := scannerFilter(table, config)
	if err != nil {
		return nil, err
	}
	path := PhysicalTablePath{TablePath: table.Path, Partition: config.Partition}
	layout, err := backend.metadata(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := layout.validateTableID(path, table.ID); err != nil {
		return nil, err
	}
	buckets, err := completeBucketIDs(layout.count, layout.leaders)
	if err != nil {
		return nil, fmt.Errorf("fgo: open log scanner for %s: %w", path, err)
	}
	scanner := &LogScanner{
		table: table, path: path, backend: backend, config: config, tableID: table.ID,
		partitionID: layout.partitionID, buckets: buckets, schema: table.Schema, offset: make(map[int32]int64, len(buckets)),
		subEpoch:  make(map[int32]uint64, len(buckets)),
		compacted: true, resolver: resolverFor(backend, table), filter: filter,
		pollGate: make(chan struct{}, 1),
	}
	_, scanner.dynamic = backend.(schemaResolverProvider)
	if provider, ok := backend.(schemaResolverProvider); ok && provider.schemaResolver() == nil {
		scanner.dynamic = false
	}
	if strings.EqualFold(strings.TrimSpace(table.Properties["table.log.format"]), string(LogFormatIndexed)) {
		scanner.compacted = false
	}
	scanner.life, scanner.cancel = context.WithCancel(context.Background()) // NOSONAR: Close owns the scanner lifecycle.
	if err := scanner.configureProjection(); err != nil {
		return nil, err
	}
	if err := scanner.initializeOffsets(ctx, start); err != nil {
		return nil, err
	}
	if err := scanner.validateStoppingOffsets(); err != nil {
		return nil, err
	}
	scanner.updateDone()
	return scanner, nil
}

func scannerFilter(table Table, config LogScannerConfig) (*scanFilter, error) {
	if config.Filter == nil {
		return nil, nil
	}
	if err := validateFilterLogFormat(table); err != nil {
		return nil, err
	}
	predicate, err := compilePredicate(*config.Filter, table.Schema)
	if err != nil {
		return nil, err
	}
	return &scanFilter{predicate: predicate, schemaID: table.SchemaID}, nil
}

func scannerConfig(options []LogScannerOption) (LogScannerConfig, error) {
	config := LogScannerConfig{
		FetchMaxBytes: 16 << 20, FetchMaxBytesForBucket: 1 << 20,
		FetchMinBytes: 1, FetchWaitMaxTime: 500 * time.Millisecond,
	}
	for _, option := range options {
		if option == nil {
			return LogScannerConfig{}, fmt.Errorf("%w: nil log scanner option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return LogScannerConfig{}, err
		}
	}
	return config, nil
}

func (s *LogScanner) configureProjection() error {
	if len(s.config.Projection) == 0 {
		return nil
	}
	if !s.compacted {
		return fmt.Errorf("%w: indexed log format does not support projection", ErrInvalidConfig)
	}
	schema, err := projectSchema(s.table.Schema, s.config.Projection)
	if err != nil {
		return err
	}
	positions := make(map[string]int32, len(s.table.Schema.Columns))
	for index, column := range s.table.Schema.Columns {
		positions[column.Name] = int32(index)
	}
	s.schema = schema
	s.projection = make([]int32, len(s.config.Projection))
	for index, name := range s.config.Projection {
		s.projection[index] = positions[name]
	}
	return nil
}

func (s *LogScanner) initializeOffsets(ctx context.Context, start ScanOffset) error {
	for _, bucket := range s.buckets {
		offset, err := s.resolveOffset(ctx, bucket, start)
		if err != nil {
			return fmt.Errorf("fgo: initialize bucket %d: %w", bucket, err)
		}
		s.offset[bucket] = offset
	}
	return nil
}

// Schema returns the result schema after projection.
func (s *LogScanner) Schema() Schema { return s.schema }

// Subscribe adds or resets one bucket at start.
func (s *LogScanner) Subscribe(ctx context.Context, bucket int32, start ScanOffset) error {
	if !s.hasBucket(bucket) {
		return fmt.Errorf("%w: %d", ErrUnknownBucket, bucket)
	}
	if err := start.Validate(); err != nil {
		return err
	}
	offset, err := s.resolveOffset(ctx, bucket, start)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.offset[bucket] = offset
	s.subEpoch[bucket]++
	s.updateDoneLocked()
	return nil
}

// Unsubscribe removes bucket from subsequent polls.
func (s *LogScanner) Unsubscribe(bucket int32) {
	s.mu.Lock()
	if _, subscribed := s.offset[bucket]; subscribed {
		s.subEpoch[bucket]++
	}
	delete(s.offset, bucket)
	s.updateDoneLocked()
	s.mu.Unlock()
}

// Poll waits for records, a terminal bound, wakeup, close, or ctx cancellation.
// Offsets and row-limit accounting advance only when Poll returns a result
// without an error; an interrupted poll leaves undelivered records available.
func (s *LogScanner) Poll(ctx context.Context) (ScanResult, error) {
	if ctx == nil {
		return ScanResult{}, fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	if err := acquirePoll(ctx, s.life, s.pollGate); err != nil {
		return ScanResult{}, err
	}
	defer func() { <-s.pollGate }()
	progress, requestCtx, cancel, err := s.beginPoll(ctx)
	if err != nil {
		return ScanResult{}, err
	}
	if cancel == nil {
		return ScanResult{HighWatermark: map[int32]int64{}, Done: true}, nil
	}
	defer s.endPoll(cancel)
	if len(progress.positions) == 0 {
		return ScanResult{}, fmt.Errorf("%w: scanner has no subscriptions", ErrInvalidConfig)
	}
	stop := context.AfterFunc(s.life, cancel)
	defer stop()
	result := ScanResult{HighWatermark: make(map[int32]int64, len(progress.positions))}
	for _, bucket := range s.buckets {
		position, subscribed := progress.positions[bucket]
		if !subscribed {
			continue
		}
		if stop, bounded := s.config.StoppingOffsets[bucket]; bounded && position.offset >= stop {
			continue
		}
		if err := s.pollBucket(requestCtx, ctx, bucket, progress, &result); err != nil {
			result.Release()
			return ScanResult{}, err
		}
		if s.config.RowLimit > 0 && progress.delivered >= s.config.RowLimit || s.Done() {
			break
		}
	}
	result.Done, err = s.commitPoll(ctx, progress)
	if err != nil {
		result.Release()
		return ScanResult{}, err
	}
	return result, nil
}

type logPollPosition struct {
	offset       int64
	next         int64
	subscription uint64
}

// logPollProgress stages positions and row accounting until Poll transfers its
// result to the caller. Discarding the result must not consume those records.
type logPollProgress struct {
	positions map[int32]logPollPosition
	delivered int64
}

func (s *LogScanner) beginPoll(
	ctx context.Context,
) (*logPollProgress, context.Context, context.CancelFunc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, nil, ErrClosed
	}
	if s.done {
		return nil, nil, nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if s.wakePending {
		s.wakePending = false
		return nil, nil, nil, ErrWakeup
	}
	progress := &logPollProgress{
		positions: make(map[int32]logPollPosition, len(s.offset)), delivered: s.delivered,
	}
	for bucket, offset := range s.offset {
		progress.positions[bucket] = logPollPosition{
			offset: offset, next: offset, subscription: s.subEpoch[bucket],
		}
	}
	requestCtx, cancel := context.WithCancel(ctx)
	s.pollCancel = cancel
	return progress, requestCtx, cancel, nil
}

func (s *LogScanner) endPoll(cancel context.CancelFunc) {
	cancel()
	s.mu.Lock()
	s.pollCancel = nil
	s.mu.Unlock()
}

func (s *LogScanner) pollBucket(
	requestCtx context.Context,
	callerCtx context.Context,
	bucket int32,
	progress *logPollProgress,
	result *ScanResult,
) error {
	position := progress.positions[bucket]
	offset := position.offset
	started := metricStart(s.observer)
	projection := s.projection
	if s.dynamic {
		projection = nil
	}
	fetched, err := s.backend.fetch(requestCtx, logFetchRequest{
		path: s.path, bucket: bucket, tableID: s.tableID, partitionID: s.partitionID,
		offset: offset, projection: projection, filter: s.filter, config: s.config,
	})
	fetchDuration := metricDuration(started)
	if err != nil {
		observeMetric(s.observer, MetricEvent{
			Kind: MetricScannerFetch, Operation: MetricOperationLogScan,
			Duration: fetchDuration, Failed: true, ErrorClass: metricErrorClass(err),
		})
		return s.recordFetchError(callerCtx, bucket, err, result)
	}
	target := s.table
	resolver := s.resolver
	if !s.dynamic && len(s.projection) != 0 {
		target.Schema = s.schema
		resolver = fixedSchemaResolver{
			path: target.Path, schemaID: target.SchemaID, schema: target.Schema,
		}
	}
	next, rows, arrows, err := decodeFetchedFetchResponseWithResolver(
		requestCtx, resolver, target, bucket, offset, fetched.records, s.compacted,
	)
	if err != nil {
		observeMetric(s.observer, MetricEvent{
			Kind: MetricDecodeFailure, Operation: MetricOperationLogScan,
			Bytes: int64(len(fetched.records)), Failed: true, ErrorClass: metricErrorClass(err),
		})
		result.BucketErrors = append(result.BucketErrors, BucketScanError{Bucket: bucket, Err: err})
		return nil
	}
	if fetched.filteredEndOffsetKnown {
		if fetched.filteredEndOffset < offset {
			releaseScanArrows(arrows)
			result.BucketErrors = append(result.BucketErrors, BucketScanError{
				Bucket: bucket,
				Err:    fmt.Errorf("%w: filtered end offset %d precedes fetch offset %d", ErrValidation, fetched.filteredEndOffset, offset),
			})
			return nil
		}
		if fetched.filteredEndOffset > next {
			next = fetched.filteredEndOffset
		}
	}
	if s.dynamic {
		rows = s.projectScanRows(rows)
		if err := s.projectScanArrows(arrows); err != nil {
			releaseScanArrows(arrows)
			return err
		}
	}
	rows, arrows, delivered, boundedNext := s.applyBounds(bucket, rows, arrows, next, progress.delivered)
	result.Records = append(result.Records, rows...)
	result.ArrowBatches = append(result.ArrowBatches, arrows...)
	result.HighWatermark[bucket] = fetched.highWatermark
	position.next = boundedNext
	progress.positions[bucket] = position
	progress.delivered += delivered
	lag := fetched.highWatermark - boundedNext
	if lag < 0 {
		lag = 0
	}
	observeMetric(s.observer, MetricEvent{
		Kind: MetricScannerFetch, Operation: MetricOperationLogScan,
		Duration: fetchDuration, Records: delivered, Bytes: int64(len(fetched.records)), Lag: lag,
	})
	return nil
}

func (s *LogScanner) recordFetchError(
	ctx context.Context,
	bucket int32,
	fetchErr error,
	result *ScanResult,
) error {
	s.mu.Lock()
	err := s.pollInterruptionLocked(ctx)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	result.BucketErrors = append(result.BucketErrors, BucketScanError{Bucket: bucket, Err: fetchErr})
	return nil
}

func (s *LogScanner) commitPoll(ctx context.Context, progress *logPollProgress) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.pollInterruptionLocked(ctx); err != nil {
		return false, err
	}
	for bucket, position := range progress.positions {
		if current, ok := s.offset[bucket]; ok && current == position.offset &&
			s.subEpoch[bucket] == position.subscription {
			s.offset[bucket] = position.next
		}
	}
	s.delivered = progress.delivered
	s.updateDoneLocked()
	return s.done, nil
}

func (s *LogScanner) pollInterruptionLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	if s.wakePending {
		s.wakePending = false
		return ErrWakeup
	}
	return nil
}

// Done reports whether a configured row or stopping-offset bound has completed.
func (s *LogScanner) Done() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.done
}

// Wakeup interrupts an active Poll, or the next Poll when none is active.
func (s *LogScanner) Wakeup() {
	s.mu.Lock()
	if !s.closed && !s.done {
		s.wakePending = true
		if s.pollCancel != nil {
			s.pollCancel()
		}
	}
	s.mu.Unlock()
}

// Close stops the scanner and interrupts an active poll.
// Close is idempotent.
func (s *LogScanner) Close() error {
	s.mu.Lock()
	s.closed = true
	s.offset = nil
	s.cancel()
	s.mu.Unlock()
	return nil
}

func (s *LogScanner) validateStoppingOffsets() error {
	if s.config.StoppingOffsets == nil {
		return nil
	}
	for _, bucket := range s.buckets {
		if _, ok := s.config.StoppingOffsets[bucket]; !ok {
			return fmt.Errorf("%w: stopping offset omitted bucket %d", ErrInvalidConfig, bucket)
		}
	}
	for bucket := range s.config.StoppingOffsets {
		if !s.hasBucket(bucket) {
			return fmt.Errorf("%w: stopping offset has unknown bucket %d", ErrInvalidConfig, bucket)
		}
	}
	return nil
}

func (s *LogScanner) updateDone() {
	s.mu.Lock()
	s.updateDoneLocked()
	s.mu.Unlock()
}

func (s *LogScanner) updateDoneLocked() {
	s.done = false
	if s.config.RowLimit > 0 && s.delivered >= s.config.RowLimit {
		s.done = true
		return
	}
	if s.config.StoppingOffsets == nil {
		return
	}
	if len(s.offset) == 0 {
		return
	}
	for bucket, offset := range s.offset {
		stop, ok := s.config.StoppingOffsets[bucket]
		if !ok || offset < stop {
			return
		}
	}
	s.done = true
}

func (s *LogScanner) resolveOffset(ctx context.Context, bucket int32, start ScanOffset) (int64, error) {
	if start.Kind == ScanFromOffset {
		return start.Offset, nil
	}
	return s.backend.listOffset(ctx, s.path, bucket, s.tableID, s.partitionID, start)
}

func (s *LogScanner) hasBucket(bucket int32) bool {
	for _, candidate := range s.buckets {
		if candidate == bucket {
			return true
		}
	}
	return false
}
