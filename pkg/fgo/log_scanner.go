package fgo

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
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

// LogScannerConfig controls projection, partition selection, fetch limits, and
// optional completion bounds.
type LogScannerConfig struct {
	// Projection lists returned columns in result order; nil selects all.
	Projection []string
	// Partition selects one named physical partition; empty selects the table.
	Partition string
	// FetchMaxBytes bounds aggregate encoded bytes per fetch.
	FetchMaxBytes int32
	// FetchMaxBytesForBucket bounds encoded bytes per bucket per fetch.
	FetchMaxBytesForBucket int32
	// FetchMinBytes is the aggregate byte threshold before a fetch may return.
	FetchMinBytes int32
	// FetchWaitMaxTime bounds server waiting when FetchMinBytes is not reached.
	FetchWaitMaxTime time.Duration
	// RowLimit is the total delivered row bound; zero is unbounded.
	RowLimit int64
	// StoppingOffsets maps every initial bucket to an exclusive end offset.
	StoppingOffsets map[int32]int64
}

// LogScannerOption configures a [LogScanner].
type LogScannerOption func(*LogScannerConfig) error

// WithScanProjection selects result columns in the requested order.
func WithScanProjection(columns ...string) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if len(columns) == 0 {
			return fmt.Errorf("%w: projection is empty", ErrInvalidConfig)
		}
		config.Projection = append([]string(nil), columns...)
		return nil
	}
}

// WithScanPartition scans the named physical partition.
func WithScanPartition(partition string) LogScannerOption {
	return func(config *LogScannerConfig) error {
		path := PhysicalTablePath{TablePath: TablePath{Database: "d", Table: "t"}, Partition: partition}
		if err := path.Validate(); err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

// WithScanPartitionSpec selects a partition using the table schema's partition-key order.
func WithScanPartitionSpec(schema Schema, spec PartitionSpec) LogScannerOption {
	return func(config *LogScannerConfig) error {
		partition, err := schema.PartitionName(spec)
		if err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

// WithLogFetchLimits sets aggregate and per-bucket fetch byte limits and wait time.
func WithLogFetchLimits(maxBytes, maxBytesForBucket, minBytes int32, waitMaxTime time.Duration) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if maxBytes <= 0 || maxBytesForBucket <= 0 || minBytes < 0 || minBytes > maxBytes ||
			waitMaxTime < 0 || waitMaxTime/time.Millisecond > math.MaxInt32 {
			return fmt.Errorf("%w: invalid scan limits", ErrInvalidConfig)
		}
		config.FetchMaxBytes, config.FetchMaxBytesForBucket = maxBytes, maxBytesForBucket
		config.FetchMinBytes, config.FetchWaitMaxTime = minBytes, waitMaxTime
		return nil
	}
}

// WithScanRowLimit completes a scanner after it has delivered limit rows across all buckets.
func WithScanRowLimit(limit int64) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if limit <= 0 {
			return fmt.Errorf("%w: scan row limit must be positive", ErrInvalidConfig)
		}
		config.RowLimit = limit
		return nil
	}
}

// WithScanStoppingOffsets sets the exclusive stopping offset for every initial bucket.
func WithScanStoppingOffsets(offsets map[int32]int64) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if len(offsets) == 0 {
			return fmt.Errorf("%w: stopping offsets are empty", ErrInvalidConfig)
		}
		config.StoppingOffsets = make(map[int32]int64, len(offsets))
		for bucket, offset := range offsets {
			if bucket < 0 || offset < 0 {
				return fmt.Errorf("%w: invalid stopping offset for bucket %d", ErrInvalidConfig, bucket)
			}
			config.StoppingOffsets[bucket] = offset
		}
		return nil
	}
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

type scannerFetch struct {
	records       []byte
	highWatermark int64
	remote        *RemoteLogFetchInfo
}

type logScannerBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]ServerNode, error)
	listOffset(context.Context, PhysicalTablePath, int32, int64, int64, ScanOffset) (int64, error)
	fetch(context.Context, logFetchRequest) (scannerFetch, error)
}

type clientLogScannerBackend struct{ client *Client }

func (b clientLogScannerBackend) schemaResolver() schemaResolver {
	if b.client == nil || b.client.schemas == nil {
		return nil
	}
	return b.client
}

type logFetchRequest struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	offset      int64
	projection  []int32
	config      LogScannerConfig
}

func (b clientLogScannerBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]ServerNode, error) {
	if path.Partition == "" {
		table, err := b.client.fetchTableMetadata(ctx, path.TablePath)
		return table.ID, table.Buckets, err
	}
	partition, err := b.client.fetchPartitionMetadata(ctx, path)
	return partition.ID, partition.Buckets, err
}

func (b clientLogScannerBackend) listOffset(
	ctx context.Context,
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	start ScanOffset,
) (int64, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyListOffsets, 0)
	if err != nil {
		return 0, err
	}
	message := request.Message().(*fmsg.ListOffsetsRequest)
	message.FollowerServerId = proto.Int32(-1)
	message.TableId = proto.Int64(tableID)
	message.BucketId = []int32{bucket}
	switch start.Kind {
	case ScanFromEarliest:
		message.OffsetType = proto.Int32(0)
	case ScanFromLatest:
		message.OffsetType = proto.Int32(1)
	case ScanFromTimestamp:
		message.OffsetType = proto.Int32(2)
		message.StartTimestamp = proto.Int64(start.Timestamp.UnixMilli())
	default:
		return 0, fmt.Errorf("%w: explicit offset does not require ListOffsets", ErrInvalidConfig)
	}
	if partitionID >= 0 {
		message.PartitionId = proto.Int64(partitionID)
	}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return 0, err
	}
	offsets, ok := response.Message().(*fmsg.ListOffsetsResponse)
	if !ok {
		return 0, fmt.Errorf("fgo: list offsets: unexpected response %T", response.Message())
	}
	if len(offsets.GetBucketsResp()) != 1 || offsets.GetBucketsResp()[0].GetBucketId() != bucket {
		return 0, fmt.Errorf("%w: list offsets omitted bucket %d", ErrValidation, bucket)
	}
	result := offsets.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyListOffsets); err != nil {
		return 0, err
	}
	return result.GetOffset(), nil
}

func (b clientLogScannerBackend) fetch(
	ctx context.Context,
	input logFetchRequest,
) (scannerFetch, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyFetchLog, 0)
	if err != nil {
		return scannerFetch{}, err
	}
	message := request.Message().(*fmsg.FetchLogRequest)
	message.FollowerServerId = proto.Int32(-1)
	message.MaxBytes = proto.Int32(input.config.FetchMaxBytes)
	message.MaxWaitMs = proto.Int32(int32(input.config.FetchWaitMaxTime / time.Millisecond))
	message.MinBytes = proto.Int32(input.config.FetchMinBytes)
	bucketRequest := &fmsg.PbFetchLogReqForBucket{
		BucketId: proto.Int32(input.bucket), FetchOffset: proto.Int64(input.offset),
		MaxFetchBytes: proto.Int32(input.config.FetchMaxBytesForBucket),
	}
	if input.partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(input.partitionID)
	}
	message.TablesReq = []*fmsg.PbFetchLogReqForTable{{
		TableId: proto.Int64(input.tableID), ProjectionPushdownEnabled: proto.Bool(len(input.projection) != 0),
		ProjectedFields: input.projection, BucketsReq: []*fmsg.PbFetchLogReqForBucket{bucketRequest},
	}}
	response, err := b.client.RequestBucket(ctx, input.path, input.bucket, request)
	if err != nil {
		return scannerFetch{}, err
	}
	fetched, ok := response.Message().(*fmsg.FetchLogResponse)
	if !ok {
		return scannerFetch{}, fmt.Errorf("fgo: fetch log: unexpected response %T", response.Message())
	}
	if len(fetched.GetTablesResp()) != 1 || fetched.GetTablesResp()[0].GetTableId() != input.tableID ||
		len(fetched.GetTablesResp()[0].GetBucketsResp()) != 1 ||
		fetched.GetTablesResp()[0].GetBucketsResp()[0].GetBucketId() != input.bucket {
		return scannerFetch{}, fmt.Errorf("%w: fetch response omitted table or bucket", ErrValidation)
	}
	result := fetched.GetTablesResp()[0].GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyFetchLog); err != nil {
		return scannerFetch{}, err
	}
	records := append([]byte(nil), result.GetRecords()...)
	remote := remoteLogFetchInfo(result.GetRemoteLogFetchInfo())
	if remote != nil {
		remoteRecords, err := b.client.readRemoteLog(ctx, remote)
		if err != nil {
			return scannerFetch{}, err
		}
		records = append(remoteRecords, records...)
	}
	return scannerFetch{records: records, highWatermark: result.GetHighWatermark(), remote: remote}, nil
}

func remoteLogFetchInfo(info *fmsg.PbRemoteLogFetchInfo) *RemoteLogFetchInfo {
	if info == nil {
		return nil
	}
	result := &RemoteLogFetchInfo{
		TabletDirectory:    info.GetRemoteLogTabletDir(),
		PartitionName:      info.GetPartitionName(),
		FirstStartPosition: int(info.GetFirstStartPos()),
		Segments:           make([]RemoteLogSegment, len(info.GetRemoteLogSegments())),
	}
	for index, segment := range info.GetRemoteLogSegments() {
		result.Segments[index] = RemoteLogSegment{
			ID:          segment.GetRemoteLogSegmentId(),
			StartOffset: segment.GetRemoteLogStartOffset(),
			EndOffset:   segment.GetRemoteLogEndOffset(),
			SizeBytes:   int64(segment.GetSegmentSizeInBytes()),
			MaxTime:     time.UnixMilli(segment.GetMaxTimestamp()),
		}
	}
	return result
}

// LogScanner polls ordered records from subscribed log buckets.
// Poll calls are serialized; Wakeup and Close may be called concurrently.
type LogScanner struct {
	table       Table
	path        PhysicalTablePath
	backend     logScannerBackend
	config      LogScannerConfig
	tableID     int64
	partitionID int64
	buckets     []int32
	schema      Schema
	projection  []int32
	compacted   bool
	observer    MetricsObserver
	resolver    schemaResolver
	dynamic     bool

	pollMu      sync.Mutex
	mu          sync.RWMutex
	offset      map[int32]int64
	delivered   int64
	done        bool
	closed      bool
	life        context.Context
	cancel      context.CancelFunc
	pollCancel  context.CancelFunc
	wakePending bool
}

// NewLogScanner creates a scanner subscribed to all current buckets at start.
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
	path := PhysicalTablePath{TablePath: table.Path, Partition: config.Partition}
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
	scanner := &LogScanner{
		table: table, path: path, backend: backend, config: config, tableID: table.ID,
		partitionID: -1, buckets: buckets, schema: table.Schema, offset: make(map[int32]int64, len(buckets)),
		compacted: true, resolver: resolverFor(backend, table),
	}
	_, scanner.dynamic = backend.(schemaResolverProvider)
	if provider, ok := backend.(schemaResolverProvider); ok && provider.schemaResolver() == nil {
		scanner.dynamic = false
	}
	if strings.EqualFold(strings.TrimSpace(table.Properties["table.log.format"]), string(LogFormatIndexed)) {
		scanner.compacted = false
	}
	scanner.life, scanner.cancel = context.WithCancel(context.Background())
	if path.Partition != "" {
		scanner.partitionID = physicalID
	}
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
	s.updateDoneLocked()
	return nil
}

// Unsubscribe removes bucket from subsequent polls.
func (s *LogScanner) Unsubscribe(bucket int32) {
	s.mu.Lock()
	delete(s.offset, bucket)
	s.updateDoneLocked()
	s.mu.Unlock()
}

// Poll waits for records, a terminal bound, wakeup, close, or ctx cancellation.
func (s *LogScanner) Poll(ctx context.Context) (ScanResult, error) {
	if ctx == nil {
		return ScanResult{}, fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	offsets, requestCtx, cancel, err := s.beginPoll(ctx)
	if err != nil {
		return ScanResult{}, err
	}
	if cancel == nil {
		return ScanResult{HighWatermark: map[int32]int64{}, Done: true}, nil
	}
	defer s.endPoll(cancel)
	if len(offsets) == 0 {
		return ScanResult{}, fmt.Errorf("%w: scanner has no subscriptions", ErrInvalidConfig)
	}
	stop := context.AfterFunc(s.life, cancel)
	defer stop()
	result := ScanResult{HighWatermark: make(map[int32]int64, len(offsets))}
	for _, bucket := range s.buckets {
		offset, subscribed := offsets[bucket]
		if !subscribed {
			continue
		}
		if stop, bounded := s.config.StoppingOffsets[bucket]; bounded && offset >= stop {
			continue
		}
		if err := s.pollBucket(requestCtx, ctx, bucket, offset, &result); err != nil {
			result.Release()
			return ScanResult{}, err
		}
		if s.Done() {
			break
		}
	}
	result.Done = s.Done()
	return result, nil
}

func (s *LogScanner) beginPoll(
	ctx context.Context,
) (map[int32]int64, context.Context, context.CancelFunc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, nil, ErrClosed
	}
	if s.done {
		return nil, nil, nil, nil
	}
	if s.wakePending {
		s.wakePending = false
		return nil, nil, nil, ErrWakeup
	}
	offsets := make(map[int32]int64, len(s.offset))
	for bucket, offset := range s.offset {
		offsets[bucket] = offset
	}
	requestCtx, cancel := context.WithCancel(ctx)
	s.pollCancel = cancel
	return offsets, requestCtx, cancel, nil
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
	offset int64,
	result *ScanResult,
) error {
	started := metricStart(s.observer)
	projection := s.projection
	if s.dynamic {
		projection = nil
	}
	fetched, err := s.backend.fetch(requestCtx, logFetchRequest{
		path: s.path, bucket: bucket, tableID: s.tableID, partitionID: s.partitionID,
		offset: offset, projection: projection, config: s.config,
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
	if s.dynamic {
		rows = s.projectScanRows(rows)
		if err := s.projectScanArrows(arrows); err != nil {
			releaseScanArrows(arrows)
			return err
		}
	}
	rows, arrows, delivered, boundedNext := s.applyBounds(bucket, rows, arrows, next)
	result.Records = append(result.Records, rows...)
	result.ArrowBatches = append(result.ArrowBatches, arrows...)
	result.HighWatermark[bucket] = fetched.highWatermark
	s.advanceOffset(bucket, offset, boundedNext, delivered)
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
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	closed := s.closed
	woken := s.wakePending
	if woken {
		s.wakePending = false
	}
	s.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if woken {
		return ErrWakeup
	}
	result.BucketErrors = append(result.BucketErrors, BucketScanError{Bucket: bucket, Err: fetchErr})
	return nil
}

func (s *LogScanner) advanceOffset(bucket int32, previous, next, delivered int64) {
	s.mu.Lock()
	if current, ok := s.offset[bucket]; ok && current == previous {
		s.offset[bucket] = next
	}
	s.delivered += delivered
	s.updateDoneLocked()
	s.mu.Unlock()
}

type scanSegment struct {
	offset int64
	row    int
	arrow  int
}

type boundedScan struct {
	bucket    int32
	stop      int64
	remaining int64
	delivered int64
	next      int64
	rows      []ScanRecord
	arrows    []ScanArrowBatch
}

func (s *LogScanner) applyBounds(
	bucket int32,
	rows []ScanRecord,
	arrows []ScanArrowBatch,
	decodedNext int64,
) ([]ScanRecord, []ScanArrowBatch, int64, int64) {
	if s.config.RowLimit == 0 && s.config.StoppingOffsets == nil {
		return rows, arrows, scanResultRows(rows, arrows), decodedNext
	}
	remaining, stop := s.scanBounds(bucket)
	segments := orderedScanSegments(rows, arrows)
	bounded := boundedScan{
		bucket: bucket, stop: stop, remaining: remaining, next: decodedNext,
		rows: make([]ScanRecord, 0, len(rows)), arrows: make([]ScanArrowBatch, 0, len(arrows)),
	}
	for _, segment := range segments {
		if segment.row >= 0 {
			bounded.appendRow(rows[segment.row])
			continue
		}
		bounded.appendArrow(segment, &arrows[segment.arrow])
	}
	if bounded.next > stop {
		bounded.next = stop
	}
	return bounded.rows, bounded.arrows, bounded.delivered, bounded.next
}

func scanResultRows(rows []ScanRecord, arrows []ScanArrowBatch) int64 {
	delivered := int64(len(rows))
	for index := range arrows {
		delivered += arrows[index].Batch.Record.NumRows()
	}
	return delivered
}

func (s *LogScanner) scanBounds(bucket int32) (int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	remaining := int64(math.MaxInt64)
	if s.config.RowLimit > 0 {
		remaining = s.config.RowLimit - s.delivered
	}
	stop := int64(math.MaxInt64)
	if configured, ok := s.config.StoppingOffsets[bucket]; ok {
		stop = configured
	}
	return remaining, stop
}

func orderedScanSegments(rows []ScanRecord, arrows []ScanArrowBatch) []scanSegment {
	segments := make([]scanSegment, 0, len(rows)+len(arrows))
	for index := range rows {
		segments = append(segments, scanSegment{offset: rows[index].Record.Offset, row: index, arrow: -1})
	}
	for index := range arrows {
		segments = append(segments, scanSegment{offset: arrows[index].Batch.BaseOffset, row: -1, arrow: index})
	}
	sort.SliceStable(segments, func(i, j int) bool { return segments[i].offset < segments[j].offset })
	return segments
}

func (b *boundedScan) appendRow(row ScanRecord) {
	if b.remaining <= 0 || row.Record.Offset >= b.stop {
		return
	}
	b.rows = append(b.rows, row)
	b.remaining--
	b.delivered++
	b.next = row.Record.Offset + 1
}

func (b *boundedScan) appendArrow(segment scanSegment, item *ScanArrowBatch) {
	batch := &item.Batch
	count := batch.Record.NumRows()
	allowed := count
	if b.stop-segment.offset < allowed {
		allowed = b.stop - segment.offset
	}
	if b.remaining < allowed {
		allowed = b.remaining
	}
	if allowed <= 0 {
		batch.Release()
		return
	}
	if allowed == count {
		b.arrows = append(b.arrows, *item)
	} else {
		b.arrows = append(b.arrows, ScanArrowBatch{
			Bucket: b.bucket,
			Batch:  sliceArrowLogBatch(batch, 0, allowed),
		})
		batch.Release()
	}
	b.remaining -= allowed
	b.delivered += allowed
	b.next = segment.offset + allowed
}

func sliceArrowLogBatch(batch *ArrowLogBatch, start, end int64) ArrowLogBatch {
	sliced := *batch
	sliced.BaseOffset += start
	sliced.Record = batch.Record.NewSlice(start, end)
	sliced.Changes = append([]ChangeType(nil), batch.Changes[start:end]...)
	sliced.owned = true
	sliced.release = &sync.Once{}
	return sliced
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

func decodeFetchedLog(
	schema Schema,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	return decodeFetchedLogFormat(schema, bucket, fetchOffset, encoded, true)
}

func decodeFetchedLogFormat(
	schema Schema,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
	compacted bool,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	table := Table{
		SchemaID: schemaIDFromFetched(encoded),
		Path:     TablePath{Database: "_", Table: "_"},
		Schema:   schema,
	}
	return decodeFetchedLogWithResolver(
		context.Background(),
		fixedSchemaResolver{path: table.Path, schemaID: table.SchemaID, schema: schema},
		table,
		bucket,
		fetchOffset,
		encoded,
		compacted,
	)
}

func decodeFetchedLogWithResolver(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
	compacted bool,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	return decodeFetchedLogBatchesWithResolver(
		ctx, resolver, table, bucket, fetchOffset, encoded,
		fetchedLogDecodeOptions{compacted: compacted},
	)
}

func decodeFetchedFetchResponseWithResolver(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
	compacted bool,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	return decodeFetchedLogBatchesWithResolver(
		ctx, resolver, table, bucket, fetchOffset, encoded,
		fetchedLogDecodeOptions{compacted: compacted, allowIncompleteTail: true},
	)
}

type fetchedLogDecodeOptions struct {
	compacted           bool
	allowIncompleteTail bool
}

func decodeFetchedLogBatchesWithResolver(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
	options fetchedLogDecodeOptions,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	next := fetchOffset
	var rows []ScanRecord
	var arrows []ScanArrowBatch
	for len(encoded) != 0 {
		size, complete, err := completeFetchedBatchSize(encoded)
		if err != nil {
			releaseScanArrows(arrows)
			return fetchOffset, nil, nil, err
		}
		if !complete {
			if options.allowIncompleteTail {
				// Byte-limited fetches may end inside the next batch. Refetch it from
				// the last complete offset, matching the Fluss Java scanner.
				break
			}
			releaseScanArrows(arrows)
			return fetchOffset, nil, nil, incompleteFetchedBatchError(encoded)
		}
		payload := encoded[:size]
		batchNext, batchRows, arrowBatch, err := decodeEvolvedFetchedBatch(
			ctx, resolver, table, bucket, next, payload, options.compacted,
		)
		if err != nil {
			releaseScanArrows(arrows)
			return fetchOffset, nil, nil, err
		}
		next = batchNext
		rows = append(rows, batchRows...)
		if arrowBatch != nil {
			arrows = append(arrows, *arrowBatch)
		}
		encoded = encoded[size:]
	}
	return next, rows, arrows, nil
}

func decodeEvolvedFetchedBatch(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	bucket int32,
	offset int64,
	payload []byte,
	compacted bool,
) (int64, []ScanRecord, *ScanArrowBatch, error) {
	_, _, schemaOffset, err := logBatchHeader(payload)
	if err != nil {
		return offset, nil, nil, err
	}
	schemaID := int32(int16(binary.LittleEndian.Uint16(payload[schemaOffset:])))
	writerSchema, err := resolver.resolveSchema(ctx, table.Path, schemaID)
	if err != nil {
		return offset, nil, nil, fmt.Errorf("fgo: resolve log schema %d: %w", schemaID, err)
	}
	next, rows, arrowBatch, err := decodeFetchedBatch(
		writerSchema, bucket, offset, payload, compacted,
	)
	if err != nil {
		return offset, nil, nil, err
	}
	if err := evolveScanRecords(rows, writerSchema, table.Schema); err != nil {
		return offset, nil, nil, err
	}
	if arrowBatch != nil {
		if err := evolveArrowBatch(&arrowBatch.Batch, writerSchema, table.Schema); err != nil {
			arrowBatch.Batch.Release()
			return offset, nil, nil, err
		}
	}
	return next, rows, arrowBatch, nil
}

func evolveScanRecords(records []ScanRecord, source, target Schema) error {
	for index := range records {
		row, err := evolveRow(source, target, records[index].Record.Value)
		if err != nil {
			return err
		}
		records[index].Record.Value = row
	}
	return nil
}

func schemaIDFromFetched(encoded []byte) int32 {
	if len(encoded) < logBatchV0HeaderSize {
		return 0
	}
	size, err := fetchedBatchSize(encoded)
	if err != nil {
		return 0
	}
	_, _, schemaOffset, err := logBatchHeader(encoded[:size])
	if err != nil {
		return 0
	}
	return int32(int16(binary.LittleEndian.Uint16(encoded[schemaOffset:])))
}

func evolveArrowBatch(batch *ArrowLogBatch, source, target Schema) error {
	mapping, err := schemaColumnMapping(source, target)
	if err != nil {
		return err
	}
	targetSchema, err := target.ArrowSchema()
	if err != nil {
		return err
	}
	columns := make([]arrow.Array, len(mapping))
	var temporary []arrow.Array
	for index, sourceIndex := range mapping {
		if sourceIndex < 0 {
			column := array.MakeArrayOfNull(
				memory.DefaultAllocator,
				targetSchema.Field(index).Type,
				int(batch.Record.NumRows()),
			)
			columns[index] = column
			temporary = append(temporary, column)
			continue
		}
		columns[index] = batch.Record.Column(sourceIndex)
	}
	record := array.NewRecordBatch(targetSchema, columns, batch.Record.NumRows())
	for _, column := range temporary {
		column.Release()
	}
	batch.Record.Release()
	batch.Record = record
	return nil
}

func (s *LogScanner) projectScanRows(rows []ScanRecord) []ScanRecord {
	if len(s.projection) == 0 {
		return rows
	}
	for index := range rows {
		projected := make(Row, len(s.projection))
		for columnIndex, position := range s.projection {
			projected[columnIndex] = rows[index].Record.Value[position]
		}
		rows[index].Record.Value = projected
	}
	return rows
}

func (s *LogScanner) projectScanArrows(batches []ScanArrowBatch) error {
	if len(s.projection) == 0 {
		return nil
	}
	schema, err := s.schema.ArrowSchema()
	if err != nil {
		return err
	}
	for index := range batches {
		source := batches[index].Batch.Record
		columns := make([]arrow.Array, len(s.projection))
		for columnIndex, position := range s.projection {
			if position < 0 || int(position) >= int(source.NumCols()) {
				return fmt.Errorf("%w: projected Arrow column %d is unavailable", ErrInvalidSchema, position)
			}
			columns[columnIndex] = source.Column(int(position))
		}
		record := array.NewRecordBatch(schema, columns, source.NumRows())
		source.Release()
		batches[index].Batch.Record = record
	}
	return nil
}

func fetchedBatchSize(encoded []byte) (int, error) {
	size, complete, err := completeFetchedBatchSize(encoded)
	if err != nil {
		return 0, err
	}
	if !complete {
		return 0, incompleteFetchedBatchError(encoded)
	}
	return size, nil
}

func completeFetchedBatchSize(encoded []byte) (int, bool, error) {
	if len(encoded) < 12 {
		return 0, false, nil
	}
	size := uint64(12) + uint64(binary.LittleEndian.Uint32(encoded[8:]))
	if size < uint64(logBatchV0HeaderSize) {
		return 0, false, fmt.Errorf("%w: invalid fetched batch size", ErrMalformedRecordBatch)
	}
	if size > uint64(len(encoded)) {
		return 0, false, nil
	}
	return int(size), true, nil
}

func incompleteFetchedBatchError(encoded []byte) error {
	if len(encoded) < 12 {
		return fmt.Errorf("%w: truncated fetched batch", ErrMalformedRecordBatch)
	}
	return fmt.Errorf("%w: invalid fetched batch size", ErrMalformedRecordBatch)
}

func decodeFetchedBatch(
	schema Schema,
	bucket int32,
	current int64,
	payload []byte,
	compacted bool,
) (int64, []ScanRecord, *ScanArrowBatch, error) {
	batch, rowErr := DecodeLogBatchRows(schema, payload, compacted)
	if rowErr == nil {
		start, next, err := fetchedBatchOffsets(batch.BaseOffset, int64(len(batch.Records)), current)
		if err != nil {
			return current, nil, nil, err
		}
		records := batch.Records[start:]
		rows := make([]ScanRecord, len(records))
		for index, record := range records {
			rows[index] = ScanRecord{Bucket: bucket, Record: record}
		}
		return next, rows, nil, nil
	}
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		return current, nil, nil, err
	}
	arrowBatch, err := DecodeArrowLogBatch(arrowSchema, payload, memory.DefaultAllocator)
	if err != nil {
		return current, nil, nil, errors.Join(rowErr, err)
	}
	count := arrowBatch.Record.NumRows()
	start, next, err := fetchedBatchOffsets(arrowBatch.BaseOffset, count, current)
	if err != nil {
		arrowBatch.Release()
		return current, nil, nil, err
	}
	if start == count {
		arrowBatch.Release()
		return next, nil, nil, nil
	}
	if start > 0 {
		sliced := sliceArrowLogBatch(arrowBatch, start, count)
		arrowBatch.Release()
		arrowBatch = &sliced
	}
	result := &ScanArrowBatch{Bucket: bucket, Batch: *arrowBatch}
	return next, nil, result, nil
}

func fetchedBatchOffsets(base, count, current int64) (int64, int64, error) {
	if count < 0 {
		return 0, current, fmt.Errorf("%w: negative log batch record count", ErrMalformedRecordBatch)
	}
	if count == 0 {
		return 0, current, nil
	}
	if base > math.MaxInt64-count {
		return 0, current, fmt.Errorf("%w: log batch offset overflow", ErrMalformedRecordBatch)
	}
	end := base + count
	start := int64(0)
	if current >= end {
		start = count
	} else if current > base {
		start = current - base
	}
	if end > current {
		current = end
	}
	return start, current, nil
}

func releaseScanArrows(batches []ScanArrowBatch) {
	for index := range batches {
		batches[index].Batch.Release()
	}
}
