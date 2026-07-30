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

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type ScanOffsetKind uint8

const (
	ScanFromOffset ScanOffsetKind = iota
	ScanFromEarliest
	ScanFromLatest
	ScanFromTimestamp
)

type ScanOffset struct {
	Kind      ScanOffsetKind
	Offset    int64
	Timestamp time.Time
}

func AtOffset(offset int64) ScanOffset { return ScanOffset{Kind: ScanFromOffset, Offset: offset} }
func Earliest() ScanOffset             { return ScanOffset{Kind: ScanFromEarliest} }
func Latest() ScanOffset               { return ScanOffset{Kind: ScanFromLatest} }
func AtTimestamp(timestamp time.Time) ScanOffset {
	return ScanOffset{Kind: ScanFromTimestamp, Timestamp: timestamp}
}

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

type LogScannerConfig struct {
	Projection      []string
	Partition       string
	MaxBytes        int32
	MaxBucketBytes  int32
	MinBytes        int32
	MaxWait         time.Duration
	RowLimit        int64
	StoppingOffsets map[int32]int64
}

type LogScannerOption func(*LogScannerConfig) error

func WithScanProjection(columns ...string) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if len(columns) == 0 {
			return fmt.Errorf("%w: projection is empty", ErrInvalidConfig)
		}
		config.Projection = append([]string(nil), columns...)
		return nil
	}
}

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

func WithScanLimits(maxBytes, maxBucketBytes, minBytes int32, maxWait time.Duration) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if maxBytes <= 0 || maxBucketBytes <= 0 || minBytes < 0 || minBytes > maxBytes ||
			maxWait < 0 || maxWait/time.Millisecond > math.MaxInt32 {
			return fmt.Errorf("%w: invalid scan limits", ErrInvalidConfig)
		}
		config.MaxBytes, config.MaxBucketBytes, config.MinBytes, config.MaxWait = maxBytes, maxBucketBytes, minBytes, maxWait
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

var ErrWakeup = errors.New("fgo: log scanner wakeup")

type ScanRecord struct {
	Bucket int32
	Record Record
}

type ScanArrowBatch struct {
	Bucket int32
	Batch  ArrowLogBatch
}

type BucketScanError struct {
	Bucket int32
	Err    error
}

type ScanResult struct {
	Records       []ScanRecord
	ArrowBatches  []ScanArrowBatch
	BucketErrors  []BucketScanError
	HighWatermark map[int32]int64
	Done          bool
}

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
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error)
	listOffset(context.Context, PhysicalTablePath, int32, int64, int64, ScanOffset) (int64, error)
	fetch(context.Context, logFetchRequest) (scannerFetch, error)
}

type clientLogScannerBackend struct{ client *Client }

type logFetchRequest struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	offset      int64
	projection  []int32
	config      LogScannerConfig
}

func (b clientLogScannerBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]Node, error) {
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
	message.MaxBytes = proto.Int32(input.config.MaxBytes)
	message.MaxWaitMs = proto.Int32(int32(input.config.MaxWait / time.Millisecond))
	message.MinBytes = proto.Int32(input.config.MinBytes)
	bucketRequest := &fmsg.PbFetchLogReqForBucket{
		BucketId: proto.Int32(input.bucket), FetchOffset: proto.Int64(input.offset),
		MaxFetchBytes: proto.Int32(input.config.MaxBucketBytes),
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

func (c *Client) NewLogScanner(ctx context.Context, table Table, start ScanOffset, options ...LogScannerOption) (*LogScanner, error) {
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
		compacted: true,
	}
	if strings.EqualFold(strings.TrimSpace(table.Properties["table.log.format"]), string(LogWriteFormatIndexed)) {
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
		MaxBytes: 16 << 20, MaxBucketBytes: 1 << 20, MinBytes: 1, MaxWait: 500 * time.Millisecond,
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

func (s *LogScanner) Schema() Schema { return s.schema }

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

func (s *LogScanner) Unsubscribe(bucket int32) {
	s.mu.Lock()
	delete(s.offset, bucket)
	s.updateDoneLocked()
	s.mu.Unlock()
}

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
	fetched, err := s.backend.fetch(requestCtx, logFetchRequest{
		path: s.path, bucket: bucket, tableID: s.tableID, partitionID: s.partitionID,
		offset: offset, projection: s.projection, config: s.config,
	})
	fetchDuration := metricDuration(started)
	if err != nil {
		observeMetric(s.observer, MetricEvent{
			Kind: MetricScannerFetch, Operation: MetricOperationLogScan,
			Duration: fetchDuration, Failed: true, ErrorClass: metricErrorClass(err),
		})
		return s.recordFetchError(callerCtx, bucket, err, result)
	}
	next, rows, arrows, err := decodeFetchedLogFormat(
		s.schema, bucket, offset, fetched.records, s.compacted,
	)
	if err != nil {
		observeMetric(s.observer, MetricEvent{
			Kind: MetricDecodeFailure, Operation: MetricOperationLogScan,
			Bytes: int64(len(fetched.records)), Failed: true, ErrorClass: metricErrorClass(err),
		})
		result.BucketErrors = append(result.BucketErrors, BucketScanError{Bucket: bucket, Err: err})
		return nil
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

func (s *LogScanner) applyBounds(
	bucket int32,
	rows []ScanRecord,
	arrows []ScanArrowBatch,
	decodedNext int64,
) ([]ScanRecord, []ScanArrowBatch, int64, int64) {
	if s.config.RowLimit == 0 && s.config.StoppingOffsets == nil {
		delivered := int64(len(rows))
		for index := range arrows {
			delivered += arrows[index].Batch.Record.NumRows()
		}
		return rows, arrows, delivered, decodedNext
	}
	s.mu.RLock()
	remaining := int64(math.MaxInt64)
	if s.config.RowLimit > 0 {
		remaining = s.config.RowLimit - s.delivered
	}
	stop := int64(math.MaxInt64)
	if configured, ok := s.config.StoppingOffsets[bucket]; ok {
		stop = configured
	}
	s.mu.RUnlock()

	segments := make([]scanSegment, 0, len(rows)+len(arrows))
	for index := range rows {
		segments = append(segments, scanSegment{offset: rows[index].Record.Offset, row: index, arrow: -1})
	}
	for index := range arrows {
		segments = append(segments, scanSegment{offset: arrows[index].Batch.BaseOffset, row: -1, arrow: index})
	}
	sort.SliceStable(segments, func(i, j int) bool { return segments[i].offset < segments[j].offset })

	boundedRows := make([]ScanRecord, 0, len(rows))
	boundedArrows := make([]ScanArrowBatch, 0, len(arrows))
	var delivered int64
	next := decodedNext
	for _, segment := range segments {
		if segment.row >= 0 {
			if remaining > 0 && segment.offset < stop {
				boundedRows = append(boundedRows, rows[segment.row])
				remaining--
				delivered++
				next = segment.offset + 1
			}
			continue
		}
		batch := &arrows[segment.arrow].Batch
		count := batch.Record.NumRows()
		allowed := count
		if stop-segment.offset < allowed {
			allowed = stop - segment.offset
		}
		if remaining < allowed {
			allowed = remaining
		}
		if allowed <= 0 {
			batch.Release()
			continue
		}
		if allowed == count {
			boundedArrows = append(boundedArrows, arrows[segment.arrow])
		} else {
			boundedArrows = append(boundedArrows, ScanArrowBatch{
				Bucket: bucket,
				Batch:  sliceArrowLogBatch(batch, allowed),
			})
			batch.Release()
		}
		remaining -= allowed
		delivered += allowed
		next = segment.offset + allowed
	}
	if next > stop {
		next = stop
	}
	return boundedRows, boundedArrows, delivered, next
}

func sliceArrowLogBatch(batch *ArrowLogBatch, rows int64) ArrowLogBatch {
	sliced := *batch
	sliced.Record = batch.Record.NewSlice(0, rows)
	sliced.Changes = append([]ChangeType(nil), batch.Changes[:rows]...)
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
	if s.config.RowLimit > 0 && s.delivered >= s.config.RowLimit {
		s.done = true
		return
	}
	if s.config.StoppingOffsets == nil {
		return
	}
	for bucket, stop := range s.config.StoppingOffsets {
		if s.offset[bucket] < stop {
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
	next := fetchOffset
	var rows []ScanRecord
	var arrows []ScanArrowBatch
	for len(encoded) != 0 {
		size, err := fetchedBatchSize(encoded)
		if err != nil {
			releaseScanArrows(arrows)
			return fetchOffset, nil, nil, err
		}
		payload := encoded[:size]
		batchNext, batchRows, arrowBatch, err := decodeFetchedBatch(
			schema, bucket, next, payload, compacted,
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

func fetchedBatchSize(encoded []byte) (int, error) {
	if len(encoded) < 12 {
		return 0, fmt.Errorf("%w: truncated fetched batch", ErrMalformedRecordBatch)
	}
	size := 12 + int(binary.LittleEndian.Uint32(encoded[8:]))
	if size < logBatchV0HeaderSize || size > len(encoded) {
		return 0, fmt.Errorf("%w: invalid fetched batch size", ErrMalformedRecordBatch)
	}
	return size, nil
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
		rows := make([]ScanRecord, len(batch.Records))
		for index, record := range batch.Records {
			rows[index] = ScanRecord{Bucket: bucket, Record: record}
		}
		if len(batch.Records) != 0 {
			current = batch.BaseOffset + int64(len(batch.Records))
		}
		return current, rows, nil, nil
	}
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		return current, nil, nil, err
	}
	arrowBatch, err := DecodeArrowLogBatch(arrowSchema, payload, memory.DefaultAllocator)
	if err != nil {
		return current, nil, nil, errors.Join(rowErr, err)
	}
	if arrowBatch.Record.NumRows() != 0 {
		current = arrowBatch.BaseOffset + arrowBatch.Record.NumRows()
	}
	result := &ScanArrowBatch{Bucket: bucket, Batch: *arrowBatch}
	return current, nil, result, nil
}

func releaseScanArrows(batches []ScanArrowBatch) {
	for index := range batches {
		batches[index].Batch.Release()
	}
}
