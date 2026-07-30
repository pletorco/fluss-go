package fgo

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
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
	Projection     []string
	Partition      string
	MaxBytes       int32
	MaxBucketBytes int32
	MinBytes       int32
	MaxWait        time.Duration
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
}

type logScannerBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error)
	listOffset(context.Context, PhysicalTablePath, int32, int64, int64, ScanOffset) (int64, error)
	fetch(context.Context, PhysicalTablePath, int32, int64, int64, int64, []int32, LogScannerConfig) (scannerFetch, error)
}

type clientLogScannerBackend struct{ client *Client }

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
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	offset int64,
	projection []int32,
	config LogScannerConfig,
) (scannerFetch, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyFetchLog, 0)
	if err != nil {
		return scannerFetch{}, err
	}
	message := request.Message().(*fmsg.FetchLogRequest)
	message.FollowerServerId = proto.Int32(-1)
	message.MaxBytes = proto.Int32(config.MaxBytes)
	message.MaxWaitMs = proto.Int32(int32(config.MaxWait / time.Millisecond))
	message.MinBytes = proto.Int32(config.MinBytes)
	bucketRequest := &fmsg.PbFetchLogReqForBucket{
		BucketId: proto.Int32(bucket), FetchOffset: proto.Int64(offset),
		MaxFetchBytes: proto.Int32(config.MaxBucketBytes),
	}
	if partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(partitionID)
	}
	message.TablesReq = []*fmsg.PbFetchLogReqForTable{{
		TableId: proto.Int64(tableID), ProjectionPushdownEnabled: proto.Bool(len(projection) != 0),
		ProjectedFields: projection, BucketsReq: []*fmsg.PbFetchLogReqForBucket{bucketRequest},
	}}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return scannerFetch{}, err
	}
	fetched, ok := response.Message().(*fmsg.FetchLogResponse)
	if !ok {
		return scannerFetch{}, fmt.Errorf("fgo: fetch log: unexpected response %T", response.Message())
	}
	if len(fetched.GetTablesResp()) != 1 || fetched.GetTablesResp()[0].GetTableId() != tableID ||
		len(fetched.GetTablesResp()[0].GetBucketsResp()) != 1 ||
		fetched.GetTablesResp()[0].GetBucketsResp()[0].GetBucketId() != bucket {
		return scannerFetch{}, fmt.Errorf("%w: fetch response omitted table or bucket", ErrValidation)
	}
	result := fetched.GetTablesResp()[0].GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyFetchLog); err != nil {
		return scannerFetch{}, err
	}
	return scannerFetch{
		records:       append([]byte(nil), result.GetRecords()...),
		highWatermark: result.GetHighWatermark(),
	}, nil
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

	pollMu sync.Mutex
	mu     sync.RWMutex
	offset map[int32]int64
	closed bool
	life   context.Context
	cancel context.CancelFunc
}

func (c *Client) NewLogScanner(ctx context.Context, table Table, start ScanOffset, options ...LogScannerOption) (*LogScanner, error) {
	return newLogScanner(ctx, clientLogScannerBackend{client: c}, table, start, options...)
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
	config := LogScannerConfig{
		MaxBytes: 16 << 20, MaxBucketBytes: 1 << 20, MinBytes: 1, MaxWait: 500 * time.Millisecond,
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil log scanner option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return nil, err
		}
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
	}
	scanner.life, scanner.cancel = context.WithCancel(context.Background())
	if path.Partition != "" {
		scanner.partitionID = physicalID
	}
	if len(config.Projection) != 0 {
		scanner.schema, err = projectSchema(table.Schema, config.Projection)
		if err != nil {
			return nil, err
		}
		positions := make(map[string]int32, len(table.Schema.Columns))
		for index, column := range table.Schema.Columns {
			positions[column.Name] = int32(index)
		}
		scanner.projection = make([]int32, len(config.Projection))
		for index, name := range config.Projection {
			scanner.projection[index] = positions[name]
		}
	}
	for _, bucket := range buckets {
		offset, err := scanner.resolveOffset(ctx, bucket, start)
		if err != nil {
			return nil, fmt.Errorf("fgo: initialize bucket %d: %w", bucket, err)
		}
		scanner.offset[bucket] = offset
	}
	return scanner, nil
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
	return nil
}

func (s *LogScanner) Unsubscribe(bucket int32) {
	s.mu.Lock()
	delete(s.offset, bucket)
	s.mu.Unlock()
}

func (s *LogScanner) Poll(ctx context.Context) (ScanResult, error) {
	if ctx == nil {
		return ScanResult{}, fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ScanResult{}, ErrClosed
	}
	offsets := make(map[int32]int64, len(s.offset))
	for bucket, offset := range s.offset {
		offsets[bucket] = offset
	}
	s.mu.RUnlock()
	if len(offsets) == 0 {
		return ScanResult{}, fmt.Errorf("%w: scanner has no subscriptions", ErrInvalidConfig)
	}
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.life, cancel)
	defer stop()
	result := ScanResult{HighWatermark: make(map[int32]int64, len(offsets))}
	for _, bucket := range s.buckets {
		offset, subscribed := offsets[bucket]
		if !subscribed {
			continue
		}
		fetched, err := s.backend.fetch(requestCtx, s.path, bucket, s.tableID, s.partitionID, offset, s.projection, s.config)
		if err != nil {
			if ctx.Err() != nil {
				result.Release()
				return ScanResult{}, ctx.Err()
			}
			s.mu.RLock()
			closed := s.closed
			s.mu.RUnlock()
			if closed {
				result.Release()
				return ScanResult{}, ErrClosed
			}
			result.BucketErrors = append(result.BucketErrors, BucketScanError{Bucket: bucket, Err: err})
			continue
		}
		next, rows, arrows, err := decodeFetchedLog(s.schema, bucket, offset, fetched.records)
		if err != nil {
			result.BucketErrors = append(result.BucketErrors, BucketScanError{Bucket: bucket, Err: err})
			continue
		}
		result.Records = append(result.Records, rows...)
		result.ArrowBatches = append(result.ArrowBatches, arrows...)
		result.HighWatermark[bucket] = fetched.highWatermark
		s.mu.Lock()
		if current, ok := s.offset[bucket]; ok && current == offset {
			s.offset[bucket] = next
		}
		s.mu.Unlock()
	}
	return result, nil
}

func (s *LogScanner) Close() error {
	s.mu.Lock()
	s.closed = true
	s.offset = nil
	s.cancel()
	s.mu.Unlock()
	return nil
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
	next := fetchOffset
	var rows []ScanRecord
	var arrows []ScanArrowBatch
	for len(encoded) != 0 {
		if len(encoded) < 12 {
			releaseScanArrows(arrows)
			return fetchOffset, nil, nil, fmt.Errorf("%w: truncated fetched batch", ErrMalformedRecordBatch)
		}
		size := 12 + int(binary.LittleEndian.Uint32(encoded[8:]))
		if size < logBatchV0HeaderSize || size > len(encoded) {
			releaseScanArrows(arrows)
			return fetchOffset, nil, nil, fmt.Errorf("%w: invalid fetched batch size", ErrMalformedRecordBatch)
		}
		payload := encoded[:size]
		batch, rowErr := DecodeLogBatchRows(schema, payload, true)
		if rowErr == nil {
			for _, record := range batch.Records {
				rows = append(rows, ScanRecord{Bucket: bucket, Record: record})
			}
			if len(batch.Records) != 0 {
				next = batch.BaseOffset + int64(len(batch.Records))
			}
		} else {
			arrowSchema, schemaErr := schema.ArrowSchema()
			if schemaErr != nil {
				releaseScanArrows(arrows)
				return fetchOffset, nil, nil, schemaErr
			}
			arrowBatch, arrowErr := DecodeArrowLogBatch(arrowSchema, payload, memory.DefaultAllocator)
			if arrowErr != nil {
				releaseScanArrows(arrows)
				return fetchOffset, nil, nil, errors.Join(rowErr, arrowErr)
			}
			if arrowBatch.Record.NumRows() != 0 {
				next = arrowBatch.BaseOffset + arrowBatch.Record.NumRows()
			}
			arrows = append(arrows, ScanArrowBatch{Bucket: bucket, Batch: *arrowBatch})
		}
		encoded = encoded[size:]
	}
	return next, rows, arrows, nil
}

func releaseScanArrows(batches []ScanArrowBatch) {
	for index := range batches {
		batches[index].Batch.Release()
	}
}
