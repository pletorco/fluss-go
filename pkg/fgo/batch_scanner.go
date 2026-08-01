package fgo

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// BatchScannerOption configures a bounded current-state or snapshot scan.
type BatchScannerOption func(*BatchScannerConfig) error

// BatchScannerConfig contains limits and projection for a bounded scan.
type BatchScannerConfig struct {
	// Limit is the maximum rows requested per poll.
	Limit int
	// Projection lists returned columns in result order; nil selects all.
	Projection []string
}

// WithBatchLimit bounds the number of rows returned by a current-state request or snapshot poll.
func WithBatchLimit(limit int) BatchScannerOption {
	return func(config *BatchScannerConfig) error {
		if limit <= 0 || limit > math.MaxInt32 {
			return fmt.Errorf("%w: invalid batch scan limit", ErrInvalidConfig)
		}
		config.Limit = limit
		return nil
	}
}

// WithBatchProjection selects result columns in the requested order.
func WithBatchProjection(columns ...string) BatchScannerOption {
	return func(config *BatchScannerConfig) error {
		if len(columns) == 0 {
			return fmt.Errorf("%w: batch projection is empty", ErrInvalidConfig)
		}
		config.Projection = append([]string(nil), columns...)
		return nil
	}
}

// BatchResult owns any Arrow batches it contains. Call Release after consuming the result.
type BatchResult struct {
	// Rows contains row-oriented results.
	Rows []Row
	// ArrowBatches contains owned Arrow results released by [BatchResult.Release].
	ArrowBatches []ArrowLogBatch
	// Done reports that no additional rows remain.
	Done bool
}

// Release frees Arrow records owned by the result.
// Release is safe to call more than once.
func (r *BatchResult) Release() {
	if r == nil {
		return
	}
	for index := range r.ArrowBatches {
		r.ArrowBatches[index].Release()
	}
	r.ArrowBatches = nil
}

// SnapshotBatchRequest identifies one immutable primary-key snapshot.
type SnapshotBatchRequest struct {
	// Table is the authoritative table metadata for the snapshot.
	Table Table
	// Bucket identifies the physical bucket being read.
	Bucket TableBucket
	// SnapshotID identifies the immutable server snapshot.
	SnapshotID int64
	// Projection lists requested columns in result order.
	Projection []string
	// Limit is the maximum rows requested from one reader call.
	Limit int
}

// SnapshotBatchReader streams rows from one immutable snapshot. io.EOF marks completion.
type SnapshotBatchReader interface {
	// ReadBatch returns at most limit rows. io.EOF marks completion and may
	// accompany a final non-empty batch.
	ReadBatch(context.Context, int) ([]Row, error)
	// Close releases reader resources and is safe after completion.
	Close() error
}

// SnapshotBatchProvider opens readers for implementation-specific Fluss snapshot storage.
type SnapshotBatchProvider interface {
	// OpenSnapshot returns a new reader owned by the caller.
	OpenSnapshot(context.Context, SnapshotBatchRequest) (SnapshotBatchReader, error)
}

// SnapshotBatchProviderFunc adapts a function to SnapshotBatchProvider.
type SnapshotBatchProviderFunc func(context.Context, SnapshotBatchRequest) (SnapshotBatchReader, error)

// OpenSnapshot calls f with the requested snapshot and projection.
func (f SnapshotBatchProviderFunc) OpenSnapshot(
	ctx context.Context,
	request SnapshotBatchRequest,
) (SnapshotBatchReader, error) {
	return f(ctx, request)
}

// WithSnapshotBatchProvider configures explicit snapshot-ID scans.
func WithSnapshotBatchProvider(provider SnapshotBatchProvider) Option {
	return func(config *config) error {
		if provider == nil {
			return fmt.Errorf("%w: nil snapshot batch provider", ErrInvalidConfig)
		}
		config.snapshotProvider = provider
		return nil
	}
}

type batchScanBackend interface {
	limitScan(context.Context, TableBucket, int32) (bool, []byte, error)
}

type clientBatchScanBackend struct{ client *Client }

func (b clientBatchScanBackend) schemaResolver() schemaResolver {
	if b.client == nil || b.client.schemas == nil {
		return nil
	}
	return b.client
}

// ResolveTableBuckets returns a stable bucket-ID ordered snapshot of current tablet leaders.
func (c *Client) ResolveTableBuckets(
	ctx context.Context,
	path PhysicalTablePath,
) ([]TableBucket, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if err := path.Validate(); err != nil {
		return nil, err
	}
	if path.Partition == "" {
		table, err := c.fetchTableMetadata(ctx, path.TablePath)
		if err != nil {
			return nil, err
		}
		return resolvedTableBuckets(table.ID, -1, table.Buckets)
	}
	partition, err := c.fetchPartitionMetadata(ctx, path)
	if err != nil {
		return nil, err
	}
	table, err := c.fetchTableMetadata(ctx, path.TablePath)
	if err != nil {
		return nil, err
	}
	return resolvedTableBuckets(table.ID, partition.ID, partition.Buckets)
}

func resolvedTableBuckets(
	tableID, partitionID int64,
	locations map[int32]Node,
) ([]TableBucket, error) {
	buckets, err := sortedBuckets(locations)
	if err != nil {
		return nil, err
	}
	result := make([]TableBucket, len(buckets))
	for index, bucket := range buckets {
		result[index] = TableBucket{
			TableID: tableID, PartitionID: partitionID, BucketID: bucket, Leader: locations[bucket],
		}
	}
	return result, nil
}

func (b clientBatchScanBackend) limitScan(
	ctx context.Context,
	bucket TableBucket,
	limit int32,
) (bool, []byte, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyLimitScan, 0)
	if err != nil {
		return false, nil, err
	}
	message := request.Message().(*fmsg.LimitScanRequest)
	message.TableId = proto.Int64(bucket.TableID)
	message.BucketId = proto.Int32(bucket.BucketID)
	message.Limit = proto.Int32(limit)
	if bucket.PartitionID >= 0 {
		message.PartitionId = proto.Int64(bucket.PartitionID)
	}
	response, err := b.client.RequestTo(ctx, bucket.Leader, request)
	if err != nil {
		return false, nil, err
	}
	scanned, ok := response.Message().(*fmsg.LimitScanResponse)
	if !ok {
		return false, nil, fmt.Errorf("fgo: limit scan: unexpected response %T", response.Message())
	}
	if err := responseServerError(
		scanned.GetErrorCode(), scanned.GetErrorMessage(), fmsg.APIKeyLimitScan,
	); err != nil {
		return false, nil, err
	}
	return scanned.GetIsLogTable(), append([]byte(nil), scanned.GetRecords()...), nil
}

// BatchScanner reads the bounded current state or one immutable snapshot of a
// table bucket. Poll calls are serialized; call Close to interrupt an active
// poll and release scanner resources.
type BatchScanner struct {
	table      Table
	bucket     TableBucket
	config     BatchScannerConfig
	backend    batchScanBackend
	snapshot   SnapshotBatchReader
	projection []int
	resolver   schemaResolver

	pollMu sync.Mutex
	mu     sync.RWMutex
	life   context.Context
	cancel context.CancelFunc
	done   bool
	closed bool
}

// NewBatchScanner scans the current state of one table bucket with Fluss LIMIT_SCAN.
func (c *Client) NewBatchScanner(
	ctx context.Context,
	table Table,
	bucket TableBucket,
	options ...BatchScannerOption,
) (*BatchScanner, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	return newBatchScanner(ctx, clientBatchScanBackend{client: c}, nil, table, bucket, options...)
}

// NewSnapshotBatchScanner scans one immutable primary-key snapshot through the configured provider.
func (c *Client) NewSnapshotBatchScanner(
	ctx context.Context,
	table Table,
	bucket TableBucket,
	snapshotID int64,
	options ...BatchScannerOption,
) (*BatchScanner, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if c.snapshotProvider == nil {
		return nil, fmt.Errorf("%w: snapshot batch provider is not configured", ErrUnsupportedAPI)
	}
	config, projection, err := batchScannerSettings(table, bucket, options)
	if err != nil {
		return nil, err
	}
	if snapshotID < 0 || table.Kind != PrimaryKeyTable {
		return nil, fmt.Errorf("%w: snapshot scans require a primary-key table and non-negative snapshot ID", ErrInvalidConfig)
	}
	reader, err := c.snapshotProvider.OpenSnapshot(ctx, SnapshotBatchRequest{
		Table: table, Bucket: bucket, SnapshotID: snapshotID,
		Projection: append([]string(nil), config.Projection...), Limit: config.Limit,
	})
	if err != nil {
		return nil, err
	}
	scanner := &BatchScanner{
		table: table, bucket: bucket, config: config, snapshot: reader, projection: projection,
	}
	scanner.life, scanner.cancel = context.WithCancel(context.Background())
	return scanner, nil
}

func newBatchScanner(
	ctx context.Context,
	backend batchScanBackend,
	snapshot SnapshotBatchReader,
	table Table,
	bucket TableBucket,
	options ...BatchScannerOption,
) (*BatchScanner, error) {
	config, projection, err := batchScannerSettings(table, bucket, options)
	if err != nil {
		return nil, err
	}
	if backend == nil && snapshot == nil {
		return nil, fmt.Errorf("%w: batch scan backend is required", ErrInvalidConfig)
	}
	scanner := &BatchScanner{
		table: table, bucket: bucket, config: config, backend: backend,
		snapshot: snapshot, projection: projection, resolver: resolverFor(backend, table),
	}
	scanner.life, scanner.cancel = context.WithCancel(context.Background())
	return scanner, nil
}

func batchScannerSettings(
	table Table,
	bucket TableBucket,
	options []BatchScannerOption,
) (BatchScannerConfig, []int, error) {
	if err := table.Schema.Validate(); err != nil {
		return BatchScannerConfig{}, nil, err
	}
	if err := bucket.Validate(); err != nil {
		return BatchScannerConfig{}, nil, err
	}
	if bucket.TableID != table.ID {
		return BatchScannerConfig{}, nil, fmt.Errorf(
			"%w: bucket table ID %d does not match table %d",
			ErrInvalidConfig, bucket.TableID, table.ID,
		)
	}
	config := BatchScannerConfig{Limit: 1024}
	for _, option := range options {
		if option == nil {
			return BatchScannerConfig{}, nil, fmt.Errorf("%w: nil batch scanner option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return BatchScannerConfig{}, nil, err
		}
	}
	projection, err := batchProjection(table.Schema, config.Projection)
	return config, projection, err
}

func batchProjection(schema Schema, names []string) ([]int, error) {
	if len(names) == 0 {
		return nil, nil
	}
	if _, err := projectSchema(schema, names); err != nil {
		return nil, err
	}
	positions := make(map[string]int, len(schema.Columns))
	for index, column := range schema.Columns {
		positions[column.Name] = index
	}
	result := make([]int, len(names))
	for index, name := range names {
		result[index] = positions[name]
	}
	return result, nil
}

// Poll returns available rows. A completed scanner keeps returning Done without further I/O.
func (s *BatchScanner) Poll(ctx context.Context) (BatchResult, error) {
	if s == nil {
		return BatchResult{}, fmt.Errorf("%w: nil batch scanner", ErrInvalidConfig)
	}
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return BatchResult{}, ErrClosed
	}
	if s.done {
		s.mu.RUnlock()
		return BatchResult{Done: true}, nil
	}
	life := s.life
	s.mu.RUnlock()
	pollCtx, cancel := batchPollContext(ctx, life)
	defer cancel()

	if s.snapshot != nil {
		return s.pollSnapshot(pollCtx)
	}
	isLog, encoded, err := s.backend.limitScan(pollCtx, s.bucket, int32(s.config.Limit))
	if err != nil {
		return BatchResult{}, err
	}
	result, err := s.decodeCurrent(pollCtx, isLog, encoded)
	if err != nil {
		return BatchResult{}, err
	}
	s.markDone()
	result.Done = true
	return result, nil
}

func batchPollContext(caller, life context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(caller)
	stop := context.AfterFunc(life, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (s *BatchScanner) pollSnapshot(ctx context.Context) (BatchResult, error) {
	rows, err := s.snapshot.ReadBatch(ctx, s.config.Limit)
	if errors.Is(err, io.EOF) {
		if len(rows) > s.config.Limit {
			return BatchResult{}, fmt.Errorf("%w: snapshot provider exceeded batch limit", ErrValidation)
		}
		s.markDone()
		return BatchResult{Rows: s.projectRows(rows), Done: true}, nil
	}
	if err != nil {
		return BatchResult{}, err
	}
	if len(rows) > s.config.Limit {
		return BatchResult{}, fmt.Errorf("%w: snapshot provider exceeded batch limit", ErrValidation)
	}
	return BatchResult{Rows: s.projectRows(rows)}, nil
}

func (s *BatchScanner) decodeCurrent(ctx context.Context, isLog bool, encoded []byte) (BatchResult, error) {
	if isLog != (s.table.Kind == LogTable) {
		return BatchResult{}, fmt.Errorf("%w: LIMIT_SCAN table kind mismatch", ErrValidation)
	}
	if len(encoded) == 0 {
		return BatchResult{}, nil
	}
	if !isLog {
		rows, err := decodeValueRecordBatchWithResolver(ctx, s.resolver, s.table, encoded)
		if err != nil {
			return BatchResult{}, err
		}
		if len(rows) > s.config.Limit {
			rows = rows[len(rows)-s.config.Limit:]
		}
		return BatchResult{Rows: s.projectRows(rows)}, nil
	}
	compacted := !strings.EqualFold(
		strings.TrimSpace(s.table.Properties["table.log.format"]),
		string(LogWriteFormatIndexed),
	)
	_, records, arrows, err := decodeFetchedLogWithResolver(
		ctx, s.resolver, s.table, s.bucket.BucketID, 0, encoded, compacted,
	)
	if err != nil {
		return BatchResult{}, err
	}
	records, arrows = limitLatestLogRecords(records, arrows, int64(s.config.Limit))
	if err := s.projectArrowBatches(arrows); err != nil {
		releaseScanArrows(arrows)
		return BatchResult{}, err
	}
	rows := make([]Row, len(records))
	for index := range records {
		rows[index] = records[index].Record.Value
	}
	result := BatchResult{Rows: s.projectRows(rows), ArrowBatches: make([]ArrowLogBatch, len(arrows))}
	for index := range arrows {
		result.ArrowBatches[index] = arrows[index].Batch
	}
	return result, nil
}

func limitLatestLogRecords(
	rows []ScanRecord,
	arrows []ScanArrowBatch,
	limit int64,
) ([]ScanRecord, []ScanArrowBatch) {
	skip := scanResultRows(rows, arrows) - limit
	if skip <= 0 {
		return rows, arrows
	}
	limitedRows := make([]ScanRecord, 0, len(rows))
	limitedArrows := make([]ScanArrowBatch, 0, len(arrows))
	for _, segment := range orderedScanSegments(rows, arrows) {
		if segment.row >= 0 {
			if skip > 0 {
				skip--
				continue
			}
			limitedRows = append(limitedRows, rows[segment.row])
			continue
		}
		item := &arrows[segment.arrow]
		count := item.Batch.Record.NumRows()
		if skip >= count {
			skip -= count
			item.Batch.Release()
			continue
		}
		if skip > 0 {
			sliced := sliceArrowLogBatch(&item.Batch, skip, count)
			item.Batch.Release()
			item.Batch = sliced
			skip = 0
		}
		limitedArrows = append(limitedArrows, *item)
	}
	return limitedRows, limitedArrows
}

func (s *BatchScanner) projectArrowBatches(batches []ScanArrowBatch) error {
	if len(s.projection) == 0 {
		return nil
	}
	projected, err := projectSchema(s.table.Schema, s.config.Projection)
	if err != nil {
		return err
	}
	schema, err := projected.ArrowSchema()
	if err != nil {
		return err
	}
	for index := range batches {
		source := batches[index].Batch.Record
		columns := make([]arrow.Array, len(s.projection))
		for columnIndex, position := range s.projection {
			if position < 0 || position >= int(source.NumCols()) {
				return fmt.Errorf("%w: projected Arrow column %d is unavailable", ErrInvalidSchema, position)
			}
			columns[columnIndex] = source.Column(position)
		}
		record := array.NewRecordBatch(schema, columns, source.NumRows())
		source.Release()
		batches[index].Batch.Record = record
	}
	return nil
}

func decodeValueRecordBatch(table Table, encoded []byte) ([]Row, error) {
	return decodeValueRecordBatchWithResolver(
		context.Background(),
		fixedSchemaResolver{path: table.Path, schemaID: table.SchemaID, schema: table.Schema},
		table,
		encoded,
	)
}

func decodeValueRecordBatchWithResolver(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	encoded []byte,
) ([]Row, error) {
	const headerSize = 9
	if len(encoded) < headerSize ||
		int(binary.LittleEndian.Uint32(encoded)) != len(encoded)-4 ||
		encoded[4] != 0 {
		return nil, fmt.Errorf("%w: invalid value batch header", ErrMalformedRecordBatch)
	}
	count := int(binary.LittleEndian.Uint32(encoded[5:]))
	if count < 0 || count > (len(encoded)-headerSize)/6 {
		return nil, fmt.Errorf("%w: invalid value record count", ErrMalformedRecordBatch)
	}
	rows := make([]Row, 0, count)
	position := headerSize
	for range count {
		row, next, err := decodeValueRecord(ctx, resolver, table, encoded, position)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		position = next
	}
	if position != len(encoded) {
		return nil, fmt.Errorf("%w: trailing value batch bytes", ErrMalformedRecordBatch)
	}
	return rows, nil
}

func decodeValueRecord(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	encoded []byte,
	position int,
) (Row, int, error) {
	if len(encoded)-position < 6 {
		return nil, position, fmt.Errorf("%w: truncated value record", ErrMalformedRecordBatch)
	}
	size := int(binary.LittleEndian.Uint32(encoded[position:]))
	position += 4
	if size < 2 || size > len(encoded)-position {
		return nil, position, fmt.Errorf("%w: invalid value record length", ErrMalformedRecordBatch)
	}
	schemaID := int32(int16(binary.LittleEndian.Uint16(encoded[position:])))
	schema, err := resolver.resolveSchema(ctx, table.Path, schemaID)
	if err != nil {
		return nil, position, fmt.Errorf("fgo: resolve value schema %d: %w", schemaID, err)
	}
	row, err := DecodeCompactedRow(schema, encoded[position+2:position+size])
	if err != nil {
		return nil, position, err
	}
	row, err = evolveRow(schema, table.Schema, row)
	return row, position + size, err
}

func (s *BatchScanner) projectRows(rows []Row) []Row {
	if len(s.projection) == 0 {
		return rows
	}
	projected := make([]Row, len(rows))
	for rowIndex, row := range rows {
		projected[rowIndex] = make(Row, len(s.projection))
		for columnIndex, position := range s.projection {
			projected[rowIndex][columnIndex] = row[position]
		}
	}
	return projected
}

func (s *BatchScanner) markDone() {
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()
}

// Done reports whether the scanner reached its configured bound.
func (s *BatchScanner) Done() bool {
	if s == nil {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.done
}

// Close stops the scanner and cancels an active poll.
// Close is idempotent.
func (s *BatchScanner) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	reader := s.snapshot
	s.mu.Unlock()
	if reader != nil {
		return reader.Close()
	}
	return nil
}
