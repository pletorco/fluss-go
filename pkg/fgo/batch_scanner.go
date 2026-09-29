package fgo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

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
	scannerID  []byte
	sequence   int32

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
	scanner.life, scanner.cancel = context.WithCancel(context.Background()) // NOSONAR: Close owns the scanner lifecycle.
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
	if bucket.BucketCount <= 0 && table.BucketCount > 0 {
		bucket.BucketCount = int32(table.BucketCount)
	}
	scanner := &BatchScanner{
		table: table, bucket: bucket, config: config, backend: backend,
		snapshot: snapshot, projection: projection, resolver: resolverFor(backend, table),
	}
	scanner.life, scanner.cancel = context.WithCancel(context.Background()) // NOSONAR: Close owns the scanner lifecycle.
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
	config := BatchScannerConfig{Limit: 1024, BatchSizeBytes: 1 << 20}
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
	if s.table.Kind == PrimaryKeyTable {
		if backend, ok := s.backend.(kvSessionScanBackend); ok {
			return s.pollKVSession(pollCtx, backend)
		}
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

func (s *BatchScanner) pollKVSession(ctx context.Context, backend kvSessionScanBackend) (BatchResult, error) {
	batch, err := backend.scanKV(
		ctx, s.bucket, int64(s.config.Limit), s.config.BatchSizeBytes,
		s.scannerID, s.sequence, false,
	)
	if err != nil {
		return BatchResult{}, err
	}
	if len(batch.scannerID) == 0 {
		// Fluss 1.0 does not register a session for an empty bucket, so its
		// terminal open response intentionally has no scanner ID.
		if batch.hasMore || len(batch.records) != 0 || len(s.scannerID) != 0 {
			return BatchResult{}, fmt.Errorf("%w: KV scan response omitted scanner ID", ErrValidation)
		}
		s.markDone()
		return BatchResult{Done: true}, nil
	}
	if len(s.scannerID) != 0 && !bytes.Equal(s.scannerID, batch.scannerID) {
		return BatchResult{}, fmt.Errorf("%w: KV scan response changed scanner ID", ErrValidation)
	}
	s.scannerID = append(s.scannerID[:0], batch.scannerID...)
	s.sequence++
	var rows []Row
	if len(batch.records) != 0 {
		rows, err = decodeValueRecordBatchWithResolver(ctx, s.resolver, s.table, batch.records)
		if err != nil {
			return BatchResult{}, err
		}
	}
	result := BatchResult{Rows: s.projectRows(rows), Done: !batch.hasMore}
	if result.Done {
		s.markDone()
	}
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
	backend, ok := s.backend.(kvSessionScanBackend)
	if ok {
		s.pollMu.Lock()
		defer s.pollMu.Unlock()
		if len(s.scannerID) != 0 && !s.done {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := backend.scanKV(
				ctx, s.bucket, int64(s.config.Limit), s.config.BatchSizeBytes,
				s.scannerID, s.sequence, true,
			)
			return err
		}
	}
	return nil
}
