package fgo

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type batchScanBackendFunc func(context.Context, TableBucket, int32) (bool, []byte, error)

func (f batchScanBackendFunc) limitScan(
	ctx context.Context,
	bucket TableBucket,
	limit int32,
) (bool, []byte, error) {
	return f(ctx, bucket, limit)
}

func testTableBucket(table Table) TableBucket {
	return TableBucket{
		TableID: table.ID, PartitionID: -1, BucketID: 0,
		Leader: Node{ID: 1, Address: "tablet:9123", Role: TabletServer},
	}
}

func TestResolvedTableBucketsAreOrdered(t *testing.T) {
	buckets, err := resolvedTableBuckets(10, 20, map[int32]Node{
		2: {ID: 12, Address: "two:9123", Role: TabletServer},
		0: {ID: 10, Address: "zero:9123", Role: TabletServer},
	})
	if err != nil || len(buckets) != 2 ||
		buckets[0].BucketID != 0 || buckets[1].BucketID != 2 ||
		buckets[0].TableID != 10 || buckets[0].PartitionID != 20 {
		t.Fatalf("resolvedTableBuckets() = %#v, %v", buckets, err)
	}
	if _, err := resolvedTableBuckets(1, -1, nil); !errors.Is(err, ErrMetadata) {
		t.Fatalf("empty buckets error = %v", err)
	}
}

func TestBatchScannerReadsCurrentKVStateOnce(t *testing.T) {
	table := kvWriterTable()
	bucket := testTableBucket(table)
	encoded := encodedValueBatch(
		t, table,
		Row{int32(1), "one", int64(10)},
		Row{int32(2), "two", int64(20)},
	)
	var calls atomic.Int32
	backend := batchScanBackendFunc(func(_ context.Context, got TableBucket, limit int32) (bool, []byte, error) {
		calls.Add(1)
		if got != bucket || limit != 2 {
			t.Fatalf("limit scan = %#v limit=%d", got, limit)
		}
		return false, encoded, nil
	})
	scanner, err := newBatchScanner(
		context.Background(), backend, nil, table, bucket,
		WithBatchLimit(2), WithBatchProjection("name", "id"),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || len(result.Rows) != 2 ||
		result.Rows[0][0] != "one" || result.Rows[0][1] != int32(1) {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	repeated, err := scanner.Poll(context.Background())
	if err != nil || !repeated.Done || calls.Load() != 1 {
		t.Fatalf("repeated Poll() = %#v, %v calls=%d", repeated, err, calls.Load())
	}
	if !scanner.Done() {
		t.Fatal("scanner is not done")
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Poll after Close error = %v", err)
	}
}

func encodedValueBatch(t *testing.T, table Table, rows ...Row) []byte {
	t.Helper()
	encoded := make([]byte, 9)
	encoded[4] = 0
	binary.LittleEndian.PutUint32(encoded[5:], uint32(len(rows)))
	for _, row := range rows {
		value, err := EncodeCompactedRow(table.Schema, row)
		if err != nil {
			t.Fatal(err)
		}
		record := make([]byte, 6)
		binary.LittleEndian.PutUint32(record, uint32(2+len(value)))
		binary.LittleEndian.PutUint16(record[4:], uint16(table.SchemaID))
		encoded = append(encoded, record...)
		encoded = append(encoded, value...)
	}
	binary.LittleEndian.PutUint32(encoded, uint32(len(encoded)-4))
	return encoded
}

func TestBatchScannerReadsLatestLogRows(t *testing.T) {
	table := logWriterTable()
	bucket := testTableBucket(table)
	encoded, err := (LogBatch{
		Magic: 1, BaseOffset: 10, SchemaID: int16(table.SchemaID), AppendOnly: true,
		Records: []Record{
			{Change: Append, Value: Row{int32(1), "one"}},
			{Change: Append, Value: Row{int32(2), "two"}},
			{Change: Append, Value: Row{int32(3), "three"}},
		},
	}).EncodeRows(table.Schema, true)
	if err != nil {
		t.Fatal(err)
	}
	scanner, err := newBatchScanner(
		context.Background(),
		batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
			return true, encoded, nil
		}),
		nil, table, bucket, WithBatchLimit(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Rows) != 2 ||
		result.Rows[0][0] != int32(2) || result.Rows[1][0] != int32(3) {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	result.Release()
	_ = scanner.Close()
}

func TestBatchScannerCurrentFailures(t *testing.T) {
	table := kvWriterTable()
	bucket := testTableBucket(table)
	tests := []struct {
		name    string
		backend batchScanBackend
		target  error
	}{
		{"request", batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
			return false, nil, ErrTimeout
		}), ErrTimeout},
		{"kind", batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
			return true, nil, nil
		}), ErrValidation},
		{"malformed", batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
			return false, []byte{1}, nil
		}), ErrMalformedRecordBatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scanner, err := newBatchScanner(context.Background(), test.backend, nil, table, bucket)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := scanner.Poll(context.Background()); !errors.Is(err, test.target) {
				t.Fatalf("Poll() error = %v, want %v", err, test.target)
			}
			_ = scanner.Close()
		})
	}
	if _, err := (*BatchScanner)(nil).Poll(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil Poll() error = %v", err)
	}
}

type fakeSnapshotBatchReader struct {
	mu       sync.Mutex
	batches  [][]Row
	block    bool
	closed   atomic.Int32
	readErr  error
	closeErr error
}

func (r *fakeSnapshotBatchReader) ReadBatch(ctx context.Context, _ int) ([]Row, error) {
	if r.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readErr != nil {
		return nil, r.readErr
	}
	if len(r.batches) == 0 {
		return nil, io.EOF
	}
	rows := r.batches[0]
	r.batches = r.batches[1:]
	return rows, nil
}

func (r *fakeSnapshotBatchReader) Close() error {
	r.closed.Add(1)
	return r.closeErr
}

func TestSnapshotBatchScannerLifecycle(t *testing.T) {
	table := kvWriterTable()
	bucket := testTableBucket(table)
	reader := &fakeSnapshotBatchReader{batches: [][]Row{{
		{int32(1), "one", int64(10)},
	}}}
	var opened SnapshotBatchRequest
	client := &Client{snapshotProvider: SnapshotBatchProviderFunc(func(
		_ context.Context,
		request SnapshotBatchRequest,
	) (SnapshotBatchReader, error) {
		opened = request
		return reader, nil
	})}
	scanner, err := client.NewSnapshotBatchScanner(
		context.Background(), table, bucket, 42,
		WithBatchLimit(2), WithBatchProjection("id"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if opened.SnapshotID != 42 || opened.Limit != 2 || opened.Projection[0] != "id" {
		t.Fatalf("snapshot request = %#v", opened)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || result.Done || len(result.Rows) != 1 || result.Rows[0][0] != int32(1) {
		t.Fatalf("first Poll() = %#v, %v", result, err)
	}
	result, err = scanner.Poll(context.Background())
	if err != nil || !result.Done || !scanner.Done() {
		t.Fatalf("terminal Poll() = %#v, %v", result, err)
	}
	if err := scanner.Close(); err != nil || reader.closed.Load() != 1 {
		t.Fatalf("Close() = %v calls=%d", err, reader.closed.Load())
	}
	if err := scanner.Close(); err != nil || reader.closed.Load() != 1 {
		t.Fatalf("repeated Close() = %v calls=%d", err, reader.closed.Load())
	}
}

func TestSnapshotBatchScannerCloseUnblocksPoll(t *testing.T) {
	table := kvWriterTable()
	reader := &fakeSnapshotBatchReader{block: true}
	scanner, err := newBatchScanner(
		context.Background(), nil, reader, table, testTableBucket(table),
	)
	if err != nil {
		t.Fatal(err)
	}
	polled := make(chan error, 1)
	go func() {
		_, err := scanner.Poll(context.Background())
		polled <- err
	}()
	time.Sleep(10 * time.Millisecond)
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-polled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Poll() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Poll")
	}
}

func TestSnapshotBatchScannerRejectsInvalidProviderResults(t *testing.T) {
	table := kvWriterTable()
	reader := &fakeSnapshotBatchReader{batches: [][]Row{{
		{int32(1)}, {int32(2)},
	}}}
	scanner, err := newBatchScanner(
		context.Background(), nil, reader, table, testTableBucket(table), WithBatchLimit(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrValidation) {
		t.Fatalf("oversized provider error = %v", err)
	}
	_ = scanner.Close()

	client := &Client{}
	if _, err := client.NewSnapshotBatchScanner(
		context.Background(), table, testTableBucket(table), 1,
	); !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("missing provider error = %v", err)
	}
	client.snapshotProvider = SnapshotBatchProviderFunc(func(
		context.Context,
		SnapshotBatchRequest,
	) (SnapshotBatchReader, error) {
		return nil, ErrStorage
	})
	if _, err := client.NewSnapshotBatchScanner(
		context.Background(), table, testTableBucket(table), 1,
	); !errors.Is(err, ErrStorage) {
		t.Fatalf("provider open error = %v", err)
	}
	if _, err := client.NewSnapshotBatchScanner(
		context.Background(), logWriterTable(), testTableBucket(logWriterTable()), 1,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("log snapshot error = %v", err)
	}
	if _, err := client.NewSnapshotBatchScanner(
		context.Background(), table, testTableBucket(table), -1,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative snapshot error = %v", err)
	}
}

func TestBatchScannerConfigurationValidation(t *testing.T) {
	table := kvWriterTable()
	bucket := testTableBucket(table)
	backend := batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
		return false, nil, nil
	})
	tests := []func() error{
		func() error {
			_, err := newBatchScanner(context.Background(), backend, nil, table, bucket, WithBatchLimit(0))
			return err
		},
		func() error {
			_, err := newBatchScanner(context.Background(), backend, nil, table, bucket, WithBatchProjection())
			return err
		},
		func() error {
			_, err := newBatchScanner(context.Background(), backend, nil, table, bucket, nil)
			return err
		},
		func() error {
			bad := bucket
			bad.TableID++
			_, err := newBatchScanner(context.Background(), backend, nil, table, bad)
			return err
		},
		func() error {
			_, err := newBatchScanner(context.Background(), nil, nil, table, bucket)
			return err
		},
	}
	for index, run := range tests {
		if err := run(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("validation %d error = %v", index, err)
		}
	}
	var config config
	if err := WithSnapshotBatchProvider(nil)(&config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil provider error = %v", err)
	}
	if err := WithSnapshotBatchProvider(SnapshotBatchProviderFunc(func(
		context.Context,
		SnapshotBatchRequest,
	) (SnapshotBatchReader, error) {
		return nil, nil
	}))(&config); err != nil || config.snapshotProvider == nil {
		t.Fatalf("provider option = %#v, %v", config, err)
	}
	if !(*BatchScanner)(nil).Done() {
		t.Fatal("nil scanner must be done")
	}
	if err := (*BatchScanner)(nil).Close(); err != nil {
		t.Fatal(err)
	}
	(&BatchResult{}).Release()
	(*BatchResult)(nil).Release()

	client := &Client{}
	scanner, err := client.NewBatchScanner(context.Background(), table, bucket)
	if err != nil {
		t.Fatal(err)
	}
	_ = scanner.Close()
}

func TestDecodeValueRecordBatchRejectsSchemaAndTrailingBytes(t *testing.T) {
	table := kvWriterTable()
	wrongSchema := encodedValueBatch(t, table, Row{int32(1), "one", int64(1)})
	binary.LittleEndian.PutUint16(wrongSchema[13:], uint16(table.SchemaID+1))
	if _, err := decodeValueRecordBatch(table, wrongSchema); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("schema error = %v", err)
	}
	trailing := encodedValueBatch(t, table, Row{int32(1), "one", int64(1)})
	trailing = append(trailing, 0)
	binary.LittleEndian.PutUint32(trailing, uint32(len(trailing)-4))
	if _, err := decodeValueRecordBatch(table, trailing); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("trailing error = %v", err)
	}
}
