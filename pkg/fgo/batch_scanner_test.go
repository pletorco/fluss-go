package fgo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type batchScanBackendFunc func(context.Context, TableBucket, int32) (bool, []byte, error)

func (f batchScanBackendFunc) limitScan(
	ctx context.Context,
	bucket TableBucket,
	limit int32,
) (bool, []byte, error) {
	return f(ctx, bucket, limit)
}

type kvSessionCall struct {
	bucket       TableBucket
	limit        int64
	batchSize    int32
	scannerID    []byte
	sequence     int32
	closeScanner bool
}

type fakeKVSessionBackend struct {
	batches []kvSessionBatch
	calls   []kvSessionCall
}

func (*fakeKVSessionBackend) limitScan(context.Context, TableBucket, int32) (bool, []byte, error) {
	return false, nil, errors.New("unexpected LIMIT_SCAN")
}

func (b *fakeKVSessionBackend) scanKV(
	_ context.Context,
	bucket TableBucket,
	limit int64,
	batchSize int32,
	scannerID []byte,
	sequence int32,
	closeScanner bool,
) (kvSessionBatch, error) {
	b.calls = append(b.calls, kvSessionCall{
		bucket: bucket, limit: limit, batchSize: batchSize,
		scannerID: append([]byte(nil), scannerID...), sequence: sequence, closeScanner: closeScanner,
	})
	if closeScanner {
		return kvSessionBatch{scannerID: append([]byte(nil), scannerID...)}, nil
	}
	batch := b.batches[0]
	b.batches = b.batches[1:]
	return batch, nil
}

func testTableBucket(table Table) TableBucket {
	return TableBucket{
		TableID: table.ID, PartitionID: -1, BucketID: 0, BucketCount: int32(table.BucketCount),
		Leader: ServerNode{ID: 1, Address: "tablet:9123", ServerType: TabletServer},
	}
}

func TestResolvedTableBucketsAreOrdered(t *testing.T) {
	buckets, err := resolvedTableBuckets(10, 20, 3, map[int32]ServerNode{
		2: {ID: 12, Address: "two:9123", ServerType: TabletServer},
		0: {ID: 10, Address: "zero:9123", ServerType: TabletServer},
	})
	if err != nil || len(buckets) != 2 ||
		buckets[0].BucketID != 0 || buckets[1].BucketID != 2 ||
		buckets[0].TableID != 10 || buckets[0].PartitionID != 20 ||
		buckets[0].BucketCount != 3 || buckets[1].BucketCount != 3 {
		t.Fatalf("resolvedTableBuckets() = %#v, %v", buckets, err)
	}
	if _, err := resolvedTableBuckets(1, -1, 0, nil); !errors.Is(err, ErrMetadata) {
		t.Fatalf("empty buckets error = %v", err)
	}
}

func TestClientResolvesTableAndPartitionBuckets(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		*response.Message().(*fmsg.MetadataResponse) = *metadataResponse(path)
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetMetadata] = 0

	tableBuckets, err := client.ResolveTableBuckets(
		context.Background(), PhysicalTablePath{TablePath: path},
	)
	if err != nil || len(tableBuckets) != 1 || tableBuckets[0].TableID != 9 ||
		tableBuckets[0].PartitionID != -1 || tableBuckets[0].BucketID != 0 {
		t.Fatalf("table buckets = %#v, %v", tableBuckets, err)
	}
	partitionBuckets, err := client.ResolveTableBuckets(context.Background(), PhysicalTablePath{
		TablePath: path, Partition: "day=2026-07-30",
	})
	if err != nil || len(partitionBuckets) != 1 || partitionBuckets[0].TableID != 9 ||
		partitionBuckets[0].PartitionID != 10 || partitionBuckets[0].BucketID != 1 {
		t.Fatalf("partition buckets = %#v, %v", partitionBuckets, err)
	}
	if _, err := client.ResolveTableBuckets(context.Background(), PhysicalTablePath{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid path error = %v", err)
	}
}

func TestClientBatchScanBackendResponses(t *testing.T) {
	node := ServerNode{ID: 2, Address: "tablet:9123", ServerType: TabletServer}
	bucket := TableBucket{TableID: 9, PartitionID: 10, BucketID: 1, Leader: node}
	requestErr := errors.New("limit scan request failed")

	for _, test := range []struct {
		name      string
		requester requesterFunc
		target    error
	}{
		{name: "request error", target: requestErr, requester: func(context.Context, fmsg.Request) (fmsg.Response, error) {
			return nil, requestErr
		}},
		{name: "unexpected response", target: ErrValidation, requester: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			return fmsg.NewResponse(fmsg.APIKeyLookup, request.Version())
		}},
		{name: "server error", target: ErrStorage, requester: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			message := response.Message().(*fmsg.LimitScanResponse)
			message.ErrorCode = proto.Int32(int32(fmsg.ErrorCodeStorageException))
			return response, nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := batchBackendClient(node, test.requester)
			if _, _, err := (clientBatchScanBackend{client: client}).limitScan(
				context.Background(), bucket, 3,
			); err == nil || test.target == ErrValidation && !strings.Contains(err.Error(), "unexpected response") ||
				test.target != ErrValidation && !errors.Is(err, test.target) {
				t.Fatalf("limitScan() error = %v, want %v", err, test.target)
			}
		})
	}

	records := []byte{1, 2, 3}
	client := batchBackendClient(node, func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		message := request.(*fmsg.MessageRequest).Message().(*fmsg.LimitScanRequest)
		if message.GetTableId() != 9 || message.GetPartitionId() != 10 ||
			message.GetBucketId() != 1 || message.GetLimit() != 3 {
			t.Fatalf("limit scan request = %#v", message)
		}
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		scanned := response.Message().(*fmsg.LimitScanResponse)
		scanned.IsLogTable = proto.Bool(true)
		scanned.Records = records
		return response, nil
	})
	isLog, got, err := (clientBatchScanBackend{client: client}).limitScan(
		context.Background(), bucket, 3,
	)
	records[0] = 9
	if err != nil || !isLog || !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("limitScan() = %t, %v, %v", isLog, got, err)
	}
}

func batchBackendClient(node ServerNode, requester requesterFunc) *Client {
	tablet := newClient(requester, nil)
	tablet.versions[fmsg.APIKeyLimitScan] = 0
	tablet.versions[fmsg.APIKeyScanKv] = 0
	manager := newConnectionManager(config{})
	manager.clients[connectionKey{id: node.ID, address: node.Address, serverType: node.ServerType}] = tablet
	return &Client{manager: manager}
}

func TestClientBatchBackendBuildsScanKVRequests(t *testing.T) {
	table := upsertWriterTable()
	bucket := testTableBucket(table)
	bucket.BucketCount = 4
	var got *fmsg.ScanKvRequest
	client := batchBackendClient(bucket.Leader, requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		got = request.(*fmsg.MessageRequest).Message().(*fmsg.ScanKvRequest)
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		message := response.Message().(*fmsg.ScanKvResponse)
		message.ScannerId, message.HasMoreResults = []byte("scan"), proto.Bool(true)
		return response, nil
	}))
	result, err := (clientBatchScanBackend{client: client}).scanKV(
		context.Background(), bucket, 25, 4096, nil, 0, false,
	)
	if err != nil || string(result.scannerID) != "scan" || !result.hasMore {
		t.Fatalf("scanKV() = %#v, %v", result, err)
	}
	if got.GetBucketScanReq().GetTableId() != table.ID || got.GetBucketScanReq().GetBucketId() != bucket.BucketID ||
		got.GetBucketScanReq().GetRoutingBucketCount() != 4 || got.GetBucketScanReq().GetLimit() != 25 ||
		got.GetBatchSizeBytes() != 4096 || got.GetCallSeqId() != 0 {
		t.Fatalf("ScanKv request = %#v", got)
	}
	bucket.BucketCount = 0
	if _, err := (clientBatchScanBackend{client: client}).scanKV(
		context.Background(), bucket, 25, 4096, nil, 0, false,
	); !errors.Is(err, ErrMetadata) {
		t.Fatalf("scanKV() missing bucket count error = %v", err)
	}
}

func TestBatchScannerReadsCurrentKVStateOnce(t *testing.T) {
	table := upsertWriterTable()
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

func TestBatchScannerUsesFluss100KVSession(t *testing.T) {
	table := upsertWriterTable()
	bucket := testTableBucket(table)
	bucket.BucketCount = 3
	backend := &fakeKVSessionBackend{batches: []kvSessionBatch{
		{scannerID: []byte("scanner"), hasMore: true, records: encodedValueBatch(t, table, Row{int32(1), "one", int64(10)})},
		{scannerID: []byte("scanner"), records: encodedValueBatch(t, table, Row{int32(2), "two", int64(20)})},
	}}
	scanner, err := newBatchScanner(
		context.Background(), backend, nil, table, bucket,
		WithBatchLimit(2), WithBatchSizeBytes(4096),
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := scanner.Poll(context.Background())
	if err != nil || first.Done || len(first.Rows) != 1 {
		t.Fatalf("first Poll() = %#v, %v", first, err)
	}
	second, err := scanner.Poll(context.Background())
	if err != nil || !second.Done || len(second.Rows) != 1 {
		t.Fatalf("second Poll() = %#v, %v", second, err)
	}
	if len(backend.calls) != 2 || backend.calls[0].limit != 2 || backend.calls[0].batchSize != 4096 ||
		backend.calls[0].sequence != 0 || len(backend.calls[0].scannerID) != 0 ||
		backend.calls[0].bucket.BucketCount != 3 || backend.calls[1].sequence != 1 ||
		string(backend.calls[1].scannerID) != "scanner" {
		t.Fatalf("scan calls = %#v", backend.calls)
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}

	backend = &fakeKVSessionBackend{batches: []kvSessionBatch{{scannerID: []byte("open"), hasMore: true}}}
	scanner, err = newBatchScanner(context.Background(), backend, nil, table, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scanner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if len(backend.calls) != 2 || !backend.calls[1].closeScanner || backend.calls[1].sequence != 1 {
		t.Fatalf("close calls = %#v", backend.calls)
	}
}

func TestBatchScannerRejectsInvalidKVSessionIdentity(t *testing.T) {
	table := upsertWriterTable()
	bucket := testTableBucket(table)
	for _, test := range []struct {
		name    string
		batches []kvSessionBatch
		polls   int
	}{
		{name: "missing scanner ID", batches: []kvSessionBatch{{hasMore: true}}, polls: 1},
		{
			name: "changed scanner ID",
			batches: []kvSessionBatch{
				{scannerID: []byte("first"), hasMore: true},
				{scannerID: []byte("second")},
			},
			polls: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeKVSessionBackend{batches: test.batches}
			scanner, err := newBatchScanner(context.Background(), backend, nil, table, bucket)
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			for index := 0; index < test.polls-1; index++ {
				if _, err := scanner.Poll(context.Background()); err != nil {
					t.Fatalf("Poll(%d) = %v", index, err)
				}
			}
			if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrValidation) {
				t.Fatalf("terminal Poll() error = %v", err)
			}
		})
	}
}

func TestBatchScannerAcceptsEmptyKVBucketWithoutSession(t *testing.T) {
	table := upsertWriterTable()
	backend := &fakeKVSessionBackend{batches: []kvSessionBatch{{}}}
	scanner, err := newBatchScanner(
		context.Background(), backend, nil, table, testTableBucket(table),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || len(result.Rows) != 0 || !scanner.Done() {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if len(backend.calls) != 1 {
		t.Fatalf("scan calls = %#v", backend.calls)
	}
}

func TestBatchScannerReadsLatestLogRows(t *testing.T) {
	table := appendWriterTable()
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

func TestBatchScannerLimitsOversizedKVResponse(t *testing.T) {
	table := upsertWriterTable()
	encoded := encodedValueBatch(
		t, table,
		Row{int32(1), "one", int64(10)},
		Row{int32(2), "two", int64(20)},
		Row{int32(3), "three", int64(30)},
	)
	scanner, err := newBatchScanner(
		context.Background(),
		batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
			return false, encoded, nil
		}),
		nil, table, testTableBucket(table), WithBatchLimit(2), WithBatchProjection("name"),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Rows) != 2 || len(result.Rows[0]) != 1 ||
		result.Rows[0][0] != "two" || result.Rows[1][0] != "three" {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	result.Release()
	_ = scanner.Close()
}

func TestBatchScannerLimitsAndProjectsArrowResponse(t *testing.T) {
	table := appendWriterTable()
	encoded := encodedArrowRows(t, table.Schema, 10, 1, 2, 3, 4)
	scanner, err := newBatchScanner(
		context.Background(),
		batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
			return true, encoded, nil
		}),
		nil, table, testTableBucket(table),
		WithBatchLimit(2), WithBatchProjection("name", "id"),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Rows) != 0 || len(result.ArrowBatches) != 1 {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	batch := result.ArrowBatches[0]
	ids := batch.Record.Column(1).(*array.Int32)
	if batch.BaseOffset != 12 || batch.Record.NumRows() != 2 || batch.Record.NumCols() != 2 ||
		batch.Record.Schema().Field(0).Name != "name" || batch.Record.Schema().Field(1).Name != "id" ||
		ids.Value(0) != 3 || ids.Value(1) != 4 ||
		len(batch.Changes) != 2 || batch.Changes[0] != UpdateAfter || batch.Changes[1] != Delete {
		t.Fatalf("limited projected Arrow batch = %#v", batch)
	}
	result.Release()
	result.Release()
	_ = scanner.Close()
}

func TestBatchScannerLimitsMixedLogFormatsByOffset(t *testing.T) {
	table := appendWriterTable()
	for _, test := range []struct {
		name          string
		encoded       []byte
		wantRows      []int32
		wantArrowBase int64
		wantArrowRows int64
		wantArrowID   int32
	}{
		{
			name: "rows before Arrow",
			encoded: append(
				encodedRows(t, table.Schema, 10, 1, 2),
				encodedArrowRows(t, table.Schema, 12, 3, 4, 5)...,
			),
			wantArrowBase: 12, wantArrowRows: 3, wantArrowID: 3,
		},
		{
			name: "Arrow before rows",
			encoded: append(
				encodedArrowRows(t, table.Schema, 10, 1, 2, 3),
				encodedRows(t, table.Schema, 13, 4, 5)...,
			),
			wantRows: []int32{4, 5}, wantArrowBase: 12, wantArrowRows: 1, wantArrowID: 3,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			scanner, err := newBatchScanner(
				context.Background(),
				batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
					return true, test.encoded, nil
				}),
				nil, table, testTableBucket(table), WithBatchLimit(3),
			)
			if err != nil {
				t.Fatal(err)
			}
			result, err := scanner.Poll(context.Background())
			if err != nil || len(result.Rows) != len(test.wantRows) || len(result.ArrowBatches) != 1 {
				t.Fatalf("Poll() = %#v, %v", result, err)
			}
			for index, want := range test.wantRows {
				if result.Rows[index][0] != want {
					t.Fatalf("row %d = %#v, want id %d", index, result.Rows[index], want)
				}
			}
			batch := result.ArrowBatches[0]
			ids := batch.Record.Column(0).(*array.Int32)
			if batch.BaseOffset != test.wantArrowBase || batch.Record.NumRows() != test.wantArrowRows ||
				ids.Value(0) != test.wantArrowID || int64(len(result.Rows))+batch.Record.NumRows() != 3 {
				t.Fatalf("mixed limited batch = %#v", batch)
			}
			result.Release()
			_ = scanner.Close()
		})
	}
}

func TestBatchScannerCurrentFailures(t *testing.T) {
	table := upsertWriterTable()
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
	mu          sync.Mutex
	batches     [][]Row
	eofWithLast bool
	block       bool
	entered     chan struct{}
	enterOnce   sync.Once
	closed      atomic.Int32
	readErr     error
	closeErr    error
}

func (r *fakeSnapshotBatchReader) ReadBatch(ctx context.Context, _ int) ([]Row, error) {
	if r.block {
		if r.entered != nil {
			r.enterOnce.Do(func() { close(r.entered) })
		}
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
	if r.eofWithLast && len(r.batches) == 0 {
		return rows, io.EOF
	}
	return rows, nil
}

func (r *fakeSnapshotBatchReader) Close() error {
	r.closed.Add(1)
	return r.closeErr
}

func TestSnapshotBatchScannerLifecycle(t *testing.T) {
	table := upsertWriterTable()
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

type snapshotEOFCase struct {
	name       string
	rows       []Row
	options    []BatchScannerOption
	wantRows   int
	wantDone   bool
	wantErr    error
	wantFields int
}

func TestSnapshotBatchScannerPreservesFinalEOFResult(t *testing.T) {
	table := upsertWriterTable()
	tests := []snapshotEOFCase{
		{name: "empty", wantDone: true},
		{
			name: "rows", rows: []Row{{int32(1), "one", int64(10)}},
			wantRows: 1, wantDone: true, wantFields: 3,
		},
		{
			name: "projection", rows: []Row{{int32(1), "one", int64(10)}},
			options:  []BatchScannerOption{WithBatchProjection("name")},
			wantRows: 1, wantDone: true, wantFields: 1,
		},
		{
			name: "oversized", rows: []Row{{int32(1)}, {int32(2)}},
			options: []BatchScannerOption{WithBatchLimit(1)}, wantErr: ErrValidation,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertSnapshotEOFCase(t, table, test)
		})
	}
}

func assertSnapshotEOFCase(t *testing.T, table Table, test snapshotEOFCase) {
	t.Helper()
	reader := &fakeSnapshotBatchReader{batches: [][]Row{test.rows}, eofWithLast: true}
	scanner, err := newBatchScanner(
		context.Background(), nil, reader, table, testTableBucket(table), test.options...,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if !errors.Is(err, test.wantErr) {
		t.Fatalf("Poll() error = %v, want %v", err, test.wantErr)
	}
	if test.wantErr != nil {
		if result.Done || scanner.Done() {
			t.Fatalf("failed final result marked done: %#v", result)
		}
		_ = scanner.Close()
		return
	}
	if result.Done != test.wantDone || scanner.Done() != test.wantDone ||
		len(result.Rows) != test.wantRows {
		t.Fatalf("Poll() = %#v, scanner done = %v", result, scanner.Done())
	}
	if test.wantRows != 0 && len(result.Rows[0]) != test.wantFields {
		t.Fatalf("projected fields = %d, want %d", len(result.Rows[0]), test.wantFields)
	}
	again, err := scanner.Poll(context.Background())
	if err != nil || !again.Done || len(again.Rows) != 0 {
		t.Fatalf("terminal Poll() = %#v, %v", again, err)
	}
	if err := scanner.Close(); err != nil || reader.closed.Load() != 1 {
		t.Fatalf("Close() = %v calls=%d", err, reader.closed.Load())
	}
}

func TestSnapshotBatchScannerCloseUnblocksPoll(t *testing.T) {
	table := upsertWriterTable()
	entered := make(chan struct{})
	reader := &fakeSnapshotBatchReader{block: true, entered: entered}
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
	<-entered
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
	table := upsertWriterTable()
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
		context.Background(), appendWriterTable(), testTableBucket(appendWriterTable()), 1,
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
	table := upsertWriterTable()
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
	table := upsertWriterTable()
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
