package fgo

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type scannerFetchCall struct {
	bucket      int32
	offset      int64
	tableID     int64
	partitionID int64
	projection  []int32
	config      LogScannerConfig
}

type fakeLogScannerBackend struct {
	mu          sync.Mutex
	physicalID  int64
	locations   map[int32]Node
	metadataErr error
	listOffsets map[int32]int64
	listErr     error
	fetches     map[int32]scannerFetch
	fetchErr    map[int32]error
	block       <-chan struct{}
	calls       []scannerFetchCall
}

func (b *fakeLogScannerBackend) metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error) {
	return b.physicalID, b.locations, b.metadataErr
}

func (b *fakeLogScannerBackend) listOffset(
	context.Context,
	PhysicalTablePath,
	int32,
	int64,
	int64,
	ScanOffset,
) (int64, error) {
	if b.listErr != nil {
		return 0, b.listErr
	}
	return b.listOffsets[0], nil
}

func (b *fakeLogScannerBackend) fetch(
	ctx context.Context,
	input logFetchRequest,
) (scannerFetch, error) {
	b.mu.Lock()
	b.calls = append(b.calls, scannerFetchCall{
		bucket: input.bucket, offset: input.offset, tableID: input.tableID, partitionID: input.partitionID,
		projection: append([]int32(nil), input.projection...), config: input.config,
	})
	b.mu.Unlock()
	if b.block != nil {
		select {
		case <-b.block:
		case <-ctx.Done():
			return scannerFetch{}, ctx.Err()
		}
	}
	return b.fetches[input.bucket], b.fetchErr[input.bucket]
}

func (b *fakeLogScannerBackend) fetchCalls() []scannerFetchCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]scannerFetchCall(nil), b.calls...)
}

func scannerBackend(bucketIDs ...int32) *fakeLogScannerBackend {
	locations := make(map[int32]Node, len(bucketIDs))
	for _, bucket := range bucketIDs {
		locations[bucket] = Node{ID: bucket + 10, Address: "tablet", Role: TabletServer}
	}
	return &fakeLogScannerBackend{
		physicalID: 9, locations: locations, listOffsets: map[int32]int64{0: 5},
		fetches: make(map[int32]scannerFetch), fetchErr: make(map[int32]error),
	}
}

func encodedRows(t *testing.T, schema Schema, base int64, values ...int32) []byte {
	t.Helper()
	batch := LogBatch{Magic: 0, BaseOffset: base, SchemaID: 3, AppendOnly: true}
	for _, value := range values {
		batch.Records = append(batch.Records, Record{Value: Row{value, "row"}, Change: Append})
	}
	encoded, err := batch.EncodeRows(schema, true)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestLogScannerPollPreservesBucketOrderAndPartialErrors(t *testing.T) {
	table := logWriterTable()
	table.BucketCount = 2
	backend := scannerBackend(1, 0)
	first := encodedRows(t, table.Schema, 5, 1, 2)
	second := encodedRows(t, table.Schema, 7, 3)
	backend.fetches[0] = scannerFetch{records: append(first, second...), highWatermark: 20}
	backend.fetchErr[1] = errors.New("bucket unavailable")
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(5))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if len(result.Records) != 3 || result.Records[0].Bucket != 0 ||
		result.Records[0].Record.Offset != 5 || result.Records[2].Record.Offset != 7 {
		t.Fatalf("records = %#v", result.Records)
	}
	if len(result.BucketErrors) != 1 || result.BucketErrors[0].Bucket != 1 {
		t.Fatalf("bucket errors = %#v", result.BucketErrors)
	}
	if result.HighWatermark[0] != 20 {
		t.Fatalf("high watermark = %#v", result.HighWatermark)
	}
	backend.fetches[0] = scannerFetch{}
	_, _ = scanner.Poll(context.Background())
	calls := backend.fetchCalls()
	if calls[2].bucket != 0 || calls[2].offset != 8 {
		t.Fatalf("second poll call = %#v", calls[2])
	}
	_ = scanner.Close()
}

func TestLogScannerRowLimitAndStoppingOffsets(t *testing.T) {
	table := logWriterTable()
	table.BucketCount = 2
	backend := scannerBackend(0, 1)
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 5, 1, 2, 3)}
	backend.fetches[1] = scannerFetch{records: encodedRows(t, table.Schema, 5, 4, 5)}
	scanner, err := newLogScanner(
		context.Background(), backend, table, AtOffset(5),
		WithScanStoppingOffsets(map[int32]int64{0: 7, 1: 6}),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || !scanner.Done() || len(result.Records) != 3 ||
		result.Records[0].Record.Offset != 5 || result.Records[1].Record.Offset != 6 ||
		result.Records[2].Record.Offset != 5 {
		t.Fatalf("bounded Poll() = %#v, %v", result, err)
	}
	result.Release()
	calls := len(backend.fetchCalls())
	repeated, err := scanner.Poll(context.Background())
	if err != nil || !repeated.Done || len(repeated.Records) != 0 ||
		len(backend.fetchCalls()) != calls {
		t.Fatalf("terminal Poll() = %#v, %v, calls=%d", repeated, err, len(backend.fetchCalls()))
	}
	_ = scanner.Close()

	backend = scannerBackend(0, 1)
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 0, 1, 2, 3)}
	backend.fetches[1] = scannerFetch{records: encodedRows(t, table.Schema, 0, 4)}
	scanner, err = newLogScanner(context.Background(), backend, table, AtOffset(0), WithScanRowLimit(2))
	if err != nil {
		t.Fatal(err)
	}
	result, err = scanner.Poll(context.Background())
	if err != nil || !result.Done || len(result.Records) != 2 || len(backend.fetchCalls()) != 1 {
		t.Fatalf("limited Poll() = %#v, %v, calls=%#v", result, err, backend.fetchCalls())
	}
	result.Release()
	_ = scanner.Close()

	backend = scannerBackend(0)
	scanner, err = newLogScanner(
		context.Background(), backend, table, AtOffset(5),
		WithScanStoppingOffsets(map[int32]int64{0: 5}),
	)
	if err != nil || !scanner.Done() {
		t.Fatalf("initially complete scanner = %#v, %v", scanner, err)
	}
	result, err = scanner.Poll(context.Background())
	if err != nil || !result.Done || len(backend.fetchCalls()) != 0 {
		t.Fatalf("initial terminal Poll() = %#v, %v", result, err)
	}
	_ = scanner.Close()
}

func TestLogScannerProjectionAndOffsetInitialization(t *testing.T) {
	table := logWriterTable()
	table.BucketCount = 1
	backend := scannerBackend(0)
	backend.listOffsets[0] = 12
	projected := Schema{Columns: []Column{{Name: "name", Type: StringType, Nullable: true}}}
	encoded, err := (LogBatch{
		Magic: 0, BaseOffset: 12, SchemaID: 3, AppendOnly: true,
		Records: []Record{{Value: Row{"projected"}, Change: Append}},
	}).EncodeRows(projected, true)
	if err != nil {
		t.Fatal(err)
	}
	backend.fetches[0] = scannerFetch{records: encoded}
	scanner, err := newLogScanner(
		context.Background(), backend, table, Earliest(),
		WithScanProjection("name"), WithScanLimits(4096, 2048, 1, time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanner.Schema(); len(got.Columns) != 1 || got.Columns[0].Name != "name" {
		t.Fatalf("projected schema = %#v", got)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Records) != 1 || result.Records[0].Record.Value[0] != "projected" {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	call := backend.fetchCalls()[0]
	if call.offset != 12 || len(call.projection) != 1 || call.projection[0] != 1 ||
		call.config.MaxBytes != 4096 {
		t.Fatalf("projected fetch = %#v", call)
	}
	result.Release()
	_ = scanner.Close()
}

func TestLogScannerDecodesIndexedTable(t *testing.T) {
	table := logWriterTable()
	table.Properties = map[string]string{"table.log.format": "INDEXED"}
	backend := scannerBackend(0)
	encoded, err := (LogBatch{
		Magic: 0, BaseOffset: 3, SchemaID: int16(table.SchemaID), AppendOnly: true,
		Records: []Record{{Value: Row{int32(7), "indexed"}, Change: Append}},
	}).EncodeRows(table.Schema, false)
	if err != nil {
		t.Fatal(err)
	}
	backend.fetches[0] = scannerFetch{records: encoded}
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(3))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Records) != 1 || result.Records[0].Record.Value[1] != "indexed" {
		t.Fatalf("indexed Poll() = %#v, %v", result, err)
	}
	result.Release()
	_ = scanner.Close()

	if _, err := newLogScanner(
		context.Background(), scannerBackend(0), table, AtOffset(0),
		WithScanProjection("name"),
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("indexed projection error = %v", err)
	}
}

func TestLogScannerArrowBatchOwnership(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil))
	builder.Field(0).(*array.Int32Builder).Append(9)
	builder.Field(1).(*array.StringBuilder).Append("arrow")
	record := builder.NewRecordBatch()
	builder.Release()
	encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, BaseOffset: 30, SchemaID: 3, Record: record, Changes: []ChangeType{Append},
	}, ArrowCompressionZSTD, memory.DefaultAllocator)
	record.Release()
	if err != nil {
		t.Fatal(err)
	}
	backend.fetches[0] = scannerFetch{records: encoded, highWatermark: 31}
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(30))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.ArrowBatches) != 1 || result.ArrowBatches[0].Batch.Record.NumRows() != 1 {
		t.Fatalf("Arrow Poll() = %#v, %v", result, err)
	}
	result.Release()
	result.Release()
	backend.fetches[0] = scannerFetch{}
	_, _ = scanner.Poll(context.Background())
	if calls := backend.fetchCalls(); calls[1].offset != 31 {
		t.Fatalf("next Arrow offset = %d", calls[1].offset)
	}
	_ = scanner.Close()
}

func TestLogScannerSlicesArrowBatchAtLimit(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil))
	for value := int32(1); value <= 3; value++ {
		builder.Field(0).(*array.Int32Builder).Append(value)
		builder.Field(1).(*array.StringBuilder).Append("arrow")
	}
	record := builder.NewRecordBatch()
	builder.Release()
	encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, BaseOffset: 10, SchemaID: 3, Record: record,
		Changes: []ChangeType{Append, Append, Append},
	}, ArrowCompressionNone, memory.DefaultAllocator)
	record.Release()
	if err != nil {
		t.Fatal(err)
	}
	backend.fetches[0] = scannerFetch{records: encoded}
	scanner, err := newLogScanner(
		context.Background(), backend, table, AtOffset(10), WithScanRowLimit(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || len(result.ArrowBatches) != 1 ||
		result.ArrowBatches[0].Batch.Record.NumRows() != 2 ||
		len(result.ArrowBatches[0].Batch.Changes) != 2 {
		t.Fatalf("sliced Arrow Poll() = %#v, %v", result, err)
	}
	result.Release()
	result.Release()
	_ = scanner.Close()
}

func TestLogScannerWakeupAndErrorPrecedence(t *testing.T) {
	table := logWriterTable()
	block := make(chan struct{})
	backend := scannerBackend(0)
	backend.block = block
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(0))
	if err != nil {
		t.Fatal(err)
	}
	polled := make(chan error, 1)
	go func() {
		_, pollErr := scanner.Poll(context.Background())
		polled <- pollErr
	}()
	time.Sleep(10 * time.Millisecond)
	scanner.Wakeup()
	if err := <-polled; !errors.Is(err, ErrWakeup) {
		t.Fatalf("Wakeup Poll error = %v", err)
	}
	backend.block = nil
	if _, err := scanner.Poll(context.Background()); err != nil {
		t.Fatalf("Poll after Wakeup = %v", err)
	}
	scanner.Wakeup()
	if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrWakeup) {
		t.Fatalf("pending Wakeup error = %v", err)
	}

	backend.block = block
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, pollErr := scanner.Poll(ctx)
		polled <- pollErr
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	scanner.Wakeup()
	if err := <-polled; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation precedence error = %v", err)
	}
	_ = scanner.Close()
}

func TestLogScannerSubscribeCloseAndCancellation(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	block := make(chan struct{})
	backend.block = block
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := scanner.Subscribe(context.Background(), 0, Latest()); err != nil {
		t.Fatal(err)
	}
	if err := scanner.Subscribe(context.Background(), 9, AtOffset(0)); !errors.Is(err, ErrUnknownBucket) {
		t.Fatalf("unknown subscribe error = %v", err)
	}
	polled := make(chan error, 1)
	go func() {
		_, pollErr := scanner.Poll(context.Background())
		polled <- pollErr
	}()
	time.Sleep(10 * time.Millisecond)
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-polled; !errors.Is(err, ErrClosed) {
		t.Fatalf("Poll after Close wakeup = %v", err)
	}
	if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Poll closed error = %v", err)
	}
	if err := scanner.Subscribe(context.Background(), 0, AtOffset(0)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe closed error = %v", err)
	}

	backend = scannerBackend(0)
	backend.block = block
	scanner, _ = newLogScanner(context.Background(), backend, table, AtOffset(0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scanner.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Poll error = %v", err)
	}
	_ = scanner.Close()
}

func TestLogScannerEmptySubscriptionAndMalformedBatch(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(0))
	if err != nil {
		t.Fatal(err)
	}
	scanner.Unsubscribe(0)
	if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty Poll error = %v", err)
	}
	_ = scanner.Close()

	if _, _, _, err := decodeFetchedLog(table.Schema, 0, 4, []byte{1}); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("truncated fetch error = %v", err)
	}
	bad := make([]byte, 12)
	bad[8] = 100
	if _, _, _, err := decodeFetchedLog(table.Schema, 0, 4, bad); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("bad size error = %v", err)
	}
}

func TestLogScannerRejectsInvalidConfiguration(t *testing.T) {
	table := logWriterTable()
	for _, test := range []struct {
		name    string
		start   ScanOffset
		backend *fakeLogScannerBackend
		options []LogScannerOption
		target  error
	}{
		{"negative offset", AtOffset(-1), scannerBackend(0), nil, ErrInvalidConfig},
		{"zero timestamp", AtTimestamp(time.Time{}), scannerBackend(0), nil, ErrInvalidConfig},
		{"unknown offset", ScanOffset{Kind: 99}, scannerBackend(0), nil, ErrInvalidConfig},
		{"nil option", AtOffset(0), scannerBackend(0), []LogScannerOption{nil}, ErrInvalidConfig},
		{"empty projection", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanProjection()}, ErrInvalidConfig},
		{"unknown projection", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanProjection("missing")}, ErrInvalidSchema},
		{"bad limits", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanLimits(0, 1, 2, -1)}, ErrInvalidConfig},
		{"bad row limit", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanRowLimit(0)}, ErrInvalidConfig},
		{"empty stops", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanStoppingOffsets(nil)}, ErrInvalidConfig},
		{"negative stop", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanStoppingOffsets(map[int32]int64{0: -1})}, ErrInvalidConfig},
		{"missing stop", AtOffset(0), scannerBackend(0, 1), []LogScannerOption{WithScanStoppingOffsets(map[int32]int64{0: 1})}, ErrInvalidConfig},
		{"unknown stop", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanStoppingOffsets(map[int32]int64{0: 1, 1: 1})}, ErrInvalidConfig},
		{"metadata", AtOffset(0), &fakeLogScannerBackend{metadataErr: context.Canceled}, nil, context.Canceled},
		{"no buckets", AtOffset(0), scannerBackend(), nil, ErrMetadata},
		{"list offset", Earliest(), &fakeLogScannerBackend{physicalID: 9, locations: scannerBackend(0).locations, listErr: context.Canceled}, nil, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newLogScanner(context.Background(), test.backend, table, test.start, test.options...)
			if !errors.Is(err, test.target) {
				t.Fatalf("newLogScanner() error = %v, want %v", err, test.target)
			}
		})
	}
	if err := (ScanOffset{Kind: ScanFromLatest, Offset: 1}).Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("valued latest error = %v", err)
	}
}

func TestClientLogScannerBackendMessages(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	var listed *fmsg.ListOffsetsRequest
	var fetched *fmsg.FetchLogRequest
	table := logWriterTable()
	records := encodedRows(t, table.Schema, 4, 8)
	client := routedWriterClient(t,
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			*response.Message().(*fmsg.MetadataResponse) = *metadataResponse(path)
			return response, nil
		},
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			switch message := response.Message().(type) {
			case *fmsg.ListOffsetsResponse:
				listed = request.(*fmsg.MessageRequest).Message().(*fmsg.ListOffsetsRequest)
				message.BucketsResp = []*fmsg.PbListOffsetsRespForBucket{{
					BucketId: proto.Int32(0), Offset: proto.Int64(4),
				}}
			case *fmsg.FetchLogResponse:
				fetched = request.(*fmsg.MessageRequest).Message().(*fmsg.FetchLogRequest)
				message.TablesResp = []*fmsg.PbFetchLogRespForTable{{
					TableId: proto.Int64(9), BucketsResp: []*fmsg.PbFetchLogRespForBucket{{
						BucketId: proto.Int32(0), HighWatermark: proto.Int64(5), Records: records,
					}},
				}}
			}
			return response, nil
		},
	)
	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", role: TabletServer}]
	tablet.versions[fmsg.APIKeyListOffsets] = 0
	tablet.versions[fmsg.APIKeyFetchLog] = 0
	scanner, err := client.NewLogScanner(context.Background(), table, AtTimestamp(time.UnixMilli(1234)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Records) != 1 {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	if listed.GetOffsetType() != 2 || listed.GetStartTimestamp() != 1234 || listed.GetFollowerServerId() != -1 {
		t.Fatalf("ListOffsets request = %#v", listed)
	}
	bucket := fetched.GetTablesReq()[0].GetBucketsReq()[0]
	if fetched.GetFollowerServerId() != -1 || bucket.GetFetchOffset() != 4 ||
		bucket.GetMaxFetchBytes() != 1<<20 {
		t.Fatalf("FetchLog request = %#v", fetched)
	}
	result.Release()
	_ = scanner.Close()
}

func TestClientLogScannerBackendResponseErrors(t *testing.T) {
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}}
	client := routedWriterClient(t,
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			*response.Message().(*fmsg.MetadataResponse) = *metadataResponse(path.TablePath)
			return response, nil
		},
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			switch message := response.Message().(type) {
			case *fmsg.ListOffsetsResponse:
				message.BucketsResp = []*fmsg.PbListOffsetsRespForBucket{{
					BucketId: proto.Int32(0), ErrorCode: proto.Int32(int32(fmsg.ErrorCodeAuthorizationException)),
				}}
			case *fmsg.FetchLogResponse:
				message.TablesResp = []*fmsg.PbFetchLogRespForTable{{
					TableId: proto.Int64(9), BucketsResp: []*fmsg.PbFetchLogRespForBucket{{
						BucketId: proto.Int32(0), ErrorCode: proto.Int32(int32(fmsg.ErrorCodeCorruptRecordException)),
					}},
				}}
			}
			return response, nil
		},
	)
	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", role: TabletServer}]
	tablet.versions[fmsg.APIKeyListOffsets], tablet.versions[fmsg.APIKeyFetchLog] = 0, 0
	backend := clientLogScannerBackend{client: client}
	if _, err := backend.listOffset(context.Background(), path, 0, 9, -1, Latest()); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("ListOffsets error = %v", err)
	}
	if _, err := backend.fetch(context.Background(), logFetchRequest{
		path: path, tableID: 9, partitionID: -1,
		config: LogScannerConfig{MaxBytes: 1, MaxBucketBytes: 1},
	}); !errors.Is(err, ErrRecord) {
		t.Fatalf("FetchLog error = %v", err)
	}
	if _, err := backend.listOffset(context.Background(), path, 0, 9, -1, AtOffset(0)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("explicit ListOffsets error = %v", err)
	}
}
