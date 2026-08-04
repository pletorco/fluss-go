package fgo

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

var errWriterTerminal = errors.New("terminal writer failure")

type producedLog struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	records     []byte
	timeout     time.Duration
	acks        int32
}

type fakeAppendWriterBackend struct {
	mu          sync.Mutex
	physicalID  int64
	locations   map[int32]ServerNode
	writerID    int64
	metadataErr error
	initErr     error
	produceErr  error
	produceErrs []error
	block       <-chan struct{}
	calls       []producedLog
}

func (b *fakeAppendWriterBackend) metadata(context.Context, PhysicalTablePath) (int64, map[int32]ServerNode, error) {
	return b.physicalID, b.locations, b.metadataErr
}

func (b *fakeAppendWriterBackend) initWriter(context.Context, PhysicalTablePath, int32) (int64, error) {
	return b.writerID, b.initErr
}

func (b *fakeAppendWriterBackend) produce(
	ctx context.Context,
	input logProduceRequest,
) (int64, error) {
	if b.block != nil {
		select {
		case <-b.block:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, producedLog{
		path: input.path, bucket: input.bucket, tableID: input.tableID, partitionID: input.partitionID,
		records: append([]byte(nil), input.records...), timeout: input.timeout, acks: input.acks,
	})
	if len(b.produceErrs) != 0 {
		err := b.produceErrs[0]
		b.produceErrs = b.produceErrs[1:]
		if err != nil {
			return 0, err
		}
	}
	if b.produceErr != nil {
		return 0, b.produceErr
	}
	return int64(len(b.calls) * 10), nil
}

func (b *fakeAppendWriterBackend) produced() []producedLog {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]producedLog(nil), b.calls...)
}

func appendWriterTable() Table {
	return Table{
		ID: 9, SchemaID: 3, Path: TablePath{Database: "db", Table: "events"}, Kind: LogTable,
		BucketCount: 1,
		Schema: Schema{Columns: []Column{
			{Name: "id", Type: IntType},
			{Name: "name", Type: StringType, Nullable: true},
		}},
	}
}

func logBackend(bucketIDs ...int32) *fakeAppendWriterBackend {
	locations := make(map[int32]ServerNode, len(bucketIDs))
	for _, id := range bucketIDs {
		locations[id] = ServerNode{ID: id + 10, Address: "tablet", ServerType: TabletServer}
	}
	return &fakeAppendWriterBackend{physicalID: 9, locations: locations, writerID: 42}
}

func TestFlussBucketMatchesJava091(t *testing.T) {
	for _, test := range []struct {
		key  string
		want int32
	}{
		{"a", 11},
		{"fluss", 13},
		{"0123456789", 0},
		{"\u00ff", 10},
	} {
		got, err := flussBucket([]byte(test.key), 17)
		if err != nil || got != test.want {
			t.Errorf("flussBucket(%q) = %d, %v; want %d", test.key, got, err, test.want)
		}
	}
	if _, err := flussBucket(nil, 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty key error = %v", err)
	}
	if _, err := sortedBuckets(nil); !errors.Is(err, ErrMetadata) {
		t.Fatalf("empty buckets error = %v", err)
	}
	if _, err := sortedBuckets(map[int32]ServerNode{-1: {}}); !errors.Is(err, ErrMetadata) {
		t.Fatalf("negative bucket error = %v", err)
	}
}

func TestAppendWriterBatchesRowsAndAdvancesSequences(t *testing.T) {
	backend := logBackend(0)
	writer, err := newAppendWriter(
		context.Background(), backend, appendWriterTable(),
		WithAppendBatchLimits(1<<20, 2), WithAppendBuffer(8), WithAppendBatchTimeout(time.Hour), WithAppendRequest(time.Second, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	futures := make([]*WriteFuture, 5)
	for index := range futures {
		futures[index] = writer.Append(context.Background(), Row{int32(index), "value"})
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertLogFutures(t, futures)
	calls := backend.produced()
	assertLogBatchCalls(t, calls)
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func assertLogFutures(t *testing.T, futures []*WriteFuture) {
	t.Helper()
	for index, future := range futures {
		result := future.Await(context.Background())
		if result.Err != nil || result.Bucket != 0 || result.Records != 1 {
			t.Fatalf("result %d = %#v", index, result)
		}
	}
}

func assertLogBatchCalls(t *testing.T, calls []producedLog) {
	t.Helper()
	if len(calls) != 3 {
		t.Fatalf("produce calls = %d, want 3", len(calls))
	}
	for index, call := range calls {
		batch, err := DecodeLogBatchRows(appendWriterTable().Schema, call.records, true)
		if err != nil || batch.WriterID != 42 || batch.BatchSequence != int32(index) {
			t.Fatalf("batch %d = %#v, %v", index, batch, err)
		}
		if call.tableID != 9 || call.partitionID != -1 || call.timeout != time.Second || call.acks != 1 {
			t.Fatalf("request %d = %#v", index, call)
		}
	}
}

func TestAppendWriterAssignmentsAndLinger(t *testing.T) {
	table := appendWriterTable()
	table.BucketCount = 3
	backend := logBackend(7, 2, 4)
	writer, err := newAppendWriter(
		context.Background(), backend, table,
		WithAppendNoKeyAssigner(NoKeyAssignerRoundRobin), WithAppendBatchTimeout(time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 3 {
		result := writer.Append(context.Background(), Row{int32(index), nil}).Await(context.Background())
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	calls := backend.produced()
	if got := []int32{calls[0].bucket, calls[1].bucket, calls[2].bucket}; got[0] != 2 || got[1] != 4 || got[2] != 7 {
		t.Fatalf("round-robin buckets = %v", got)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	table.Schema.BucketKey = []string{"id"}
	backend = logBackend(0, 1, 2)
	writer, err = newAppendWriter(context.Background(), backend, table, WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if result := writer.Append(context.Background(), Row{int32(7), "same"}).Await(context.Background()); result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	calls = backend.produced()
	if calls[0].bucket != calls[1].bucket {
		t.Fatalf("equal bucket keys routed to %d and %d", calls[0].bucket, calls[1].bucket)
	}
	_ = writer.Close(context.Background())
}

func TestAppendWriterFlushWaitsAndCancellationCompletes(t *testing.T) {
	release := make(chan struct{})
	backend := logBackend(0)
	backend.block = release
	writer, err := newAppendWriter(context.Background(), backend, appendWriterTable(), WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	first := writer.Append(context.Background(), Row{int32(1), "blocked"})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	second := writer.Append(canceled, Row{int32(2), "canceled"})
	flushed := make(chan error, 1)
	go func() { flushed <- writer.Flush(context.Background()) }()
	select {
	case err := <-flushed:
		t.Fatalf("Flush returned before accepted write: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	if result := first.Await(context.Background()); result.Err != nil {
		t.Fatal(result.Err)
	}
	if result := second.Await(context.Background()); !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("canceled result = %#v", result)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result := writer.Append(context.Background(), Row{int32(3), nil}).Await(context.Background()); !errors.Is(result.Err, ErrClosed) {
		t.Fatalf("append after close = %#v", result)
	}
}

func TestAppendWriterFailurePoisonsOnlyBucket(t *testing.T) {
	backend := logBackend(0)
	backend.produceErr = errors.New("ambiguous transport failure")
	writer, err := newAppendWriter(context.Background(), backend, appendWriterTable(), WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	if result := writer.Append(context.Background(), Row{int32(1), nil}).Await(context.Background()); result.Err == nil {
		t.Fatal("first append succeeded")
	}
	backend.produceErr = nil
	if result := writer.Append(context.Background(), Row{int32(2), nil}).Await(context.Background()); !errors.Is(result.Err, ErrWriterState) {
		t.Fatalf("append after ambiguous failure = %#v", result)
	}
	_ = writer.Close(context.Background())
	if calls := backend.produced(); len(calls) != 1 {
		t.Fatalf("produce calls = %d, want 1", len(calls))
	}
}

func TestAppendWriterRetriesIdenticalIdempotentBatch(t *testing.T) {
	backend := logBackend(0)
	backend.produceErrs = []error{
		responseServerError(
			int32(fmsg.ErrorCodeNetworkException), "retry", fmsg.APIKeyProduceLog,
		),
		nil,
	}
	writer, err := newAppendWriter(
		context.Background(), backend, appendWriterTable(), WithAppendBatchTimeout(0),
		WithAppendRetryPolicy(WriterRetryPolicy{MaxAttempts: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := writer.Append(context.Background(), Row{int32(1), "one"}).
		Await(context.Background())
	if result.Err != nil || !result.OffsetKnown {
		t.Fatalf("retried append = %#v", result)
	}
	calls := backend.produced()
	if len(calls) != 2 || !bytes.Equal(calls[0].records, calls[1].records) {
		t.Fatalf("produce attempts = %#v", calls)
	}
	first, err := DecodeLogBatchRows(appendWriterTable().Schema, calls[0].records, true)
	if err != nil || first.WriterID != 42 || first.BatchSequence != 0 {
		t.Fatalf("retried batch = %#v, %v", first, err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAppendWriterRowFormats(t *testing.T) {
	for _, test := range []struct {
		format    LogFormat
		compacted bool
	}{
		{LogFormatCompacted, true},
		{LogFormatIndexed, false},
	} {
		t.Run(string(test.format), func(t *testing.T) {
			testAppendWriterRowFormat(t, test.format, test.compacted)
		})
	}
	config, err := appendWriterConfig(nil)
	if err != nil || config.Format != LogFormatAuto ||
		config.ArrowCompressionType != ArrowCompressionNone {
		t.Fatalf("default format config = %#v, %v", config, err)
	}
}

func testAppendWriterRowFormat(t *testing.T, format LogFormat, compacted bool) {
	t.Helper()
	table := appendWriterTable()
	table.Properties = map[string]string{"table.log.format": strings.ToUpper(string(format))}
	backend := logBackend(0)
	writer, err := newAppendWriter(
		context.Background(), backend, table,
		WithAppendLogFormat(format), WithAppendBatchTimeout(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := writer.Append(context.Background(), Row{int32(1), "one"}).
		Await(context.Background())
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	batch, err := DecodeLogBatchRows(table.Schema, backend.produced()[0].records, compacted)
	if err != nil || len(batch.Records) != 1 || batch.Records[0].Value[1] != "one" {
		t.Fatalf("decoded %s batch = %#v, %v", format, batch, err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAppendWriterArrowAndPartition(t *testing.T) {
	table := appendWriterTable()
	table.BucketCount = 2
	backend := logBackend(1, 3)
	backend.physicalID = 88
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil))
	builder.Field(0).(*array.Int32Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append("one")
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()

	writer, err := newAppendWriter(
		context.Background(), backend, table,
		WithAppendPartition("day=1"), WithAppendBatchTimeout(0),
		WithAppendLogFormat(LogFormatArrow), WithAppendArrowCompression(ArrowCompressionLZ4),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := writer.AppendArrow(context.Background(), 3, record, []ChangeType{Append}).Await(context.Background())
	if result.Err != nil || result.Bucket != 3 || result.Records != 1 {
		t.Fatalf("Arrow result = %#v", result)
	}
	call := backend.produced()[0]
	if call.tableID != 9 || call.partitionID != 88 || call.path.Partition != "day=1" {
		t.Fatalf("partition request = %#v", call)
	}
	decoded, err := DecodeArrowLogBatch(record.Schema(), call.records, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Release()
	if result := writer.Append(context.Background(), Row{int32(2), "two"}).
		Await(context.Background()); !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("row passed to Arrow writer = %#v", result)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	rowWriter, err := newAppendWriter(
		context.Background(), logBackend(1, 3), table,
		WithAppendLogFormat(LogFormatIndexed),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result := rowWriter.AppendArrow(context.Background(), 1, record, []ChangeType{Append}).
		Await(context.Background()); !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("Arrow passed to indexed writer = %#v", result)
	}
	badSchema := arrow.NewSchema([]arrow.Field{{Name: "other", Type: arrow.PrimitiveTypes.Int32}}, nil)
	badBuilder := array.NewRecordBuilder(memory.DefaultAllocator, badSchema)
	badBuilder.Field(0).(*array.Int32Builder).Append(1)
	badRecord := badBuilder.NewRecordBatch()
	badBuilder.Release()
	defer badRecord.Release()
	autoWriter, err := newAppendWriter(context.Background(), logBackend(1, 3), table)
	if err != nil {
		t.Fatal(err)
	}
	if result := autoWriter.AppendArrow(context.Background(), 1, badRecord, []ChangeType{Append}).
		Await(context.Background()); !errors.Is(result.Err, ErrInvalidSchema) {
		t.Fatalf("mismatched Arrow schema = %#v", result)
	}
	_ = rowWriter.Close(context.Background())
	_ = autoWriter.Close(context.Background())
}

func TestAppendWriterRejectsInvalidConfiguration(t *testing.T) {
	table := appendWriterTable()
	primary := table
	primary.Kind = PrimaryKeyTable
	arrowTable := table
	arrowTable.Properties = map[string]string{"table.log.format": "arrow"}
	for _, test := range []struct {
		name    string
		table   Table
		backend *fakeAppendWriterBackend
		options []AppendWriterOption
		target  error
	}{
		{"primary", primary, logBackend(0), nil, ErrTableKind},
		{"nil option", table, logBackend(0), []AppendWriterOption{nil}, ErrInvalidConfig},
		{"bad limits", table, logBackend(0), []AppendWriterOption{WithAppendBatchLimits(1, 0)}, ErrInvalidConfig},
		{"bad buffer", table, logBackend(0), []AppendWriterOption{WithAppendBuffer(0)}, ErrInvalidConfig},
		{"bad concurrency", table, logBackend(0), []AppendWriterOption{WithAppendConcurrency(0)}, ErrInvalidConfig},
		{"bad batch timeout", table, logBackend(0), []AppendWriterOption{WithAppendBatchTimeout(-1)}, ErrInvalidConfig},
		{"bad request", table, logBackend(0), []AppendWriterOption{WithAppendRequest(0, 2)}, ErrInvalidConfig},
		{
			"request timeout overflow", table, logBackend(0),
			[]AppendWriterOption{WithAppendRequest((time.Duration(math.MaxInt32)+1)*time.Millisecond, 1)},
			ErrInvalidConfig,
		},
		{"bad no-key assigner", table, logBackend(0), []AppendWriterOption{WithAppendNoKeyAssigner("random")}, ErrInvalidConfig},
		{"bad format", table, logBackend(0), []AppendWriterOption{WithAppendLogFormat("unknown")}, ErrInvalidConfig},
		{"bad compression", table, logBackend(0), []AppendWriterOption{WithAppendArrowCompression(ArrowCompressionType(99))}, ErrInvalidConfig},
		{"row compression", table, logBackend(0), []AppendWriterOption{WithAppendLogFormat(LogFormatIndexed), WithAppendArrowCompression(ArrowCompressionZSTD)}, ErrInvalidConfig},
		{"table format", arrowTable, logBackend(0), []AppendWriterOption{WithAppendLogFormat(LogFormatCompacted)}, ErrInvalidConfig},
		{"no buckets", table, logBackend(), nil, ErrMetadata},
		{"metadata error", table, &fakeAppendWriterBackend{metadataErr: context.Canceled}, nil, context.Canceled},
		{"init error", table, &fakeAppendWriterBackend{physicalID: 9, locations: logBackend(0).locations, initErr: context.Canceled}, nil, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newAppendWriter(context.Background(), test.backend, test.table, test.options...)
			if !errors.Is(err, test.target) {
				t.Fatalf("newAppendWriter() error = %v, want %v", err, test.target)
			}
		})
	}
	if result := (*WriteFuture)(nil).Await(context.Background()); !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("nil future = %#v", result)
	}
}

func TestAppendWriterRejectsInvalidOperations(t *testing.T) {
	table := appendWriterTable()
	writer, err := newAppendWriter(
		context.Background(), logBackend(0), table, WithAppendBatchTimeout(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result := writer.Append(nil, Row{int32(1), "one"}).Await(context.Background()); !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("Append(nil) = %#v", result)
	}
	if result := writer.Append(context.Background(), Row{int32(1)}).Await(context.Background()); !errors.Is(result.Err, ErrInvalidRow) {
		t.Fatalf("Append(invalid row) = %#v", result)
	}
	if result := writer.AppendArrow(context.Background(), 0, nil, nil).Await(context.Background()); !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("AppendArrow(nil) = %#v", result)
	}

	schema, err := table.Schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*array.Int32Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append("one")
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()

	for _, future := range []*WriteFuture{
		writer.AppendArrow(context.Background(), 99, record, []ChangeType{Append}),
		writer.AppendArrow(context.Background(), 0, record, nil),
		writer.AppendArrow(context.Background(), 0, record, []ChangeType{ChangeType(99)}),
	} {
		if result := future.Await(context.Background()); result.Err == nil {
			t.Fatalf("invalid Arrow append = %#v", result)
		}
	}
	if err := writer.Flush(nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Flush(nil) error = %v", err)
	}
	if err := writer.Close(nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Close(nil) error = %v", err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush after Close error = %v", err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("repeated Close error = %v", err)
	}
}

func TestAppendWriterCloseCanTimeOutWhileWriteIsBlocked(t *testing.T) {
	release := make(chan struct{})
	backend := logBackend(0)
	backend.block = release
	writer, err := newAppendWriter(
		context.Background(), backend, appendWriterTable(), WithAppendBatchTimeout(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	future := writer.Append(context.Background(), Row{int32(1), "blocked"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := writer.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v, want deadline", err)
	}
	close(release)
	if result := future.Await(context.Background()); result.Err != nil {
		t.Fatalf("blocked append = %#v", result)
	}
	select {
	case <-writer.done:
	case <-time.After(time.Second):
		t.Fatal("writer did not finish after backend release")
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("repeated Close() error = %v", err)
	}
}

func TestAppendWriterRequestTimeoutTerminatesClose(t *testing.T) {
	backend := logBackend(0)
	backend.block = make(chan struct{})
	writer, err := newAppendWriter(
		context.Background(), backend, appendWriterTable(),
		WithAppendBatchTimeout(time.Hour), WithAppendRequest(20*time.Millisecond, -1),
	)
	if err != nil {
		t.Fatal(err)
	}
	future := writer.Append(context.Background(), Row{int32(1), "blocked"})
	started := time.Now()
	if err := writer.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v, want deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close() took %v", elapsed)
	}
	if result := future.Await(context.Background()); !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf("future = %#v", result)
	}
	select {
	case <-writer.done:
	default:
		t.Fatal("writer scheduler remained active after timed out close")
	}
}

func TestAppendWriterClosePreservesTerminalFailure(t *testing.T) {
	release := make(chan struct{})
	backend := logBackend(0)
	backend.block = release
	backend.produceErr = errWriterTerminal
	writer, err := newAppendWriter(
		context.Background(), backend, appendWriterTable(), WithAppendBatchTimeout(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	future := writer.Append(context.Background(), Row{int32(1), "blocked"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := writer.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close() error = %v, want deadline", err)
	}
	close(release)
	if result := future.Await(context.Background()); !errors.Is(result.Err, errWriterTerminal) {
		t.Fatalf("future = %#v", result)
	}
	if err := writer.Close(context.Background()); !errors.Is(err, errWriterTerminal) {
		t.Fatalf("repeated Close() error = %v, want terminal failure", err)
	}
}

func TestAppendWriterConcurrentCloseReturnsTerminalResult(t *testing.T) {
	backend := logBackend(0)
	backend.produceErr = errWriterTerminal
	writer, err := newAppendWriter(
		context.Background(), backend, appendWriterTable(), WithAppendBatchTimeout(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Append(context.Background(), Row{int32(1), "one"})
	const callers = 8
	results := make(chan error, callers)
	for range callers {
		go func() { results <- writer.Close(context.Background()) }()
	}
	for range callers {
		if err := <-results; !errors.Is(err, errWriterTerminal) {
			t.Fatalf("Close() error = %v, want terminal failure", err)
		}
	}
}

func TestAppendWriterMovesStickyBatchAfterSizeBoundary(t *testing.T) {
	backend := logBackend(0, 1)
	table := appendWriterTable()
	table.BucketCount = 2
	writer, err := newAppendWriter(
		context.Background(), backend, table,
		WithAppendBatchTimeout(time.Hour),
		WithAppendBatchLimits(logBatchV0HeaderSize+1, 100),
	)
	if err != nil {
		t.Fatal(err)
	}
	first := writer.Append(context.Background(), Row{int32(1), strings.Repeat("a", 40)})
	second := writer.Append(context.Background(), Row{int32(2), strings.Repeat("b", 40)})
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	firstResult := first.Await(context.Background())
	secondResult := second.Await(context.Background())
	if firstResult.Err != nil || secondResult.Err != nil {
		t.Fatalf("write results = %#v, %#v", firstResult, secondResult)
	}
	if firstResult.Bucket == secondResult.Bucket {
		t.Fatalf("sticky batches remained on bucket %d", firstResult.Bucket)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestClientAppendWriterBackendUsesFluss091Messages(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	var produced *fmsg.ProduceLogRequest
	client := routedWriterClient(t,
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			switch message := response.Message().(type) {
			case *fmsg.MetadataResponse:
				*message = *metadataResponse(path)
			default:
				t.Fatalf("coordinator request = %T", message)
			}
			return response, nil
		},
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			switch message := response.Message().(type) {
			case *fmsg.InitWriterResponse:
				got := request.(*fmsg.MessageRequest).Message().(*fmsg.InitWriterRequest)
				if got.GetTablePath()[0].GetTableName() != "events" {
					t.Fatalf("init writer = %#v", got)
				}
				message.WriterId = proto.Int64(77)
			case *fmsg.ProduceLogResponse:
				produced = request.(*fmsg.MessageRequest).Message().(*fmsg.ProduceLogRequest)
				message.BucketsResp = []*fmsg.PbProduceLogRespForBucket{{
					BucketId: proto.Int32(0), ErrorCode: proto.Int32(0), BaseOffset: proto.Int64(100),
				}}
			default:
				t.Fatalf("tablet request = %T", message)
			}
			return response, nil
		},
	)
	table := appendWriterTable()
	writer, err := client.NewAppendWriter(context.Background(), table, WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	result := writer.Append(context.Background(), Row{int32(1), "one"}).Await(context.Background())
	if result.Err != nil || result.BaseOffset != 100 {
		t.Fatalf("write result = %#v", result)
	}
	if produced.GetTableId() != 9 || produced.GetAcks() != -1 || produced.GetTimeoutMs() != 30_000 ||
		produced.GetBucketsReq()[0].GetBucketId() != 0 {
		t.Fatalf("produce request = %#v", produced)
	}
	batch, err := DecodeLogBatchRows(table.Schema, produced.GetBucketsReq()[0].GetRecords(), true)
	if err != nil || batch.WriterID != 77 {
		t.Fatalf("wire batch = %#v, %v", batch, err)
	}
	_ = writer.Close(context.Background())
}

func TestClientAppendWriterBackendResponseErrors(t *testing.T) {
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
			case *fmsg.InitWriterResponse:
				message.WriterId = proto.Int64(1)
			case *fmsg.ProduceLogResponse:
				message.BucketsResp = []*fmsg.PbProduceLogRespForBucket{{
					BucketId: proto.Int32(0), ErrorCode: proto.Int32(int32(fmsg.ErrorCodeNotLeaderOrFollower)),
					ErrorMessage: proto.String("moved"),
				}}
			}
			return response, nil
		},
	)
	backend := clientAppendWriterBackend{client: client}
	if _, err := backend.produce(context.Background(), logProduceRequest{
		path: path, tableID: 9, partitionID: -1, records: []byte{1}, timeout: time.Second, acks: 1,
	}); !errors.Is(err, ErrMetadata) {
		t.Fatalf("server error = %v", err)
	}
	if _, err := backend.produce(context.Background(), logProduceRequest{
		path: path, tableID: 9, partitionID: -1,
		timeout: time.Duration(int64(^uint32(0))) * time.Millisecond, acks: 1,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("timeout error = %v", err)
	}

	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", serverType: TabletServer}]
	tablet.requester = requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(fmsg.APIKeyApiVersions, 0)
		return response, nil
	})
	if _, err := backend.initWriter(context.Background(), path, 0); err == nil {
		t.Fatal("unexpected init response succeeded")
	}
	if _, err := backend.produce(context.Background(), logProduceRequest{
		path: path, tableID: 9, partitionID: -1, timeout: time.Second, acks: 1,
	}); err == nil {
		t.Fatal("unexpected produce response succeeded")
	}

	tablet.requester = requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		response.Message().(*fmsg.ProduceLogResponse).BucketsResp = []*fmsg.PbProduceLogRespForBucket{}
		return response, nil
	})
	if _, err := backend.produce(context.Background(), logProduceRequest{
		path: path, tableID: 9, partitionID: -1, timeout: time.Second, acks: 1,
	}); !errors.Is(err, ErrValidation) {
		t.Fatalf("omitted bucket error = %v", err)
	}
}

func TestClientAppendWriterBackendMetadataFailures(t *testing.T) {
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}}
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(fmsg.APIKeyApiVersions, 0)
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetMetadata] = 0
	backend := clientAppendWriterBackend{client: client}
	if _, _, err := backend.metadata(context.Background(), path); err == nil {
		t.Fatal("unexpected metadata response succeeded")
	}
}

func routedWriterClient(
	t *testing.T,
	coordinatorHandler requesterFunc,
	tabletHandler requesterFunc,
) *Client {
	t.Helper()
	coordinatorNode := ServerNode{ID: 1, Address: "coordinator:9123", ServerType: Coordinator}
	tabletNode := ServerNode{ID: 2, Address: "tablet:9123", ServerType: TabletServer}
	coordinator := newClient(coordinatorHandler, nil)
	coordinator.serverID, coordinator.address, coordinator.serverType = 1, coordinatorNode.Address, Coordinator
	coordinator.versions[fmsg.APIKeyGetMetadata] = 0
	tablet := newClient(tabletHandler, nil)
	tablet.serverID, tablet.address, tablet.serverType = 2, tabletNode.Address, TabletServer
	tablet.versions[fmsg.APIKeyInitWriter] = 0
	tablet.versions[fmsg.APIKeyProduceLog] = 0
	manager := newConnectionManager(config{})
	manager.clients[connectionKey{id: 1, address: coordinatorNode.Address, serverType: Coordinator}] = coordinator
	manager.clients[connectionKey{id: 2, address: tabletNode.Address, serverType: TabletServer}] = tablet
	client := newClient(nil, nil)
	client.manager = manager
	client.router = NewRouter(coordinatorNode, client.fetchTableMetadata).
		WithPhysicalMetadataFetcher(client.fetchPartitionMetadata)
	return client
}
