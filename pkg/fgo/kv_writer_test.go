package fgo

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type putKVCall struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	targets     []int32
	records     []byte
	timeout     time.Duration
	acks        int32
	mergeMode   MergeMode
}

type fakeUpsertWriterBackend struct {
	mu          sync.Mutex
	physicalID  int64
	locations   map[int32]ServerNode
	writerID    int64
	metadataErr error
	initErr     error
	putErr      error
	putErrs     []error
	block       <-chan struct{}
	calls       []putKVCall
}

func (b *fakeUpsertWriterBackend) metadata(context.Context, PhysicalTablePath) (int64, map[int32]ServerNode, error) {
	return b.physicalID, b.locations, b.metadataErr
}

func (b *fakeUpsertWriterBackend) initWriter(context.Context, PhysicalTablePath, int32) (int64, error) {
	return b.writerID, b.initErr
}

func (b *fakeUpsertWriterBackend) put(
	ctx context.Context,
	input kvPutRequest,
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
	b.calls = append(b.calls, putKVCall{
		path: input.path, bucket: input.bucket, tableID: input.tableID, partitionID: input.partitionID,
		targets: append([]int32(nil), input.targets...), records: append([]byte(nil), input.records...),
		timeout: input.timeout, acks: input.acks, mergeMode: input.mergeMode,
	})
	if len(b.putErrs) != 0 {
		err := b.putErrs[0]
		b.putErrs = b.putErrs[1:]
		if err != nil {
			return 0, err
		}
	}
	if b.putErr != nil {
		return 0, b.putErr
	}
	batch, err := DecodeKVBatch(input.records)
	if err != nil {
		return 0, err
	}
	return int64(len(b.calls)*10 + len(batch.Records)), nil
}

func (b *fakeUpsertWriterBackend) putCalls() []putKVCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]putKVCall(nil), b.calls...)
}

func upsertWriterTable() Table {
	return Table{
		ID: 11, SchemaID: 4, Path: TablePath{Database: "db", Table: "users"},
		Kind: PrimaryKeyTable, BucketCount: 1,
		Schema: Schema{
			Columns: []Column{
				{Name: "id", Type: IntType},
				{Name: "name", Type: StringType, Nullable: true},
				{Name: "score", Type: BigIntType, Nullable: true},
			},
			PrimaryKey: []string{"id"}, BucketKey: []string{"id"},
		},
	}
}

func kvBackend(bucketIDs ...int32) *fakeUpsertWriterBackend {
	locations := make(map[int32]ServerNode, len(bucketIDs))
	for _, id := range bucketIDs {
		locations[id] = ServerNode{ID: id + 10, Address: "tablet", ServerType: TabletServer}
	}
	return &fakeUpsertWriterBackend{physicalID: 11, locations: locations, writerID: 99}
}

func TestUpsertWriterBatchesUpsertsDeletesAndSequences(t *testing.T) {
	table := upsertWriterTable()
	backend := kvBackend(0)
	writer, err := newUpsertWriter(
		context.Background(), backend, table,
		WithUpsertBatchLimits(1<<20, 2), WithUpsertBuffer(8), WithUpsertBatchTimeout(time.Hour), WithUpsertRequest(time.Second, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	first := writer.Upsert(context.Background(), Row{int32(1), "one", int64(10)})
	deleted := writer.Delete(context.Background(), PrimaryKey{int32(2)})
	third := writer.Upsert(context.Background(), Row{int32(3), nil, nil})
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertKVFutures(t, first, deleted, third)
	calls := backend.putCalls()
	assertKVBatchCalls(t, calls)
	firstBatch, _ := DecodeKVBatch(calls[0].records)
	if len(firstBatch.Records) != 2 || firstBatch.Records[1].Value != nil {
		t.Fatalf("upsert/delete batch = %#v", firstBatch)
	}
	row, err := DecodeCompactedRow(table.Schema, firstBatch.Records[0].Value)
	if err != nil || row[1] != "one" {
		t.Fatalf("upsert value = %#v, %v", row, err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUpsertWriterRetriesIdenticalIdempotentBatch(t *testing.T) {
	backend := kvBackend(0)
	backend.putErrs = []error{
		responseServerError(
			int32(fmsg.ErrorCodeRequestTimeOut), "retry", fmsg.APIKeyPutKv,
		),
		nil,
	}
	writer, err := newUpsertWriter(
		context.Background(), backend, upsertWriterTable(), WithUpsertBatchTimeout(0),
		WithUpsertRetryPolicy(WriterRetryPolicy{MaxAttempts: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := writer.Upsert(context.Background(), Row{int32(1), "one", nil}).
		Await(context.Background())
	if result.Err != nil || !result.OffsetKnown {
		t.Fatalf("retried upsert = %#v", result)
	}
	calls := backend.putCalls()
	if len(calls) != 2 || !bytes.Equal(calls[0].records, calls[1].records) {
		t.Fatalf("put attempts = %#v", calls)
	}
	first, err := DecodeKVBatch(calls[0].records)
	if err != nil || first.WriterID != 99 || first.BatchSequence != 0 {
		t.Fatalf("retried batch = %#v, %v", first, err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUpsertWriterMergeModes(t *testing.T) {
	table := upsertWriterTable()
	table.Properties = map[string]string{"table.merge-engine": "aggregation"}
	backend := kvBackend(0)
	writer, err := newUpsertWriter(
		context.Background(), backend, table,
		WithUpsertMergeMode(MergeModeOverwrite), WithUpsertBatchTimeout(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := writer.Upsert(context.Background(), Row{int32(1), "one", int64(10)}).
		Await(context.Background())
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if calls := backend.putCalls(); len(calls) != 1 || calls[0].mergeMode != MergeModeOverwrite {
		t.Fatalf("put calls = %#v", calls)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	table.Properties = map[string]string{}
	if _, err := newUpsertWriter(
		context.Background(), kvBackend(0), table, WithUpsertMergeMode(MergeModeOverwrite),
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("overwrite without merge engine error = %v", err)
	}
	if _, err := upsertWriterConfig([]UpsertWriterOption{
		WithUpsertMergeMode(MergeMode(2)),
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid merge mode error = %v", err)
	}
	config, err := upsertWriterConfig(nil)
	if err != nil || config.MergeMode != MergeModeDefault {
		t.Fatalf("default merge mode = %d, %v", config.MergeMode, err)
	}
}

func assertKVFutures(t *testing.T, futures ...*WriteFuture) {
	t.Helper()
	for _, future := range futures {
		if result := future.Await(context.Background()); result.Err != nil || result.Bucket != 0 {
			t.Fatalf("result = %#v", result)
		}
	}
}

func assertKVBatchCalls(t *testing.T, calls []putKVCall) {
	t.Helper()
	if len(calls) != 2 {
		t.Fatalf("put calls = %d, want 2", len(calls))
	}
	for index, call := range calls {
		batch, err := DecodeKVBatch(call.records)
		if err != nil || batch.WriterID != 99 || batch.BatchSequence != int32(index) {
			t.Fatalf("batch %d = %#v, %v", index, batch, err)
		}
		if call.tableID != 11 || call.partitionID != -1 || call.timeout != time.Second || call.acks != 1 {
			t.Fatalf("call %d = %#v", index, call)
		}
	}
}

func TestUpsertWriterPartialUpdateUsesTargetColumns(t *testing.T) {
	table := upsertWriterTable()
	backend := kvBackend(0)
	writer, err := newUpsertWriter(context.Background(), backend, table, WithUpsertBatchTimeout(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	partial := writer.PartialUpsert(context.Background(), []string{"id", "name"}, Row{int32(7), "new"})
	full := writer.Upsert(context.Background(), Row{int32(8), "full", int64(1)})
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result := partial.Await(context.Background()); result.Err != nil {
		t.Fatal(result.Err)
	}
	if result := full.Await(context.Background()); result.Err != nil {
		t.Fatal(result.Err)
	}
	calls := backend.putCalls()
	if len(calls) != 2 || len(calls[0].targets) != 2 || calls[0].targets[0] != 0 || calls[0].targets[1] != 1 ||
		len(calls[1].targets) != 0 {
		t.Fatalf("target column calls = %#v", calls)
	}
	batch, _ := DecodeKVBatch(calls[0].records)
	partialRow, err := DecodeCompactedRow(table.Schema, batch.Records[0].Value)
	if err != nil || partialRow[0] != int32(7) || partialRow[1] != "new" || partialRow[2] != nil {
		t.Fatalf("partial value = %#v, %v", partialRow, err)
	}
	_ = writer.Close(context.Background())
}

func TestUpsertWriterPartitionRoutingAndHash(t *testing.T) {
	table := upsertWriterTable()
	table.BucketCount = 3
	backend := kvBackend(0, 1, 2)
	backend.physicalID = 55
	writer, err := newUpsertWriter(context.Background(), backend, table, WithUpsertPartition("day=1"), WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result := writer.Upsert(context.Background(), Row{int32(7), "same", nil}).Await(context.Background())
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	calls := backend.putCalls()
	if calls[0].bucket != calls[1].bucket || calls[0].tableID != 11 || calls[0].partitionID != 55 {
		t.Fatalf("partition/hash calls = %#v", calls)
	}
	_ = writer.Close(context.Background())
}

func TestUpsertWriterFailureCancellationAndClose(t *testing.T) {
	table := upsertWriterTable()
	backend := kvBackend(0)
	backend.putErr = errors.New("ambiguous")
	writer, err := newUpsertWriter(context.Background(), backend, table, WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	if result := writer.Upsert(context.Background(), Row{int32(1), nil, nil}).Await(context.Background()); result.Err == nil {
		t.Fatal("ambiguous write succeeded")
	}
	backend.putErr = nil
	if result := writer.Delete(context.Background(), PrimaryKey{int32(1)}).Await(context.Background()); !errors.Is(result.Err, ErrWriterState) {
		t.Fatalf("write after poison = %#v", result)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result := writer.Delete(context.Background(), PrimaryKey{int32(1)}).Await(context.Background()); !errors.Is(result.Err, ErrClosed) {
		t.Fatalf("delete after close = %#v", result)
	}

	release := make(chan struct{})
	backend = kvBackend(0)
	backend.block = release
	writer, _ = newUpsertWriter(context.Background(), backend, table, WithUpsertBatchTimeout(0))
	first := writer.Upsert(context.Background(), Row{int32(1), nil, nil})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	second := writer.Upsert(ctx, Row{int32(2), nil, nil})
	flushed := make(chan error, 1)
	go func() { flushed <- writer.Flush(context.Background()) }()
	select {
	case <-flushed:
		t.Fatal("Flush returned before blocked write")
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
		t.Fatalf("canceled mutation = %#v", result)
	}
	_ = writer.Close(context.Background())
}

func TestUpsertWriterRejectsInvalidMutations(t *testing.T) {
	table := upsertWriterTable()
	writer, err := newUpsertWriter(context.Background(), kvBackend(0), table, WithUpsertBatchTimeout(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	tests := []*WriteFuture{
		writer.Upsert(nil, Row{int32(1), nil, nil}),
		writer.Upsert(context.Background(), Row{int32(1)}),
		writer.PartialUpsert(context.Background(), nil, nil),
		writer.PartialUpsert(context.Background(), []string{"name"}, Row{"no-key"}),
		writer.PartialUpsert(context.Background(), []string{"id", "id"}, Row{int32(1), int32(1)}),
		writer.Delete(context.Background(), PrimaryKey{}),
	}
	for index, future := range tests {
		if result := future.Await(context.Background()); result.Err == nil {
			t.Errorf("invalid mutation %d succeeded", index)
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

	auto := table
	auto.Schema.AutoIncrement = []string{"score"}
	writer, err = newUpsertWriter(context.Background(), kvBackend(0), auto, WithUpsertBatchTimeout(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result := writer.Upsert(context.Background(), Row{int32(1), nil, int64(1)}).Await(context.Background()); result.Err == nil {
		t.Fatal("full auto-increment upsert succeeded")
	}
	if result := writer.PartialUpsert(context.Background(), []string{"id", "score"}, Row{int32(1), int64(1)}).Await(context.Background()); result.Err == nil {
		t.Fatal("partial auto-increment upsert succeeded")
	}
	_ = writer.Close(context.Background())
}

func TestUpsertWriterCloseCanTimeOutWhileWriteIsBlocked(t *testing.T) {
	release := make(chan struct{})
	backend := kvBackend(0)
	backend.block = release
	writer, err := newUpsertWriter(
		context.Background(), backend, upsertWriterTable(), WithUpsertBatchTimeout(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	future := writer.Upsert(context.Background(), Row{int32(1), "blocked", nil})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := writer.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v, want deadline", err)
	}
	close(release)
	if result := future.Await(context.Background()); result.Err != nil {
		t.Fatalf("blocked mutation = %#v", result)
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

func TestUpsertWriterRequestTimeoutTerminatesClose(t *testing.T) {
	backend := kvBackend(0)
	backend.block = make(chan struct{})
	writer, err := newUpsertWriter(
		context.Background(), backend, upsertWriterTable(),
		WithUpsertBatchTimeout(time.Hour), WithUpsertRequest(20*time.Millisecond, -1),
	)
	if err != nil {
		t.Fatal(err)
	}
	future := writer.Upsert(context.Background(), Row{int32(1), "blocked", nil})
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

func TestUpsertWriterClosePreservesTerminalFailure(t *testing.T) {
	release := make(chan struct{})
	backend := kvBackend(0)
	backend.block = release
	backend.putErr = errWriterTerminal
	writer, err := newUpsertWriter(
		context.Background(), backend, upsertWriterTable(), WithUpsertBatchTimeout(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	future := writer.Upsert(context.Background(), Row{int32(1), "blocked", nil})
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

func TestUpsertWriterConcurrentCloseReturnsTerminalResult(t *testing.T) {
	backend := kvBackend(0)
	backend.putErr = errWriterTerminal
	writer, err := newUpsertWriter(
		context.Background(), backend, upsertWriterTable(), WithUpsertBatchTimeout(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Upsert(context.Background(), Row{int32(1), "one", nil})
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

func TestUpsertWriterRejectsInvalidConfiguration(t *testing.T) {
	table := upsertWriterTable()
	logTable := table
	logTable.Kind = LogTable
	badBucket := table
	badBucket.Schema.BucketKey = []string{"name"}
	for _, test := range []struct {
		name    string
		table   Table
		backend *fakeUpsertWriterBackend
		options []UpsertWriterOption
		target  error
	}{
		{"log table", logTable, kvBackend(0), nil, ErrTableKind},
		{"bad bucket key", badBucket, kvBackend(0), nil, ErrInvalidSchema},
		{"nil option", table, kvBackend(0), []UpsertWriterOption{nil}, ErrInvalidConfig},
		{"batch limits", table, kvBackend(0), []UpsertWriterOption{WithUpsertBatchLimits(1, 0)}, ErrInvalidConfig},
		{"buffer", table, kvBackend(0), []UpsertWriterOption{WithUpsertBuffer(0)}, ErrInvalidConfig},
		{"concurrency", table, kvBackend(0), []UpsertWriterOption{WithUpsertConcurrency(65)}, ErrInvalidConfig},
		{"batch timeout", table, kvBackend(0), []UpsertWriterOption{WithUpsertBatchTimeout(-1)}, ErrInvalidConfig},
		{"request", table, kvBackend(0), []UpsertWriterOption{WithUpsertRequest(0, 2)}, ErrInvalidConfig},
		{"metadata", table, &fakeUpsertWriterBackend{metadataErr: context.Canceled}, nil, context.Canceled},
		{"no buckets", table, kvBackend(), nil, ErrMetadata},
		{"init", table, &fakeUpsertWriterBackend{physicalID: 11, locations: kvBackend(0).locations, initErr: context.Canceled}, nil, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newUpsertWriter(context.Background(), test.backend, test.table, test.options...)
			if !errors.Is(err, test.target) {
				t.Fatalf("newUpsertWriter() error = %v, want %v", err, test.target)
			}
		})
	}
}

func TestClientUpsertWriterBackendMessagesAndErrors(t *testing.T) {
	path := TablePath{Database: "db", Table: "users"}
	var requestMessage *fmsg.PutKvRequest
	client := routedWriterClient(t,
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			metadata := metadataResponse(path)
			metadata.TableMetadata[0].TableId = proto.Int64(11)
			proto.Merge(response.Message().(*fmsg.MetadataResponse), metadata)
			return response, nil
		},
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			switch message := response.Message().(type) {
			case *fmsg.InitWriterResponse:
				message.WriterId = proto.Int64(5)
			case *fmsg.PutKvResponse:
				if request.Version() != 1 {
					t.Fatalf("PutKv version = %d, want negotiated v1", request.Version())
				}
				requestMessage = request.(*fmsg.MessageRequest).Message().(*fmsg.PutKvRequest)
				message.BucketsResp = []*fmsg.PbPutKvRespForBucket{{
					BucketId: proto.Int32(0), LogEndOffset: proto.Int64(10),
				}}
			}
			return response, nil
		},
	)
	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", serverType: TabletServer}]
	tablet.versions[fmsg.APIKeyPutKv] = 1
	writer, err := client.NewUpsertWriter(
		context.Background(), upsertWriterTable(),
		WithUpsertBatchTimeout(0), WithUpsertMergeMode(MergeModeOverwrite),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := writer.PartialUpsert(context.Background(), []string{"id", "name"}, Row{int32(1), "one"}).Await(context.Background())
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if requestMessage.GetTableId() != 11 || requestMessage.GetAggMode() != 1 ||
		len(requestMessage.GetTargetColumns()) != 2 || requestMessage.GetBucketsReq()[0].GetBucketId() != 0 {
		t.Fatalf("PutKv request = %#v", requestMessage)
	}
	_ = writer.Close(context.Background())

	tablet.requester = requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		response.Message().(*fmsg.PutKvResponse).BucketsResp = []*fmsg.PbPutKvRespForBucket{{
			BucketId: proto.Int32(0), ErrorCode: proto.Int32(int32(fmsg.ErrorCodeKvStorageException)),
		}}
		return response, nil
	})
	backend := clientUpsertWriterBackend{client: client}
	if _, err := backend.put(context.Background(), kvPutRequest{
		path: PhysicalTablePath{TablePath: path}, tableID: 11, partitionID: -1, timeout: time.Second, acks: 1,
	}); !errors.Is(err, ErrStorage) {
		t.Fatalf("PutKv server error = %v", err)
	}
}
