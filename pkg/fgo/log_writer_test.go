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

type producedLog struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	records     []byte
	timeout     time.Duration
	acks        int32
}

type fakeLogWriterBackend struct {
	mu          sync.Mutex
	physicalID  int64
	locations   map[int32]Node
	writerID    int64
	metadataErr error
	initErr     error
	produceErr  error
	block       <-chan struct{}
	calls       []producedLog
}

func (b *fakeLogWriterBackend) metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error) {
	return b.physicalID, b.locations, b.metadataErr
}

func (b *fakeLogWriterBackend) initWriter(context.Context, PhysicalTablePath, int32) (int64, error) {
	return b.writerID, b.initErr
}

func (b *fakeLogWriterBackend) produce(
	_ context.Context,
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	records []byte,
	timeout time.Duration,
	acks int32,
) (int64, error) {
	if b.block != nil {
		<-b.block
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, producedLog{
		path: path, bucket: bucket, tableID: tableID, partitionID: partitionID,
		records: append([]byte(nil), records...), timeout: timeout, acks: acks,
	})
	if b.produceErr != nil {
		return 0, b.produceErr
	}
	return int64(len(b.calls) * 10), nil
}

func (b *fakeLogWriterBackend) produced() []producedLog {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]producedLog(nil), b.calls...)
}

func logWriterTable() Table {
	return Table{
		ID: 9, SchemaID: 3, Path: TablePath{Database: "db", Table: "events"}, Kind: LogTable,
		BucketCount: 1,
		Schema: Schema{Columns: []Column{
			{Name: "id", Type: IntType},
			{Name: "name", Type: StringType, Nullable: true},
		}},
	}
}

func logBackend(bucketIDs ...int32) *fakeLogWriterBackend {
	locations := make(map[int32]Node, len(bucketIDs))
	for _, id := range bucketIDs {
		locations[id] = Node{ID: id + 10, Address: "tablet", Role: TabletServer}
	}
	return &fakeLogWriterBackend{physicalID: 9, locations: locations, writerID: 42}
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
	if _, err := sortedBuckets(map[int32]Node{-1: {}}); !errors.Is(err, ErrMetadata) {
		t.Fatalf("negative bucket error = %v", err)
	}
}

func TestLogWriterBatchesRowsAndAdvancesSequences(t *testing.T) {
	backend := logBackend(0)
	writer, err := newLogWriter(
		context.Background(), backend, logWriterTable(),
		WithLogBatchLimits(1<<20, 2), WithLogBuffer(8), WithLogLinger(time.Hour), WithLogRequest(time.Second, 1),
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
	for index, future := range futures {
		result := future.Await(context.Background())
		if result.Err != nil || result.Bucket != 0 || result.Records != 1 {
			t.Fatalf("result %d = %#v", index, result)
		}
	}
	calls := backend.produced()
	if len(calls) != 3 {
		t.Fatalf("produce calls = %d, want 3", len(calls))
	}
	for index, call := range calls {
		batch, decodeErr := DecodeLogBatchRows(logWriterTable().Schema, call.records, true)
		if decodeErr != nil || batch.WriterID != 42 || batch.BatchSequence != int32(index) {
			t.Fatalf("batch %d = %#v, %v", index, batch, decodeErr)
		}
		if call.tableID != 9 || call.partitionID != -1 || call.timeout != time.Second || call.acks != 1 {
			t.Fatalf("request %d = %#v", index, call)
		}
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLogWriterAssignmentsAndLinger(t *testing.T) {
	table := logWriterTable()
	table.BucketCount = 3
	backend := logBackend(7, 2, 4)
	writer, err := newLogWriter(
		context.Background(), backend, table,
		WithLogBucketAssignment(AssignmentRoundRobin), WithLogLinger(time.Millisecond),
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
	writer, err = newLogWriter(context.Background(), backend, table, WithLogLinger(0))
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

func TestLogWriterFlushWaitsAndCancellationCompletes(t *testing.T) {
	release := make(chan struct{})
	backend := logBackend(0)
	backend.block = release
	writer, err := newLogWriter(context.Background(), backend, logWriterTable(), WithLogLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	first := writer.Append(context.Background(), Row{int32(1), "blocked"})
	canceled, cancel := context.WithCancel(context.Background())
	second := writer.Append(canceled, Row{int32(2), "canceled"})
	cancel()
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

func TestLogWriterFailurePoisonsOnlyBucket(t *testing.T) {
	backend := logBackend(0)
	backend.produceErr = errors.New("ambiguous transport failure")
	writer, err := newLogWriter(context.Background(), backend, logWriterTable(), WithLogLinger(0))
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

func TestLogWriterArrowAndPartition(t *testing.T) {
	table := logWriterTable()
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

	writer, err := newLogWriter(context.Background(), backend, table, WithLogPartition("day=1"), WithLogLinger(0))
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
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLogWriterRejectsInvalidConfiguration(t *testing.T) {
	table := logWriterTable()
	primary := table
	primary.Kind = PrimaryKeyTable
	for _, test := range []struct {
		name    string
		table   Table
		backend *fakeLogWriterBackend
		options []LogWriterOption
		target  error
	}{
		{"primary", primary, logBackend(0), nil, ErrTableKind},
		{"nil option", table, logBackend(0), []LogWriterOption{nil}, ErrInvalidConfig},
		{"bad limits", table, logBackend(0), []LogWriterOption{WithLogBatchLimits(1, 0)}, ErrInvalidConfig},
		{"bad buffer", table, logBackend(0), []LogWriterOption{WithLogBuffer(0)}, ErrInvalidConfig},
		{"bad linger", table, logBackend(0), []LogWriterOption{WithLogLinger(-1)}, ErrInvalidConfig},
		{"bad request", table, logBackend(0), []LogWriterOption{WithLogRequest(0, 2)}, ErrInvalidConfig},
		{"bad assignment", table, logBackend(0), []LogWriterOption{WithLogBucketAssignment("random")}, ErrInvalidConfig},
		{"no buckets", table, logBackend(), nil, ErrMetadata},
		{"metadata error", table, &fakeLogWriterBackend{metadataErr: context.Canceled}, nil, context.Canceled},
		{"init error", table, &fakeLogWriterBackend{physicalID: 9, locations: logBackend(0).locations, initErr: context.Canceled}, nil, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newLogWriter(context.Background(), test.backend, test.table, test.options...)
			if !errors.Is(err, test.target) {
				t.Fatalf("newLogWriter() error = %v, want %v", err, test.target)
			}
		})
	}
	if result := (*WriteFuture)(nil).Await(context.Background()); !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("nil future = %#v", result)
	}
}

func TestClientLogWriterBackendUsesFluss091Messages(t *testing.T) {
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
	table := logWriterTable()
	writer, err := client.NewLogWriter(context.Background(), table, WithLogLinger(0))
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

func TestClientLogWriterBackendResponseErrors(t *testing.T) {
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
	backend := clientLogWriterBackend{client: client}
	if _, err := backend.produce(context.Background(), path, 0, 9, -1, []byte{1}, time.Second, 1); !errors.Is(err, ErrMetadata) {
		t.Fatalf("server error = %v", err)
	}
	if _, err := backend.produce(context.Background(), path, 0, 9, -1, nil, time.Duration(int64(^uint32(0)))*time.Millisecond, 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("timeout error = %v", err)
	}

	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", role: TabletServer}]
	tablet.requester = requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(fmsg.APIKeyApiVersions, 0)
		return response, nil
	})
	if _, err := backend.initWriter(context.Background(), path, 0); err == nil {
		t.Fatal("unexpected init response succeeded")
	}
	if _, err := backend.produce(context.Background(), path, 0, 9, -1, nil, time.Second, 1); err == nil {
		t.Fatal("unexpected produce response succeeded")
	}

	tablet.requester = requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		response.Message().(*fmsg.ProduceLogResponse).BucketsResp = []*fmsg.PbProduceLogRespForBucket{}
		return response, nil
	})
	if _, err := backend.produce(context.Background(), path, 0, 9, -1, nil, time.Second, 1); !errors.Is(err, ErrValidation) {
		t.Fatalf("omitted bucket error = %v", err)
	}
}

func TestClientLogWriterBackendMetadataFailures(t *testing.T) {
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}}
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(fmsg.APIKeyApiVersions, 0)
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetMetadata] = 0
	backend := clientLogWriterBackend{client: client}
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
	coordinatorNode := Node{ID: 1, Address: "coordinator:9123", Role: Coordinator}
	tabletNode := Node{ID: 2, Address: "tablet:9123", Role: TabletServer}
	coordinator := newClient(coordinatorHandler, nil)
	coordinator.serverID, coordinator.address, coordinator.role = 1, coordinatorNode.Address, Coordinator
	coordinator.versions[fmsg.APIKeyGetMetadata] = 0
	tablet := newClient(tabletHandler, nil)
	tablet.serverID, tablet.address, tablet.role = 2, tabletNode.Address, TabletServer
	tablet.versions[fmsg.APIKeyInitWriter] = 0
	tablet.versions[fmsg.APIKeyProduceLog] = 0
	manager := newConnectionManager(config{})
	manager.clients[connectionKey{id: 1, address: coordinatorNode.Address, role: Coordinator}] = coordinator
	manager.clients[connectionKey{id: 2, address: tabletNode.Address, role: TabletServer}] = tablet
	client := newClient(nil, nil)
	client.manager = manager
	client.router = NewRouter(coordinatorNode, client.fetchTableMetadata).
		WithPhysicalMetadataFetcher(client.fetchPartitionMetadata)
	return client
}
