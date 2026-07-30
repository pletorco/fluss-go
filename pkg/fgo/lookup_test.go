package fgo

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type lookupCall struct {
	prefix      bool
	bucket      int32
	tableID     int64
	partitionID int64
	keys        [][]byte
}

type fakeLookupBackend struct {
	mu          sync.Mutex
	physicalID  int64
	locations   map[int32]Node
	metadataErr error
	values      map[string][]byte
	prefixes    map[string][][]byte
	lookupErr   error
	prefixErr   error
	block       <-chan struct{}
	calls       []lookupCall
	active      int
	maxActive   int
}

func (b *fakeLookupBackend) metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error) {
	return b.physicalID, b.locations, b.metadataErr
}

func (b *fakeLookupBackend) lookup(
	ctx context.Context,
	_ PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	keys [][]byte,
) ([][]byte, error) {
	if err := b.begin(ctx, lookupCall{bucket: bucket, tableID: tableID, partitionID: partitionID, keys: cloneBytesList(keys)}); err != nil {
		return nil, err
	}
	defer b.end()
	if b.lookupErr != nil {
		return nil, b.lookupErr
	}
	values := make([][]byte, len(keys))
	for index, key := range keys {
		values[index] = append([]byte(nil), b.values[string(key)]...)
	}
	return values, nil
}

func (b *fakeLookupBackend) prefixLookup(
	ctx context.Context,
	_ PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	keys [][]byte,
) ([][][]byte, error) {
	if err := b.begin(ctx, lookupCall{prefix: true, bucket: bucket, tableID: tableID, partitionID: partitionID, keys: cloneBytesList(keys)}); err != nil {
		return nil, err
	}
	defer b.end()
	if b.prefixErr != nil {
		return nil, b.prefixErr
	}
	values := make([][][]byte, len(keys))
	for index, key := range keys {
		values[index] = cloneBytesList(b.prefixes[string(key)])
	}
	return values, nil
}

func (b *fakeLookupBackend) begin(ctx context.Context, call lookupCall) error {
	b.mu.Lock()
	b.calls = append(b.calls, call)
	b.active++
	if b.active > b.maxActive {
		b.maxActive = b.active
	}
	b.mu.Unlock()
	if b.block != nil {
		select {
		case <-b.block:
		case <-ctx.Done():
			b.end()
			return ctx.Err()
		}
	}
	return nil
}

func (b *fakeLookupBackend) end() {
	b.mu.Lock()
	b.active--
	b.mu.Unlock()
}

func lookupTable() Table {
	return Table{
		ID: 15, SchemaID: 2, Path: TablePath{Database: "db", Table: "profiles"},
		Kind: PrimaryKeyTable, BucketCount: 2,
		Schema: Schema{
			Columns: []Column{
				{Name: "tenant", Type: StringType},
				{Name: "id", Type: IntType},
				{Name: "name", Type: StringType, Nullable: true},
			},
			PrimaryKey: []string{"tenant", "id"}, BucketKey: []string{"tenant"},
		},
	}
}

func lookupBackendFor(table Table, bucketIDs ...int32) *fakeLookupBackend {
	locations := make(map[int32]Node, len(bucketIDs))
	for _, bucket := range bucketIDs {
		locations[bucket] = Node{ID: bucket + 10, Address: "tablet", Role: TabletServer}
	}
	return &fakeLookupBackend{
		physicalID: table.ID, locations: locations,
		values: make(map[string][]byte), prefixes: make(map[string][][]byte),
	}
}

func putLookupValue(t *testing.T, backend *fakeLookupBackend, table Table, key PrimaryKey, row Row) {
	t.Helper()
	encodedKey, err := EncodeLookupKey(table.Schema, key, 1)
	if err != nil {
		t.Fatal(err)
	}
	value, err := EncodeCompactedRow(table.Schema, row)
	if err != nil {
		t.Fatal(err)
	}
	backend.values[string(encodedKey)] = value
}

func TestLookupClientPreservesInputAndNotFound(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	putLookupValue(t, backend, table, PrimaryKey{"a", int32(1)}, Row{"a", int32(1), "one"})
	putLookupValue(t, backend, table, PrimaryKey{"b", int32(2)}, Row{"b", int32(2), "two"})
	client, err := newLookupClient(context.Background(), backend, table, WithLookupBatch(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	results := client.Lookup(
		context.Background(),
		PrimaryKey{"b", int32(2)},
		PrimaryKey{"missing", int32(3)},
		PrimaryKey{"a", int32(1)},
	)
	if !results[0].Found || results[0].Row[2] != "two" || !errors.Is(results[1].Err, ErrNotFound) ||
		!results[2].Found || results[2].Row[2] != "one" {
		t.Fatalf("lookup results = %#v", results)
	}
	if len(backend.calls) != 3 || backend.maxActive > 2 {
		t.Fatalf("calls = %d, max active = %d", len(backend.calls), backend.maxActive)
	}
	_ = client.Close()
}

func TestPrefixLookupClientDecodesRows(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	key, err := EncodePrefixLookupKey(table.Schema, PrimaryKey{"a"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []Row{{"a", int32(1), "one"}, {"a", int32(2), "two"}} {
		value, encodeErr := EncodeCompactedRow(table.Schema, row)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		backend.prefixes[string(key)] = append(backend.prefixes[string(key)], value)
	}
	client, err := newLookupClient(context.Background(), backend, table)
	if err != nil {
		t.Fatal(err)
	}
	results := client.PrefixLookup(context.Background(), PrimaryKey{"a"}, PrimaryKey{"none"})
	if results[0].Err != nil || len(results[0].Rows) != 2 || results[0].Rows[1][2] != "two" ||
		results[1].Err != nil || len(results[1].Rows) != 0 {
		t.Fatalf("prefix results = %#v", results)
	}
	_ = client.Close()
}

func TestLookupClientPartialErrorsAndValidation(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	backend.lookupErr = errors.New("lookup unavailable")
	backend.prefixErr = errors.New("prefix unavailable")
	client, err := newLookupClient(context.Background(), backend, table)
	if err != nil {
		t.Fatal(err)
	}
	results := client.Lookup(context.Background(), PrimaryKey{"a"}, PrimaryKey{"a", int32(1)})
	if results[0].Err == nil || !errors.Is(results[1].Err, backend.lookupErr) {
		t.Fatalf("point errors = %#v", results)
	}
	prefix := client.PrefixLookup(context.Background(), PrimaryKey{}, PrimaryKey{"a"})
	if prefix[0].Err == nil || !errors.Is(prefix[1].Err, backend.prefixErr) {
		t.Fatalf("prefix errors = %#v", prefix)
	}
	_ = client.Close()

	bad := table
	bad.Schema.BucketKey = []string{"id"}
	client, err = newLookupClient(context.Background(), lookupBackendFor(bad, 0, 1), bad)
	if err != nil {
		t.Fatal(err)
	}
	if result := client.PrefixLookup(context.Background(), PrimaryKey{"a", int32(1)})[0]; !errors.Is(result.Err, ErrInvalidSchema) {
		t.Fatalf("non-leading bucket prefix = %#v", result)
	}
	_ = client.Close()
}

func TestLookupClientCancellationAndClose(t *testing.T) {
	table := lookupTable()
	release := make(chan struct{})
	backend := lookupBackendFor(table, 0, 1)
	backend.block = release
	client, err := newLookupClient(context.Background(), backend, table)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan []LookupResult, 1)
	go func() { done <- client.Lookup(context.Background(), PrimaryKey{"a", int32(1)}) }()
	time.Sleep(10 * time.Millisecond)
	_ = client.Close()
	if result := <-done; !errors.Is(result[0].Err, context.Canceled) {
		t.Fatalf("close result = %#v", result)
	}
	if result := client.Lookup(context.Background(), PrimaryKey{"a", int32(1)})[0]; !errors.Is(result.Err, ErrClosed) {
		t.Fatalf("lookup after close = %#v", result)
	}
	if result := client.PrefixLookup(nil, PrimaryKey{"a"})[0]; !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("nil-context prefix = %#v", result)
	}
	close(release)
}

func TestLookupClientRejectsInvalidConfiguration(t *testing.T) {
	table := lookupTable()
	logTable := table
	logTable.Kind = LogTable
	for _, test := range []struct {
		name    string
		table   Table
		backend *fakeLookupBackend
		options []LookupOption
		target  error
	}{
		{"log table", logTable, lookupBackendFor(table, 0), nil, ErrTableKind},
		{"nil option", table, lookupBackendFor(table, 0), []LookupOption{nil}, ErrInvalidConfig},
		{"bad batch", table, lookupBackendFor(table, 0), []LookupOption{WithLookupBatch(0, 0)}, ErrInvalidConfig},
		{"metadata", table, &fakeLookupBackend{metadataErr: context.Canceled}, nil, context.Canceled},
		{"no buckets", table, lookupBackendFor(table), nil, ErrMetadata},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newLookupClient(context.Background(), test.backend, test.table, test.options...)
			if !errors.Is(err, test.target) {
				t.Fatalf("newLookupClient() error = %v, want %v", err, test.target)
			}
		})
	}
}

func TestClientLookupBackendMessagesAndErrors(t *testing.T) {
	table := lookupTable()
	path := table.Path
	pointValue, _ := EncodeCompactedRow(table.Schema, Row{"a", int32(1), "one"})
	var pointRequest *fmsg.LookupRequest
	var prefixRequest *fmsg.PrefixLookupRequest
	client := routedWriterClient(t,
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			metadata := metadataResponse(path)
			metadata.TableMetadata[0].TableId = proto.Int64(table.ID)
			metadata.TableMetadata[0].BucketMetadata = []*fmsg.PbBucketMetadata{
				{BucketId: proto.Int32(0), LeaderId: proto.Int32(2)},
				{BucketId: proto.Int32(1), LeaderId: proto.Int32(2)},
			}
			proto.Merge(response.Message().(*fmsg.MetadataResponse), metadata)
			return response, nil
		},
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			switch message := response.Message().(type) {
			case *fmsg.LookupResponse:
				if request.Version() != 1 {
					t.Fatalf("Lookup version = %d", request.Version())
				}
				pointRequest = request.(*fmsg.MessageRequest).Message().(*fmsg.LookupRequest)
				message.BucketsResp = []*fmsg.PbLookupRespForBucket{{
					BucketId: pointRequest.BucketsReq[0].BucketId,
					Values:   []*fmsg.PbValue{{Values: pointValue}},
				}}
			case *fmsg.PrefixLookupResponse:
				if request.Version() != 1 {
					t.Fatalf("PrefixLookup version = %d", request.Version())
				}
				prefixRequest = request.(*fmsg.MessageRequest).Message().(*fmsg.PrefixLookupRequest)
				message.BucketsResp = []*fmsg.PbPrefixLookupRespForBucket{{
					BucketId: prefixRequest.BucketsReq[0].BucketId,
					ValueLists: []*fmsg.PbValueList{{
						Values: [][]byte{pointValue},
					}},
				}}
			}
			return response, nil
		},
	)
	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", role: TabletServer}]
	tablet.versions[fmsg.APIKeyLookup], tablet.versions[fmsg.APIKeyPrefixLookup] = 1, 1
	lookup, err := client.NewLookupClient(context.Background(), table)
	if err != nil {
		t.Fatal(err)
	}
	if result := lookup.Lookup(context.Background(), PrimaryKey{"a", int32(1)})[0]; result.Err != nil {
		t.Fatal(result.Err)
	}
	if result := lookup.PrefixLookup(context.Background(), PrimaryKey{"a"})[0]; result.Err != nil || len(result.Rows) != 1 {
		t.Fatalf("prefix wire result = %#v", result)
	}
	if pointRequest.GetTableId() != table.ID || len(pointRequest.GetBucketsReq()[0].GetKeys()) != 1 ||
		prefixRequest.GetTableId() != table.ID {
		t.Fatalf("requests = %#v / %#v", pointRequest, prefixRequest)
	}
	_ = lookup.Close()
}

func TestClientLookupBackendRejectsBadResponses(t *testing.T) {
	table := lookupTable()
	client := routedWriterClient(t,
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			metadata := metadataResponse(table.Path)
			metadata.TableMetadata[0].TableId = proto.Int64(table.ID)
			proto.Merge(response.Message().(*fmsg.MetadataResponse), metadata)
			return response, nil
		},
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			switch message := response.Message().(type) {
			case *fmsg.LookupResponse:
				message.BucketsResp = []*fmsg.PbLookupRespForBucket{{
					BucketId: proto.Int32(0), ErrorCode: proto.Int32(int32(fmsg.ErrorCodeAuthorizationException)),
				}}
			case *fmsg.PrefixLookupResponse:
				message.BucketsResp = []*fmsg.PbPrefixLookupRespForBucket{{BucketId: proto.Int32(0)}}
			}
			return response, nil
		},
	)
	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", role: TabletServer}]
	tablet.versions[fmsg.APIKeyLookup], tablet.versions[fmsg.APIKeyPrefixLookup] = 1, 1
	backend := clientLookupBackend{client: client}
	path := PhysicalTablePath{TablePath: table.Path}
	if _, err := backend.lookup(context.Background(), path, 0, table.ID, -1, [][]byte{{1}}); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("lookup body error = %v", err)
	}
	if _, err := backend.prefixLookup(context.Background(), path, 0, table.ID, -1, [][]byte{{1}}); !errors.Is(err, ErrValidation) {
		t.Fatalf("prefix count error = %v", err)
	}
}
