package fgo

import (
	"context"
	"encoding/binary"
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
	insert      bool
	timeout     time.Duration
	acks        int32
}

type fakeLookupBackend struct {
	mu          sync.Mutex
	physicalID  int64
	locations   map[int32]Node
	metadataErr error
	values      map[string][]byte
	prefixes    map[string][][]byte
	lookupErr   error
	lookupErrs  []error
	prefixErr   error
	prefixErrs  []error
	block       <-chan struct{}
	delay       time.Duration
	entered     chan struct{}
	enterOnce   sync.Once
	calls       []lookupCall
	active      int
	maxActive   int
}

func (b *fakeLookupBackend) metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error) {
	return b.physicalID, b.locations, b.metadataErr
}

func (b *fakeLookupBackend) lookup(
	ctx context.Context,
	input lookupRequest,
) ([][]byte, error) {
	if err := b.begin(ctx, lookupCall{
		bucket: input.bucket, tableID: input.tableID, partitionID: input.partitionID,
		keys: cloneBytesList(input.keys), insert: input.insertIfNotExist,
		timeout: input.timeout, acks: input.acks,
	}); err != nil {
		return nil, err
	}
	defer b.end()
	b.mu.Lock()
	if len(b.lookupErrs) != 0 {
		err := b.lookupErrs[0]
		b.lookupErrs = b.lookupErrs[1:]
		b.mu.Unlock()
		if err != nil {
			return nil, err
		}
	} else {
		b.mu.Unlock()
	}
	if b.lookupErr != nil {
		return nil, b.lookupErr
	}
	values := make([][]byte, len(input.keys))
	for index, key := range input.keys {
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
	b.mu.Lock()
	if len(b.prefixErrs) != 0 {
		err := b.prefixErrs[0]
		b.prefixErrs = b.prefixErrs[1:]
		b.mu.Unlock()
		if err != nil {
			return nil, err
		}
	} else {
		b.mu.Unlock()
	}
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
	if b.entered != nil {
		b.enterOnce.Do(func() { close(b.entered) })
	}
	if b.block != nil {
		select {
		case <-b.block:
		case <-ctx.Done():
			b.end()
			return ctx.Err()
		}
	}
	if b.delay > 0 {
		timer := time.NewTimer(b.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
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

func putLookupValue(t testing.TB, backend *fakeLookupBackend, table Table, key PrimaryKey, row Row) {
	t.Helper()
	encodedKey, err := EncodeLookupKey(table.Schema, key, 1)
	if err != nil {
		t.Fatal(err)
	}
	value := encodeLookupValue(t, table, row)
	backend.values[string(encodedKey)] = value
}

func encodeLookupValue(t testing.TB, table Table, row Row) []byte {
	t.Helper()
	encoded, err := EncodeCompactedRow(table.Schema, row)
	if err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 2+len(encoded))
	binary.LittleEndian.PutUint16(value, uint16(table.SchemaID))
	copy(value[2:], encoded)
	return value
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

func TestLookupClientBatchesConcurrentCalls(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	key := PrimaryKey{"shared", int32(1)}
	putLookupValue(t, backend, table, key, Row{"shared", int32(1), "one"})
	const callers = 16
	client, err := newLookupClient(
		context.Background(), backend, table,
		WithLookupBatch(callers, 2),
		WithLookupQueue(callers, time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan LookupResult, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			results <- client.Lookup(context.Background(), key)[0]
		}()
	}
	ready.Wait()
	close(start)
	for range callers {
		if result := <-results; result.Err != nil || !result.Found {
			t.Fatalf("batched lookup = %#v", result)
		}
	}
	backend.mu.Lock()
	calls := append([]lookupCall(nil), backend.calls...)
	backend.mu.Unlock()
	if len(calls) != 1 || len(calls[0].keys) != callers {
		t.Fatalf("cross-call batches = %#v", calls)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLookupClientKeepsCallerCancellationIndependent(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	key := PrimaryKey{"shared", int32(1)}
	putLookupValue(t, backend, table, key, Row{"shared", int32(1), "one"})
	release := make(chan struct{})
	backend.block = release
	backend.entered = make(chan struct{})
	client, err := newLookupClient(
		context.Background(), backend, table,
		WithLookupBatch(2, 1),
		WithLookupQueue(2, time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, cancel := context.WithCancel(context.Background())
	first := make(chan LookupResult, 1)
	second := make(chan LookupResult, 1)
	go func() { first <- client.Lookup(firstCtx, key)[0] }()
	go func() { second <- client.Lookup(context.Background(), key)[0] }()
	<-backend.entered
	cancel()
	if result := <-first; !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("canceled caller = %#v", result)
	}
	close(release)
	if result := <-second; result.Err != nil || !result.Found {
		t.Fatalf("independent caller = %#v", result)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLookupClientBoundsQueuedAndInflightKeys(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	release := make(chan struct{})
	backend.block = release
	backend.entered = make(chan struct{})
	client, err := newLookupClient(
		context.Background(), backend, table,
		WithLookupBatch(1, 1),
		WithLookupQueue(2, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan LookupResult, 2)
	go func() {
		results <- client.Lookup(context.Background(), PrimaryKey{"a", int32(1)})[0]
	}()
	<-backend.entered
	go func() {
		results <- client.Lookup(context.Background(), PrimaryKey{"b", int32(2)})[0]
	}()
	waitForTestCondition(t, "lookup queue saturation", func() bool {
		return len(client.slots) == 2
	})

	ctx, cancel := context.WithCancel(context.Background())
	third := make(chan LookupResult, 1)
	go func() {
		third <- client.Lookup(ctx, PrimaryKey{"c", int32(3)})[0]
	}()
	cancel()
	if result := <-third; !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("saturated lookup = %#v", result)
	}
	close(release)
	for range 2 {
		<-results
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLookupClientSchedulesIndependentBatchesFairly(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	release := make(chan struct{})
	backend.block = release
	client, err := newLookupClient(
		context.Background(), backend, table,
		WithLookupBatch(1, 2), WithLookupQueue(2, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan LookupResult, 2)
	for index := range 2 {
		go func() {
			results <- client.Lookup(
				context.Background(), PrimaryKey{"fair", int32(index)},
			)[0]
		}()
	}
	waitForTestCondition(t, "two active lookup batches", func() bool {
		backend.mu.Lock()
		defer backend.mu.Unlock()
		return backend.maxActive == 2
	})
	close(release)
	for range 2 {
		<-results
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLookupClientRetriesOnlyReadOnlyRequests(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	key := PrimaryKey{"retry", int32(1)}
	putLookupValue(t, backend, table, key, Row{"retry", int32(1), "one"})
	backend.lookupErrs = []error{
		responseServerError(
			int32(fmsg.ErrorCodeRequestTimeOut), "retry", fmsg.APIKeyLookup,
		),
		nil,
	}
	client, err := newLookupClient(
		context.Background(), backend, table,
		WithLookupBatch(1, 1),
		WithLookupQueue(1, 0),
		WithLookupRetryPolicy(RetryPolicy{MaxAttempts: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}
	metrics := &metricRecorder{}
	client.observer = metrics
	if result := client.Lookup(context.Background(), key)[0]; result.Err != nil || !result.Found {
		t.Fatalf("retried lookup = %#v", result)
	}
	if len(backend.calls) != 2 {
		t.Fatalf("lookup attempts = %d", len(backend.calls))
	}
	if event, ok := metrics.find(MetricRetry, MetricOperationLookup); !ok ||
		event.Attempt != 2 || !event.Failed {
		t.Fatalf("lookup retry metric = %#v, %t", event, ok)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = newLookupClient(
		context.Background(), lookupBackendFor(table, 0, 1), table,
		WithLookupInsertIfNotExists(time.Second, -1),
		WithLookupRetryPolicy(RetryPolicy{MaxAttempts: 2}),
	)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("insert retry config error = %v", err)
	}
}

func TestLookupClientRequestTimeout(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	backend.block = make(chan struct{})
	client, err := newLookupClient(
		context.Background(), backend, table,
		WithLookupBatch(1, 1), WithLookupQueue(1, 0),
		WithLookupTimeout(10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	result := client.Lookup(context.Background(), PrimaryKey{"a", int32(1)})[0]
	if !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf("timed out lookup = %#v", result)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLookupClientCloseCompletesQueuedAndInflight(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	backend.block = make(chan struct{})
	backend.entered = make(chan struct{})
	client, err := newLookupClient(
		context.Background(), backend, table,
		WithLookupBatch(1, 1), WithLookupQueue(3, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan LookupResult, 3)
	for index := range 3 {
		go func() {
			results <- client.Lookup(
				context.Background(), PrimaryKey{"close", int32(index)},
			)[0]
		}()
	}
	<-backend.entered
	waitForTestCondition(t, "all lookup slots", func() bool {
		return len(client.slots) == 3
	})
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		result := <-results
		if !errors.Is(result.Err, context.Canceled) && !errors.Is(result.Err, ErrClosed) {
			t.Fatalf("close result = %#v", result)
		}
	}
	if len(client.slots) != 0 {
		t.Fatalf("lookup slots after close = %d", len(client.slots))
	}
}

func TestLookupInsertIfNotExists(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	key := PrimaryKey{"new", int32(1)}
	putLookupValue(t, backend, table, key, Row{"new", int32(1), nil})
	client, err := newLookupClient(
		context.Background(), backend, table,
		WithLookupInsertIfNotExists(2*time.Second, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	const callers = 16
	results := make(chan LookupResult, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- client.Lookup(context.Background(), key)[0]
		}()
	}
	wait.Wait()
	close(results)
	for result := range results {
		if result.Err != nil || !result.Found || result.Row[0] != "new" {
			t.Fatalf("insert lookup result = %#v", result)
		}
	}
	for _, call := range backend.calls {
		if !call.insert || call.timeout != 2*time.Second || call.acks != 1 {
			t.Fatalf("insert lookup call = %#v", call)
		}
	}
	prefix := client.PrefixLookup(context.Background(), PrimaryKey{"new"})
	if len(prefix) != 1 || !errors.Is(prefix[0].Err, ErrInvalidConfig) {
		t.Fatalf("insert prefix result = %#v", prefix)
	}
	_ = client.Close()
}

func TestDecodeLookupValueRejectsInvalidEnvelope(t *testing.T) {
	table := lookupTable()
	if _, err := decodeLookupValue(table, []byte{1}); !errors.Is(err, ErrMalformedRow) {
		t.Fatalf("short lookup value error = %v", err)
	}
	value := encodeLookupValue(t, table, Row{"a", int32(1), "one"})
	binary.LittleEndian.PutUint16(value, uint16(table.SchemaID+1))
	if _, err := decodeLookupValue(table, value); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("schema mismatch error = %v", err)
	}
}

func TestPrefixLookupClientDecodesRows(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	key, err := EncodePrefixLookupKey(table.Schema, PrimaryKey{"a"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []Row{{"a", int32(1), "one"}, {"a", int32(2), "two"}} {
		value := encodeLookupValue(t, table, row)
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

func TestPrefixLookupClientBatchesConcurrentCalls(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	prefix := PrimaryKey{"a"}
	encoded, err := EncodePrefixLookupKey(table.Schema, prefix, 1)
	if err != nil {
		t.Fatal(err)
	}
	backend.prefixes[string(encoded)] = [][]byte{
		encodeLookupValue(t, table, Row{"a", int32(1), "one"}),
	}
	const callers = 4
	client, err := newLookupClient(
		context.Background(), backend, table,
		WithLookupBatch(callers, 1), WithLookupQueue(callers, time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan PrefixLookupResult, callers)
	for range callers {
		go func() {
			results <- client.PrefixLookup(context.Background(), prefix)[0]
		}()
	}
	for range callers {
		if result := <-results; result.Err != nil || len(result.Rows) != 1 {
			t.Fatalf("batched prefix lookup = %#v", result)
		}
	}
	backend.mu.Lock()
	calls := append([]lookupCall(nil), backend.calls...)
	backend.mu.Unlock()
	if len(calls) != 1 || !calls[0].prefix || len(calls[0].keys) != callers {
		t.Fatalf("prefix batches = %#v", calls)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
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
	entered := make(chan struct{})
	backend := lookupBackendFor(table, 0, 1)
	backend.block = release
	backend.entered = entered
	client, err := newLookupClient(context.Background(), backend, table)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan []LookupResult, 1)
	go func() { done <- client.Lookup(context.Background(), PrimaryKey{"a", int32(1)}) }()
	<-entered
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
	requiredValue := table
	requiredValue.Schema.Columns = append([]Column(nil), table.Schema.Columns...)
	requiredValue.Schema.Columns[2].Nullable = false
	for _, test := range []struct {
		name    string
		table   Table
		backend *fakeLookupBackend
		options []LookupOption
		target  error
	}{
		{"log table", logTable, lookupBackendFor(table, 0), nil, ErrTableKind},
		{"nil option", table, lookupBackendFor(table, 0), []LookupOption{nil}, ErrInvalidConfig},
		{"bad partition", table, lookupBackendFor(table, 0), []LookupOption{WithLookupPartition("   ")}, ErrInvalidConfig},
		{"bad batch", table, lookupBackendFor(table, 0), []LookupOption{WithLookupBatch(0, 0)}, ErrInvalidConfig},
		{"bad queue", table, lookupBackendFor(table, 0), []LookupOption{WithLookupQueue(0, -1)}, ErrInvalidConfig},
		{"bad retry", table, lookupBackendFor(table, 0), []LookupOption{WithLookupRetryPolicy(RetryPolicy{})}, ErrInvalidConfig},
		{"bad timeout", table, lookupBackendFor(table, 0), []LookupOption{WithLookupTimeout(0)}, ErrInvalidConfig},
		{"bad insert request", table, lookupBackendFor(table, 0), []LookupOption{WithLookupInsertIfNotExists(0, 2)}, ErrInvalidConfig},
		{"required insert value", requiredValue, lookupBackendFor(requiredValue, 0), []LookupOption{WithLookupInsertIfNotExists(time.Second, -1)}, ErrInvalidSchema},
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
	var config LookupConfig
	if err := WithLookupPartition("day=2026-07-30")(&config); err != nil ||
		config.Partition != "day=2026-07-30" {
		t.Fatalf("WithLookupPartition() = %#v, %v", config, err)
	}
}

func TestClientLookupBackendMessagesAndErrors(t *testing.T) {
	table := lookupTable()
	path := table.Path
	pointValue := encodeLookupValue(t, table, Row{"a", int32(1), "one"})
	var pointRequests []*fmsg.LookupRequest
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
				pointRequest := request.(*fmsg.MessageRequest).Message().(*fmsg.LookupRequest)
				pointRequests = append(pointRequests, pointRequest)
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
	backend := clientLookupBackend{client: client}
	if _, err := backend.lookup(context.Background(), lookupRequest{
		path: PhysicalTablePath{TablePath: path}, bucket: 0, tableID: table.ID, partitionID: -1,
		keys: [][]byte{{1}}, insertIfNotExist: true, timeout: 2 * time.Second, acks: -1,
	}); err != nil {
		t.Fatal(err)
	}
	if len(pointRequests) != 2 || pointRequests[0].InsertIfNotExists != nil ||
		pointRequests[0].Acks != nil || pointRequests[0].TimeoutMs != nil ||
		!pointRequests[1].GetInsertIfNotExists() || pointRequests[1].GetAcks() != -1 ||
		pointRequests[1].GetTimeoutMs() != 2000 ||
		pointRequests[1].GetTableId() != table.ID ||
		len(pointRequests[1].GetBucketsReq()[0].GetKeys()) != 1 ||
		prefixRequest.GetTableId() != table.ID {
		t.Fatalf("requests = %#v / %#v", pointRequests, prefixRequest)
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
	if _, err := backend.lookup(context.Background(), lookupRequest{
		path: path, bucket: 0, tableID: table.ID, partitionID: -1, keys: [][]byte{{1}},
	}); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("lookup body error = %v", err)
	}
	if _, err := backend.prefixLookup(context.Background(), path, 0, table.ID, -1, [][]byte{{1}}); !errors.Is(err, ErrValidation) {
		t.Fatalf("prefix count error = %v", err)
	}
}

func BenchmarkLookupCrossCallBatching(b *testing.B) {
	for _, test := range []struct {
		name  string
		batch int
		delay time.Duration
	}{
		{"immediate", 1, 0},
		{"batch_64", 64, 100 * time.Microsecond},
	} {
		b.Run(test.name, func(b *testing.B) {
			table := lookupTable()
			backend := lookupBackendFor(table, 0, 1)
			backend.delay = 100 * time.Microsecond
			key := PrimaryKey{"bench", int32(1)}
			putLookupValue(b, backend, table, key, Row{"bench", int32(1), "one"})
			client, err := newLookupClient(
				context.Background(), backend, table,
				WithLookupBatch(test.batch, 8),
				WithLookupQueue(1024, test.delay),
			)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := client.Close(); err != nil {
					b.Error(err)
				}
			})
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(parallel *testing.PB) {
				for parallel.Next() {
					result := client.Lookup(context.Background(), key)[0]
					if result.Err != nil {
						b.Error(result.Err)
						return
					}
				}
			})
		})
	}
}
