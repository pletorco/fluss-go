package fgo

import (
	"context"
	"encoding/binary"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestReaderKeepsCallerSchemaOutOfSharedCache(t *testing.T) {
	ctx := context.Background()
	client, path, calls := schemaOwnershipClient(t)
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	want := table.Schema.Columns[1].Name
	table.Schema.Columns[1].Name = "locally_renamed"
	scanner, err := client.NewBatchScanner(ctx, table, testTableBucket(table))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scanner.Close() })
	got, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Schema.Columns[1].Name != want || calls.Load() != 1 {
		t.Fatalf("server schema column = %q, fetches = %d; want %q and one fetch", got.Schema.Columns[1].Name, calls.Load(), want)
	}
}

func schemaOwnershipFixture() Schema {
	schema, _, _ := mutableAppendInput()
	schema.PrimaryKey, schema.BucketKey = []string{"id"}, []string{"id"}
	schema.PartitionKey, schema.AutoIncrement = []string{"id"}, []string{"id"}
	return schema
}

func mutateOwnedSchema(schema Schema) {
	schema.Columns[0].Name, schema.Columns[0].Type = "changed", StringType
	schema.Columns[0].Description, schema.Columns[0].ID = "changed", 99
	if schema.Columns[0].LogicalType != nil {
		schema.Columns[0].LogicalType.Root = "STRING"
	}
	schema.Columns[2].LogicalType.Precision = 1
	schema.Columns[3].LogicalType.Element.Fields[0].Type.Root = "STRING"
	schema.Columns[4].LogicalType.Key.Root = "STRING"
	schema.Columns[4].LogicalType.Value.Element.Root = "STRING"
	schema.Columns[5].LogicalType.Value.Root = "STRING"
	schema.Columns[6].LogicalType.Fields[0].Name = "changed"
	schema.Columns[6].LogicalType.Fields[0].Description = "changed"
	for _, keys := range [][]string{schema.PrimaryKey, schema.BucketKey, schema.PartitionKey, schema.AutoIncrement} {
		keys[0] = "changed"
	}
}

func TestSchemaCacheOwnsStoredAndFetchedMetadata(t *testing.T) {
	for _, mode := range []string{"store", "fetch"} {
		t.Run(mode, func(t *testing.T) {
			input, want := schemaOwnershipFixture(), schemaOwnershipFixture()
			path := TablePath{Database: "db", Table: "events"}
			var calls atomic.Int32
			cache := newSchemaCache(2, func(context.Context, TablePath, int64, int32) (Schema, error) {
				calls.Add(1)
				return input, nil
			})
			t.Cleanup(cache.close)
			if mode == "store" {
				cache.store(path, 9, 1, input)
			}
			first, err := cache.resolveSchema(context.Background(), path, 9, 1)
			if err != nil {
				t.Fatal(err)
			}
			mutateOwnedSchema(input)
			second, err := cache.resolveSchema(context.Background(), path, 9, 1)
			if err != nil || !reflect.DeepEqual(first, want) || !reflect.DeepEqual(second, want) {
				t.Fatalf("%s input changed cached metadata: first=%#v second=%#v err=%v", mode, first, second, err)
			}
			if &first.Columns[0] != &second.Columns[0] {
				t.Fatal("internal cache reads copied the immutable schema")
			}
			allocations := testing.AllocsPerRun(100, func() {
				if _, err := cache.resolveSchema(context.Background(), path, 9, 1); err != nil {
					t.Fatal(err)
				}
			})
			if allocations != 0 {
				t.Fatalf("cache hit allocated %.1f times; want no per-record metadata copies", allocations)
			}
			wantCalls := int32(1)
			if mode == "store" {
				wantCalls = 0
			}
			if calls.Load() != wantCalls {
				t.Fatalf("schema fetches=%d; want %d", calls.Load(), wantCalls)
			}
		})
	}
}

func schemaOwnershipClient(t testing.TB) (*Client, TablePath, *atomic.Int32) {
	t.Helper()
	schema := schemaOwnershipFixture()
	encoded, err := schema.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, err := fmsg.NewResponse(request.APIKey(), request.Version())
		if err != nil {
			return nil, err
		}
		switch message := response.Message().(type) {
		case *fmsg.GetTableInfoResponse:
			message.TableId, message.SchemaId = proto.Int64(9), proto.Int32(1)
			message.TableJson = []byte(`{"bucket_count":1,"bucket_key":["id"],"partition_key":["id"],"properties":{"purpose":"original"}}`)
		case *fmsg.GetTableSchemaResponse:
			calls.Add(1)
			message.SchemaId, message.SchemaJson = proto.Int32(1), encoded
		}
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetTableInfo], client.versions[fmsg.APIKeyGetTableSchema] = 0, 0
	t.Cleanup(func() { _ = client.Close() })
	return client, TablePath{Database: "db", Table: "events"}, &calls
}

func TestGetTableOwnsSchemaMetadata(t *testing.T) {
	for _, mutateHit := range []bool{false, true} {
		name := "initial fetch"
		if mutateHit {
			name = "cache hit"
		}
		t.Run(name, func(t *testing.T) {
			client, path, calls := schemaOwnershipClient(t)
			first, err := client.GetTable(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			borrowed, err := client.resolveSchema(context.Background(), path, 9, 1)
			if err != nil {
				t.Fatal(err)
			}
			second, err := client.GetTable(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			want := snapshotSchema(first.Schema)
			changed, other := first, second
			if mutateHit {
				changed, other = second, first
			}
			mutateOwnedSchema(changed.Schema)
			changed.Properties["purpose"] = "changed"
			third, err := client.GetTable(context.Background(), path)
			if err != nil || !reflect.DeepEqual(third.Schema, want) || !reflect.DeepEqual(other.Schema, want) {
				t.Fatalf("public mutation changed other metadata: next=%#v other=%#v err=%v", third.Schema, other.Schema, err)
			}
			cached, err := client.resolveSchema(context.Background(), path, 9, 1)
			if err != nil || !reflect.DeepEqual(cached, want) || !reflect.DeepEqual(borrowed, want) || calls.Load() != 1 {
				t.Fatalf("public mutation changed cache or refetched: schema=%#v calls=%d err=%v", cached, calls.Load(), err)
			}
			if third.Properties["purpose"] != "original" || other.Properties["purpose"] != "original" {
				t.Fatal("table properties alias another result")
			}
		})
	}
}

func TestGetTableMutationDoesNotRaceCachedReaders(t *testing.T) {
	client, path, calls := schemaOwnershipClient(t)
	mutable, err := client.GetTable(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := client.GetTable(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	want := snapshotSchema(reader.Schema)
	_, row, _ := mutableAppendInput()
	value, err := EncodeCompactedRow(reader.Schema, row)
	if err != nil {
		t.Fatal(err)
	}
	wantRow, err := DecodeCompactedRow(reader.Schema, value)
	if err != nil {
		t.Fatal(err)
	}
	encoded := make([]byte, 2+len(value))
	binary.LittleEndian.PutUint16(encoded, uint16(reader.SchemaID))
	copy(encoded[2:], value)
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		for range 100 {
			mutateOwnedSchema(mutable.Schema)
		}
	}()
	go func() {
		defer workers.Done()
		for range 100 {
			got, err := client.GetTable(context.Background(), path)
			if err != nil || !reflect.DeepEqual(got.Schema, want) {
				t.Errorf("concurrent GetTable metadata changed: %v", err)
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for range 100 {
			got, err := decodeLookupValueWithResolver(context.Background(), client, reader, encoded, compactedEncoding)
			if err != nil || !reflect.DeepEqual(got, wantRow) {
				t.Errorf("concurrent cached decode changed: got=%#v err=%v", got, err)
				return
			}
		}
	}()
	workers.Wait()
	if calls.Load() != 1 {
		t.Fatalf("cached readers fetched schema %d times", calls.Load())
	}
}

func TestGetTableCoalescedResultsOwnSchemaMetadata(t *testing.T) {
	client, path, calls := schemaOwnershipClient(t)
	base := client.requester
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	client.requester = requesterFunc(func(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
		if request.APIKey() == fmsg.APIKeyGetTableSchema {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return base.Request(ctx, request)
	})
	type tableResult struct {
		table Table
		err   error
	}
	const callers = 8
	results := make(chan tableResult, callers)
	for range callers {
		go func() {
			table, err := client.GetTable(context.Background(), path)
			results <- tableResult{table, err}
		}()
	}
	<-entered
	waitForCoalescerWaiters(t, client.schemas.flights, schemaCacheKey{path: path, tableID: 9, id: 1}, callers)
	unblock.Do(func() { close(release) })
	var tables []Table
	for range callers {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		tables = append(tables, result.table)
	}
	want := snapshotSchema(tables[0].Schema)
	mutateOwnedSchema(tables[0].Schema)
	for _, table := range tables[1:] {
		if !reflect.DeepEqual(table.Schema, want) {
			t.Fatal("coalesced GetTable results share mutable schema metadata")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("coalesced GetTable fetched schema %d times", calls.Load())
	}
}
