package fgo

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestSchemaCacheCoalescesFetchesAndSeparatesWaiterCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	cache := newSchemaCache(4, func(ctx context.Context, _ TablePath, _ int32) (Schema, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return evolutionSchema("name", true, StringType), nil
		case <-ctx.Done():
			return Schema{}, ctx.Err()
		}
	})
	path := TablePath{Database: "db", Table: "events"}
	type result struct {
		schema Schema
		err    error
	}
	results := make(chan result, 7)
	canceledResult := make(chan result, 1)
	canceled, cancel := context.WithCancel(context.Background())
	go func() {
		schema, err := cache.resolveSchema(canceled, path, 1)
		canceledResult <- result{schema: schema, err: err}
	}()
	for range 7 {
		go func() {
			schema, err := cache.resolveSchema(context.Background(), path, 1)
			results <- result{schema: schema, err: err}
		}()
	}
	<-started
	waitForCoalescerWaiters(t, cache.flights, schemaCacheKey{path: path, id: 1}, 8)
	cancel()
	if result := <-canceledResult; !errors.Is(result.err, context.Canceled) {
		t.Fatalf("canceled resolve = %#v", result)
	}
	close(release)
	var successfulResults int
	for range 7 {
		result := <-results
		switch {
		case result.err == nil && len(result.schema.Columns) == 2:
			successfulResults++
		default:
			t.Fatalf("resolve result = %#v", result)
		}
	}
	if calls.Load() != 1 || successfulResults != 7 {
		t.Fatalf("calls/successful = %d/%d", calls.Load(), successfulResults)
	}
}

func TestSchemaCacheCancelsUnobservedFetchAndEvictsLRU(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	fetchCanceled := make(chan struct{})
	canceling := newSchemaCache(1, func(ctx context.Context, _ TablePath, _ int32) (Schema, error) {
		<-ctx.Done()
		close(fetchCanceled)
		return Schema{}, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := canceling.resolveSchema(ctx, path, 1)
		result <- err
	}()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled resolve = %v", err)
	}
	select {
	case <-fetchCanceled:
	case <-time.After(time.Second):
		t.Fatal("unobserved schema fetch was not canceled")
	}

	var mu sync.Mutex
	calls := make(map[int32]int)
	cache := newSchemaCache(2, func(_ context.Context, _ TablePath, id int32) (Schema, error) {
		mu.Lock()
		calls[id]++
		mu.Unlock()
		return evolutionSchema("name", true, StringType), nil
	})
	for _, id := range []int32{1, 2, 1, 3, 2} {
		if _, err := cache.resolveSchema(context.Background(), path, id); err != nil {
			t.Fatal(err)
		}
	}
	if calls[1] != 1 || calls[2] != 2 || calls[3] != 1 {
		t.Fatalf("fetch calls = %#v", calls)
	}
	cache.close()
	if _, err := cache.resolveSchema(context.Background(), path, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed cache error = %v", err)
	}
}

func TestSchemaCacheRejectsInvalidInputsAndMalformedSchemas(t *testing.T) {
	cache := newSchemaCache(1, func(context.Context, TablePath, int32) (Schema, error) {
		return Schema{}, nil
	})
	path := TablePath{Database: "db", Table: "events"}
	if _, err := cache.resolveSchema(nil, path, 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := cache.resolveSchema(context.Background(), TablePath{}, 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid path error = %v", err)
	}
	if _, err := cache.resolveSchema(context.Background(), path, -1); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("negative schema ID error = %v", err)
	}
	if _, err := cache.resolveSchema(context.Background(), path, 1); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("malformed fetched schema error = %v", err)
	}
}

func TestEvolveRowSupportsCompatibleSchemaChanges(t *testing.T) {
	source := evolutionSchema("old_name", true, StringType)
	target := Schema{
		Columns: []Column{
			{Name: "id", Type: IntType, ID: 1},
			{Name: "new_name", Type: StringType, Nullable: true, ID: 2},
			{Name: "added", Type: BigIntType, Nullable: true, ID: 3},
		},
		HighestFieldID: 3,
	}
	row, err := evolveRow(source, target, Row{int32(7), "value"})
	if err != nil || len(row) != 3 || row[0] != int32(7) || row[1] != "value" || row[2] != nil {
		t.Fatalf("evolved row = %#v, %v", row, err)
	}
	dropped := Schema{
		Columns:        []Column{{Name: "id", Type: IntType, ID: 1}},
		HighestFieldID: 3,
	}
	row, err = evolveRow(target, dropped, Row{int32(8), "renamed", int64(9)})
	if err != nil || len(row) != 1 || row[0] != int32(8) {
		t.Fatalf("dropped row = %#v, %v", row, err)
	}
}

func TestEvolveRowRejectsIncompatibleChanges(t *testing.T) {
	source := evolutionSchema("name", true, StringType)
	for name, target := range map[string]Schema{
		"required add": {
			Columns: []Column{
				{Name: "id", Type: IntType, ID: 1},
				{Name: "name", Type: StringType, Nullable: true, ID: 2},
				{Name: "required", Type: BigIntType, ID: 3},
			},
			HighestFieldID: 3,
		},
		"type change": evolutionSchema("name", true, BytesType),
		"not null":    evolutionSchema("name", false, StringType),
	} {
		t.Run(name, func(t *testing.T) {
			row := Row{int32(1), "value"}
			if name == "not null" {
				row[1] = nil
			}
			if _, err := evolveRow(source, target, row); !errors.Is(err, ErrInvalidSchema) {
				t.Fatalf("evolve error = %v", err)
			}
		})
	}
	if _, err := evolveRow(source, source, Row{int32(1)}); !errors.Is(err, ErrMalformedRow) {
		t.Fatalf("short source row error = %v", err)
	}
}

func TestHistoricalSchemasDecodeAcrossReadPaths(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	oldSchema := evolutionSchema("old_name", true, StringType)
	currentSchema := Schema{
		Columns: []Column{
			{Name: "id", Type: IntType, ID: 1},
			{Name: "name", Type: StringType, Nullable: true, ID: 2},
			{Name: "extra", Type: BigIntType, Nullable: true, ID: 3},
		},
		HighestFieldID: 3,
	}
	table := Table{ID: 9, SchemaID: 2, Path: path, Kind: LogTable, Schema: currentSchema}
	resolver := &mapSchemaResolver{schemas: map[int32]Schema{1: oldSchema, 2: currentSchema}}

	oldEncoded, err := EncodeCompactedRow(oldSchema, Row{int32(1), "old"})
	if err != nil {
		t.Fatal(err)
	}
	lookupValue := append([]byte{1, 0}, oldEncoded...)
	row, err := decodeLookupValueWithResolver(context.Background(), resolver, table, lookupValue)
	if err != nil || len(row) != 3 || row[1] != "old" || row[2] != nil {
		t.Fatalf("historical lookup = %#v, %v", row, err)
	}

	currentEncoded, err := EncodeCompactedRow(currentSchema, Row{int32(2), "new", int64(4)})
	if err != nil {
		t.Fatal(err)
	}
	valueBatch := encodedSchemaValueBatch(
		[]schemaValue{{id: 1, row: oldEncoded}, {id: 2, row: currentEncoded}},
	)
	rows, err := decodeValueRecordBatchWithResolver(context.Background(), resolver, table, valueBatch)
	if err != nil || len(rows) != 2 || rows[0][2] != nil || rows[1][2] != int64(4) {
		t.Fatalf("historical value batch = %#v, %v", rows, err)
	}

	oldLog, err := (LogBatch{
		Magic: 0, BaseOffset: 0, SchemaID: 1, AppendOnly: true,
		Records: []Record{{Value: Row{int32(1), "old"}, Change: Append}},
	}).EncodeRows(oldSchema, true)
	if err != nil {
		t.Fatal(err)
	}
	currentLog, err := (LogBatch{
		Magic: 0, BaseOffset: 1, SchemaID: 2, AppendOnly: true,
		Records: []Record{{Value: Row{int32(2), "new", int64(4)}, Change: Append}},
	}).EncodeRows(currentSchema, true)
	if err != nil {
		t.Fatal(err)
	}
	next, records, arrows, err := decodeFetchedLogWithResolver(
		context.Background(), resolver, table, 0, 0, append(oldLog, currentLog...), true,
	)
	releaseScanArrows(arrows)
	if err != nil || next != 2 || len(records) != 2 ||
		records[0].Record.Value[2] != nil || records[1].Record.Value[2] != int64(4) {
		t.Fatalf("historical log = next %d, records %#v, %v", next, records, err)
	}
}

func TestJava091SchemaFixturesResolveByID(t *testing.T) {
	fixtures := map[int32][]byte{
		1: []byte(`{"version":1,"columns":[{"name":"id","data_type":{"type":"INTEGER","nullable":false},"id":1},{"name":"old_name","data_type":{"type":"STRING","nullable":true},"id":2}],"highest_field_id":2}`),
		2: []byte(`{"version":1,"columns":[{"name":"id","data_type":{"type":"INTEGER","nullable":false},"id":1},{"name":"name","data_type":{"type":"STRING","nullable":true},"id":2},{"name":"extra","data_type":{"type":"BIGINT","nullable":true},"id":3}],"highest_field_id":3}`),
	}
	cache := newSchemaCache(4, func(_ context.Context, _ TablePath, id int32) (Schema, error) {
		return ParseSchemaJSON(fixtures[id])
	})
	path := TablePath{Database: "db", Table: "events"}
	for _, id := range []int32{1, 2, 1} {
		schema, err := cache.resolveSchema(context.Background(), path, id)
		if err != nil || schema.HighestFieldID != int(id+1) {
			t.Fatalf("schema %d = %#v, %v", id, schema, err)
		}
	}
}

func TestFetchSchemaValidatesResponseIDAndEmitsRequestMetric(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	var events []MetricEvent
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		message := request.(*fmsg.MessageRequest).Message().(*fmsg.GetTableSchemaRequest)
		if message.GetSchemaId() != 1 || message.GetTablePath().GetDatabaseName() != "db" {
			t.Fatalf("schema request = %#v", message)
		}
		response, err := fmsg.NewResponse(request.APIKey(), request.Version())
		if err != nil {
			return nil, err
		}
		schema := response.Message().(*fmsg.GetTableSchemaResponse)
		schema.SchemaId = proto.Int32(2)
		schema.SchemaJson = []byte(`{"version":1,"columns":[{"name":"id","data_type":{"type":"INTEGER"},"id":1}],"highest_field_id":1}`)
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetTableSchema] = 0
	client.observer = MetricsObserverFunc(func(event MetricEvent) {
		events = append(events, event)
	})
	if _, err := client.fetchSchema(context.Background(), path, 1); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("mismatched schema response error = %v", err)
	}
	if len(events) != 1 || events[0].Kind != MetricRequest ||
		events[0].APIKey != fmsg.APIKeyGetTableSchema || events[0].Failed {
		t.Fatalf("schema request metrics = %#v", events)
	}
}

type mapSchemaResolver struct {
	schemas map[int32]Schema
}

func (r *mapSchemaResolver) resolveSchema(
	_ context.Context,
	_ TablePath,
	id int32,
) (Schema, error) {
	schema, ok := r.schemas[id]
	if !ok {
		return Schema{}, ErrInvalidSchema
	}
	return schema, nil
}

func evolutionSchema(name string, nullable bool, dataType DataType) Schema {
	return Schema{
		Columns: []Column{
			{Name: "id", Type: IntType, ID: 1},
			{Name: name, Type: dataType, Nullable: nullable, ID: 2},
		},
		HighestFieldID: 2,
	}
}

type schemaValue struct {
	id  int16
	row []byte
}

func encodedSchemaValueBatch(values []schemaValue) []byte {
	size := 9
	for _, value := range values {
		size += 6 + len(value.row)
	}
	encoded := make([]byte, size)
	binary.LittleEndian.PutUint32(encoded, uint32(size-4))
	binary.LittleEndian.PutUint32(encoded[5:], uint32(len(values)))
	position := 9
	for _, value := range values {
		binary.LittleEndian.PutUint32(encoded[position:], uint32(len(value.row)+2))
		binary.LittleEndian.PutUint16(encoded[position+4:], uint16(value.id))
		copy(encoded[position+6:], value.row)
		position += 6 + len(value.row)
	}
	return encoded
}
