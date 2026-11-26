package fgo

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"
)

func partitionedKVTable() Table {
	table := upsertWriterTable()
	table.Schema = Schema{
		Columns: []Column{
			{Name: "region", Type: StringType},
			{Name: "tenant", Type: IntType},
			{Name: "session", Type: IntType},
			{Name: "name", Type: StringType, Nullable: true},
		},
		PrimaryKey:   []string{"region", "tenant", "session"},
		PartitionKey: []string{"region"},
		BucketKey:    []string{"tenant"},
	}
	return table
}

func TestPartitionedKVWriterPhysicalKeys(t *testing.T) {
	ctx := t.Context()
	table := partitionedKVTable()
	buckets := []int32{0, 1, 2, 3, 4}
	table.BucketCount = len(buckets)
	backend := kvBackend(buckets...)
	backend.partitionID = 55
	writer, err := newUpsertWriter(ctx, backend, table, WithUpsertPartition("apac"), WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	for _, future := range []*WriteFuture{
		writer.Upsert(ctx, Row{"apac", int32(7), int32(42), "name"}),
		writer.PartialUpsert(ctx, []string{"session", "tenant", "region", "name"}, Row{int32(42), int32(7), "apac", "updated"}),
		writer.Delete(ctx, PrimaryKey{"apac", int32(7), int32(42)}),
	} {
		if result := future.Await(ctx); result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	for _, future := range []*WriteFuture{
		writer.Upsert(ctx, Row{"eu", int32(7), int32(42), "name"}),
		writer.PartialUpsert(ctx, []string{"region", "tenant", "session"}, Row{"eu", int32(7), int32(42)}),
		writer.Delete(ctx, PrimaryKey{"eu", int32(7), int32(42)}),
	} {
		if result := future.Await(ctx); !errors.Is(result.Err, ErrInvalidRow) {
			t.Errorf("wrong partition error = %v", result.Err)
		}
	}
	wantBucket, _ := flussBucket([]byte{7}, table.BucketCount)
	calls := backend.putCalls()
	if len(calls) != 3 {
		t.Fatalf("got %d put requests; want three", len(calls))
	}
	for i, call := range calls {
		batch, err := DecodeKVBatch(call.records)
		if err != nil || len(batch.Records) != 1 {
			t.Fatalf("mutation %d batch = %#v, %v", i, batch, err)
		}
		record := batch.Records[0]
		if call.partitionID != 55 || call.bucket != wantBucket || !bytes.Equal(record.Key, []byte{7, 42}) {
			t.Errorf("mutation %d partition=%d bucket=%d key=%x", i, call.partitionID, call.bucket, record.Key)
		}
		if i != 2 {
			row, err := DecodeCompactedRow(table.Schema, record.Value)
			if err != nil || row[0] != "apac" {
				t.Errorf("mutation %d value row = %v, %v", i, row, err)
			}
		} else if record.Value != nil {
			t.Error("delete contains a row value")
		}
	}
}

func TestPartitionedKVLookupPhysicalKeys(t *testing.T) {
	ctx := t.Context()
	table := partitionedKVTable()
	// Put the partition column between two physical key columns.
	table.Schema.PrimaryKey = []string{"tenant", "region", "session"}
	backend := lookupBackendFor(table, 0)
	value := encodeLookupValue(t, table, Row{"apac", int32(7), int32(42), "name"})
	backend.values[string([]byte{7, 42})] = value
	backend.prefixes[string([]byte{7})] = [][]byte{value}
	lookup, err := newLookuper(ctx, backend, table, WithLookupPartition("apac"))
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	if result := lookup.Lookup(ctx, PrimaryKey{int32(7), "apac", int32(42)})[0]; result.Err != nil || !result.Found {
		t.Errorf("point = %#v", result)
	}
	if result := lookup.Lookup(ctx, PrimaryKey{int32(7), "eu", int32(42)})[0]; !errors.Is(result.Err, ErrInvalidRow) {
		t.Errorf("wrong partition point = %#v", result)
	}
	prefixes := lookup.PrefixLookup(ctx, PrimaryKey{int32(7)}, PrimaryKey{int32(7), "apac"}, PrimaryKey{int32(7), "eu"})
	for _, result := range prefixes[:2] {
		if result.Err != nil || len(result.Rows) != 1 {
			t.Errorf("prefix = %#v", result)
		}
	}
	if !errors.Is(prefixes[2].Err, ErrInvalidRow) {
		t.Errorf("wrong partition prefix = %#v", prefixes[2])
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	sent := 0
	for _, call := range backend.calls {
		sent += len(call.keys)
	}
	if sent != 3 {
		t.Errorf("sent %d keys; want one valid point and two valid prefixes", sent)
	}
}

func TestPartitionedKVDefaultBucketKey(t *testing.T) {
	schema := partitionedKVTable().Schema
	schema.BucketKey = nil
	layout, err := newKVKeyLayout(schema, "apac")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{7, 42}
	bucket, _ := flussBucket(want, 17)
	key, got, err := layout.encodePoint(schema, map[string]any{"region": "apac", "tenant": int32(7), "session": int32(42)}, 17)
	if err != nil || !bytes.Equal(key, want) || got != bucket {
		t.Errorf("point = %x, %d, %v; want %x, %d", key, got, err, want, bucket)
	}
	if _, _, err := layout.encodePrefix(schema, map[string]any{"region": "apac", "tenant": int32(7)}, 17); !errors.Is(err, ErrInvalidRow) {
		t.Errorf("short prefix error = %v", err)
	}
	if _, err := newKVKeyLayout(schema, ""); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("missing partition error = %v", err)
	}
	schema.BucketKey = []string{"region"}
	if _, err := newKVKeyLayout(schema, "apac"); !errors.Is(err, ErrInvalidSchema) {
		t.Errorf("partition column in bucket key error = %v", err)
	}
}

func TestKVPartitionColumnValueMatching(t *testing.T) {
	for _, test := range []struct {
		kind            DataType
		name            string
		match, mismatch any
	}{
		{BoolType, "false", false, "false"},
		{TinyIntType, "-128", int8(-128), int32(-128)},
		{IntType, "42", int32(42), int32(41)},
		{BytesType, "80FF", []byte{0x80, 0xff}, []byte{0x80}},
		{FloatType, "1_25E-3", float32(0.00125), float32(0.125)},
		{DoubleType, "NaN", math.NaN(), math.Inf(1)},
		{DoubleType, "-0_0", math.Copysign(0, -1), float64(0)},
		{DateType, "2026-07-16", time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC), time.Time{}},
		{TimeType, "12-34-56_789", time.Date(2026, 7, 16, 12, 34, 56, 789000000, time.UTC), time.Time{}},
		{TimestampType, "2026-07-16-12-34-56_123456789", time.Date(2026, 7, 16, 12, 34, 56, 123456789, time.FixedZone("KST", 9*3600)), time.Time{}},
		{TimestampLTZType, "2026-07-16-03-34-56_", time.Date(2026, 7, 16, 12, 34, 56, 0, time.FixedZone("KST", 9*3600)), time.Time{}},
	} {
		t.Run(string(test.kind)+"/"+test.name, func(t *testing.T) {
			value, err := parseKVPartitionValue(test.kind, test.name)
			if err != nil {
				t.Fatal(err)
			}
			column := physicalPartitionColumn{kind: test.kind, value: value}
			if !column.matches(test.match) || column.matches(test.mismatch) || column.matches([]int{1}) {
				t.Errorf("unexpected comparison against %v", value)
			}
		})
	}
}
