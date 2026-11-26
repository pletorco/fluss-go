package fgo

import (
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestAppendArrowRejectsEntireBatchOnPartitionMismatch(t *testing.T) {
	ctx := t.Context()
	table := appendWriterTable()
	table.Properties["table.log.format"] = "ARROW"
	table.Schema.PartitionKey = []string{"id", "name"}
	name, err := table.Schema.PartitionName(PartitionSpec{"id": "7", "name": "east"})
	if err != nil {
		t.Fatal(err)
	}
	backend := logBackend(0)
	writer, err := newAppendWriter(ctx, backend, table, WithAppendPartition(name), WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	schema, err := table.Schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, tail, column string
	}{
		{"wrong name", `{"id":7,"name":"west"}`, "name"},
		{"wrong id", `{"id":8,"name":"east"}`, "id"},
		{"nullable partition value", `{"id":7,"name":null}`, "name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := arrowPartitionTestRecord(t, schema, `{"id":7,"name":"east"}`, tc.tail)
			defer r.Release()
			got := writer.AppendArrow(ctx, 0, r, []ChangeType{Append, Append}).Await(ctx)
			if !errors.Is(got.Err, ErrInvalidRow) || !strings.Contains(got.Err.Error(), tc.column) {
				t.Fatalf("got %v, want partition mismatch for %s", got.Err, tc.column)
			}
		})
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(backend.produced()); got != 0 {
		t.Fatalf("dispatched %d invalid batches", got)
	}
	record := arrowPartitionTestRecord(t, schema, `{"id":7,"name":"east"}`)
	defer record.Release()
	if got := writer.AppendArrow(ctx, 0, record, []ChangeType{Append}).Await(ctx); got.Err != nil || got.Records != 1 {
		t.Fatalf("valid append = %+v", got)
	}
	if got := len(backend.produced()); got != 1 {
		t.Fatalf("dispatched %d batches, want 1", got)
	}
}

func arrowPartitionTestRecord(t *testing.T, schema *arrow.Schema, rows ...string) arrow.RecordBatch {
	t.Helper()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for _, row := range rows {
		if err := builder.UnmarshalJSON([]byte(row)); err != nil {
			t.Fatal(err)
		}
	}
	return builder.NewRecordBatch()
}

func TestArrowPartitionScalarTypes(t *testing.T) {
	for _, tc := range []struct {
		kind         DataType
		dataType     arrow.DataType
		name, values string
	}{
		{BoolType, arrow.FixedWidthTypes.Boolean, "true", `[true,false]`},
		{TinyIntType, arrow.PrimitiveTypes.Int8, "-7", `[-7,7]`},
		{SmallIntType, arrow.PrimitiveTypes.Int16, "273", `[273,274]`},
		{IntType, arrow.PrimitiveTypes.Int32, "65536", `[65536,65537]`},
		{BigIntType, arrow.PrimitiveTypes.Int64, "4294967296", `[4294967296,4294967297]`},
		{FloatType, arrow.PrimitiveTypes.Float32, "-0_0", `[-0.0,0.0]`},
		{DoubleType, arrow.PrimitiveTypes.Float64, "0_125", `[0.125,0.25]`},
		{StringType, arrow.BinaryTypes.String, "east", `["east","west"]`},
		{CharType, arrow.BinaryTypes.String, "east", `["east","west"]`},
		{BytesType, arrow.BinaryTypes.Binary, "07", `["Bw==","CA=="]`},
		{BinaryType, &arrow.FixedSizeBinaryType{ByteWidth: 1}, "07", `["Bw==","CA=="]`},
		{DateType, arrow.FixedWidthTypes.Date32, "1969-12-31", `[-1,0]`},
		{TimeType, &arrow.Time32Type{Unit: arrow.Millisecond}, "00-00-00_000", `[0,86400000]`},
		{TimeType, &arrow.Time64Type{Unit: arrow.Microsecond}, "00-00-00_000", `[0,1,-1]`},
		{TimestampType, &arrow.TimestampType{Unit: arrow.Nanosecond}, "1969-12-31-23-59-59_999999999", `[-1,0]`},
		{TimestampLTZType, &arrow.TimestampType{Unit: arrow.Millisecond}, "1970-01-01", `[0,1]`},
	} {
		t.Run(string(tc.kind)+"/"+tc.dataType.String(), func(t *testing.T) {
			value, err := parseKVPartitionValue(tc.kind, tc.name)
			if err != nil {
				t.Fatal(err)
			}
			column, _, err := array.FromJSON(memory.DefaultAllocator, tc.dataType, strings.NewReader(tc.values))
			if err != nil {
				t.Fatal(err)
			}
			defer column.Release()
			partition := physicalPartitionColumn{kind: tc.kind, value: value}
			for row := 0; row < column.Len(); row++ {
				if got := matchesArrowPartitionValue(partition, column, row); got != (row == 0) {
					t.Errorf("value %s matches %q: got %v", column.ValueStr(row), tc.name, got)
				}
			}
		})
	}
}
