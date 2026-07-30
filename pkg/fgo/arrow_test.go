package fgo

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestArrowSchemaCoversFluss091LogicalTypes(t *testing.T) {
	integer := LogicalType{Root: "INTEGER"}
	stringType := LogicalType{Root: "STRING", Nullable: true}
	rowType := LogicalType{Root: "ROW", Fields: []LogicalField{{Name: "id", Type: integer}, {Name: "label", Type: stringType}}}
	mapKey := LogicalType{Root: "STRING"}
	mapValue := LogicalType{Root: "BIGINT", Nullable: true}
	types := []LogicalType{
		{Root: "BOOLEAN"}, {Root: "TINYINT"}, {Root: "SMALLINT"}, integer,
		{Root: "BIGINT"}, {Root: "FLOAT"}, {Root: "DOUBLE"}, {Root: "STRING"},
		{Root: "BINARY", Length: 8}, {Root: "BYTES"},
		{Root: "DECIMAL", Precision: 20, Scale: 3}, {Root: "DATE"},
		{Root: "TIME_WITHOUT_TIME_ZONE", Precision: 0},
		{Root: "TIME_WITHOUT_TIME_ZONE", Precision: 3},
		{Root: "TIME_WITHOUT_TIME_ZONE", Precision: 6},
		{Root: "TIME_WITHOUT_TIME_ZONE", Precision: 9},
		{Root: "TIMESTAMP_WITHOUT_TIME_ZONE", Precision: 9},
		{Root: "ARRAY", Element: &stringType},
		{Root: "MAP", Key: &mapKey, Value: &mapValue},
		rowType,
	}
	columns := make([]Column, len(types))
	for i, logicalType := range types {
		columns[i] = logicalColumn("field_"+string(rune('a'+i)), logicalType)
	}
	schema := Schema{Columns: columns}
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	got, err := SchemaFromArrow(arrowSchema)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Columns) != len(columns) ||
		got.Columns[8].LogicalType.Length != 8 ||
		got.Columns[10].LogicalType.Precision != 20 ||
		got.Columns[17].LogicalType.Element.Root != "STRING" ||
		got.Columns[18].LogicalType.Key.Nullable ||
		len(got.Columns[19].LogicalType.Fields) != 2 {
		t.Fatalf("Arrow round trip = %#v", got)
	}
}

func TestArrowPayloadCaptureLifecycle(t *testing.T) {
	capture := &arrowPayloadCapture{}
	if err := capture.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := capture.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	for _, test := range []struct {
		precision int
		want      arrow.TimeUnit
	}{
		{precision: 0, want: arrow.Second},
		{precision: 3, want: arrow.Millisecond},
		{precision: 6, want: arrow.Microsecond},
		{precision: 9, want: arrow.Nanosecond},
	} {
		if got := arrowUnit(test.precision); got != test.want {
			t.Fatalf("arrowUnit(%d) = %v, want %v", test.precision, got, test.want)
		}
	}
}

func TestArrowRecordDecoderRejectsPayloadBoundaries(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	if record, err := decodeArrowRecord(schema, nil, 0, allocator); err != nil || record != nil {
		t.Fatalf("decode empty record = %v, %v", record, err)
	}
	if _, err := decodeArrowRecord(schema, []byte{1}, 0, allocator); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("empty record trailing data error = %v", err)
	}
	if _, err := decodeArrowRecord(schema, []byte{1}, 1, allocator); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("corrupt payload error = %v", err)
	}

	record := testArrowRecord(t, allocator, 8)
	payload, err := encodeArrowPayload(record, ArrowCompressionNone, allocator)
	if err != nil {
		record.Release()
		t.Fatal(err)
	}
	if decoded, err := decodeArrowRecord(record.Schema(), payload, 1, allocator); !errors.Is(err, ErrMalformedRecordBatch) {
		if decoded != nil {
			decoded.Release()
		}
		record.Release()
		t.Fatalf("row count mismatch error = %v", err)
	}
	record.Release()
	allocator.AssertSize(t, 0)
}

func TestArrowSchemaNormalizesLossyLogicalDistinctions(t *testing.T) {
	charType := LogicalType{Root: "CHAR", Length: 4}
	localTimestamp := LogicalType{Root: "TIMESTAMP_WITH_LOCAL_TIME_ZONE", Precision: 3}
	schema := Schema{Columns: []Column{
		logicalColumn("char", charType),
		logicalColumn("local_timestamp", localTimestamp),
	}}
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	got, err := SchemaFromArrow(arrowSchema)
	if err != nil {
		t.Fatal(err)
	}
	if got.Columns[0].Type != StringType || got.Columns[1].Type != TimestampType {
		t.Fatalf("normalized schema = %#v", got)
	}
}

func TestArrowSchemaRejectsInvalidSchema(t *testing.T) {
	if _, err := (Schema{}).ArrowSchema(); err == nil {
		t.Fatal("ArrowSchema() error = nil")
	}
	if _, err := SchemaFromArrow(nil); err == nil {
		t.Fatal("SchemaFromArrow(nil) error = nil")
	}
	for _, logicalType := range []LogicalType{
		{Root: "BINARY"}, {Root: "DECIMAL", Precision: 39}, {Root: "ARRAY"},
		{Root: "MAP"}, {Root: "TIME_WITHOUT_TIME_ZONE", Precision: 10},
	} {
		if _, err := arrowType(logicalType); err == nil {
			t.Fatalf("arrowType(%#v) succeeded", logicalType)
		}
	}
	unsupported := arrow.Field{Name: "duration", Type: &arrow.DurationType{Unit: arrow.Second}}
	if _, err := SchemaFromArrow(arrow.NewSchema([]arrow.Field{unsupported}, nil)); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("unsupported Arrow type error = %v", err)
	}
}

func TestArrowLogBatchRoundTripAndOwnership(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	record := testArrowRecord(t, allocator, 64<<10)

	for _, compression := range []ArrowCompression{ArrowCompressionNone, ArrowCompressionLZ4, ArrowCompressionZSTD} {
		for _, magic := range []byte{0, 1} {
			batch := ArrowLogBatch{
				Magic: magic, BaseOffset: 10, CommitTime: 20, LeaderEpoch: 3, SchemaID: 4,
				WriterID: 5, BatchSequence: 6, Record: record,
				Changes: []ChangeType{Insert, UpdateBefore, UpdateAfter, Delete},
			}
			encoded, err := EncodeArrowLogBatch(batch, compression, allocator)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeArrowLogBatch(record.Schema(), encoded, allocator)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Magic != magic || decoded.BaseOffset != 10 || decoded.SchemaID != 4 ||
				decoded.Record.NumRows() != 4 || !array.RecordEqual(decoded.Record, record) ||
				len(decoded.Changes) != 4 || decoded.Changes[3] != Delete {
				t.Fatalf("decoded Arrow batch = %#v", decoded)
			}
			decoded.Release()
			decoded.Release()
		}
	}
	record.Release()
	allocator.AssertSize(t, 0)
}

func TestArrowNestedRecordBatchRoundTrip(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	integer := LogicalType{Root: "INTEGER"}
	stringType := LogicalType{Root: "STRING", Nullable: true}
	arrayType := LogicalType{Root: "ARRAY", Element: &integer}
	rowType := LogicalType{Root: "ROW", Fields: []LogicalField{
		{Name: "id", Type: integer}, {Name: "label", Type: stringType},
	}}
	flussSchema := Schema{Columns: []Column{
		logicalColumn("values", arrayType),
		logicalColumn("nested", rowType),
	}}
	schema, err := flussSchema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(allocator, schema)
	list := builder.Field(0).(*array.ListBuilder)
	list.Append(true)
	list.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	nested := builder.Field(1).(*array.StructBuilder)
	nested.Append(true)
	nested.FieldBuilder(0).(*array.Int32Builder).Append(7)
	nested.FieldBuilder(1).(*array.StringBuilder).Append("go")
	record := builder.NewRecordBatch()
	builder.Release()
	encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 1, Record: record, Changes: []ChangeType{Insert},
	}, ArrowCompressionLZ4, allocator)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeArrowLogBatch(schema, encoded, allocator)
	if err != nil || !array.RecordEqual(decoded.Record, record) {
		t.Fatalf("nested Arrow batch = %#v, %v", decoded, err)
	}
	decoded.Release()
	record.Release()
	allocator.AssertSize(t, 0)
}

func TestArrowLogBatchDecodesJava091Fixture(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	record := java091ArrowRecord(t, allocator)
	javaBytes, err := hex.DecodeString("000000000000000026010000000000000000000000028570bb0900000100000007000000000000000d000000020000000104ffffffffc800000014000000000000000c0016000e001500100004000c0000003000000000000000000004001000000000030a0018000c00080004000a00000014000000680000000200000000000000000000000500000000000000000000000100000000000000080000000000000008000000000000001000000000000000010000000000000018000000000000000c000000000000002800000000000000060000000000000000000000020000000200000000000000000000000000000002000000000000000000000000000000030000000000000001000000020000000300000000000000000000000300000006000000000000006f6e6574776f0000")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeArrowLogBatch(record.Schema(), javaBytes, allocator)
	if err != nil {
		t.Fatal(err)
	}
	if !array.RecordEqual(decoded.Record, record) ||
		len(decoded.Changes) != 2 || decoded.Changes[0] != Insert || decoded.Changes[1] != Delete {
		t.Fatalf("decoded Java Arrow fixture = %#v", decoded)
	}
	decoded.Release()

	goBytes, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, SchemaID: 9, WriterID: 7, BatchSequence: 13,
		Record: record, Changes: []ChangeType{Insert, Delete},
	}, ArrowCompressionNone, allocator)
	if err != nil {
		t.Fatal(err)
	}
	if len(goBytes) <= logBatchV0HeaderSize {
		t.Fatal("Go Arrow fixture has no IPC payload")
	}
	record.Release()
	allocator.AssertSize(t, 0)
}

func TestArrowAppendOnlyAndEmptyBatches(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	record := testArrowRecord(t, allocator, 8)
	schema := record.Schema()
	encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, AppendOnly: true, Record: record,
		Changes: []ChangeType{Append, Append, Append, Append},
	}, ArrowCompressionNone, allocator)
	record.Release()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeArrowLogBatch(schema, encoded, allocator)
	if err != nil || !decoded.AppendOnly || decoded.Changes[2] != Append {
		t.Fatalf("append-only batch = %#v, %v", decoded, err)
	}
	decoded.Release()

	empty, err := EncodeArrowLogBatch(ArrowLogBatch{Magic: 1, AppendOnly: true}, ArrowCompressionNone, allocator)
	if err != nil || len(empty) != logBatchV1HeaderSize {
		t.Fatalf("empty Arrow batch length = %d, %v", len(empty), err)
	}
	emptyDecoded, err := DecodeArrowLogBatch(schema, empty, allocator)
	if err != nil || emptyDecoded.Record != nil || len(emptyDecoded.Changes) != 0 {
		t.Fatalf("empty Arrow batch = %#v, %v", emptyDecoded, err)
	}
	emptyDecoded.Release()
	allocator.AssertSize(t, 0)
}

func TestArrowLogBatchRejectsInvalidInput(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	record := testArrowRecord(t, allocator, 8)
	schema := record.Schema()

	tests := []ArrowLogBatch{
		{Magic: 2, Record: record, Changes: []ChangeType{Append, Append, Append, Append}},
		{Magic: 0, Record: record, Changes: []ChangeType{Append}},
		{Magic: 0, AppendOnly: true, Record: record, Changes: []ChangeType{Insert, Append, Append, Append}},
	}
	for _, batch := range tests {
		if _, err := EncodeArrowLogBatch(batch, ArrowCompressionNone, allocator); err == nil {
			t.Fatalf("EncodeArrowLogBatch(%#v) succeeded", batch)
		}
	}
	valid, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, Record: record, Changes: []ChangeType{Append, Insert, UpdateAfter, Delete},
	}, ArrowCompressionNone, allocator)
	if err != nil {
		t.Fatal(err)
	}
	corruptCRC := append([]byte(nil), valid...)
	corruptCRC[len(corruptCRC)-1] ^= 1
	badChange := append([]byte(nil), valid...)
	badChange[logBatchV0HeaderSize] = 99
	rewriteArrowCRC(badChange, 21, 25)
	badCount := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint32(badCount[44:], 5)
	rewriteArrowCRC(badCount, 21, 25)
	for _, encoded := range [][]byte{nil, valid[:20], corruptCRC, badChange, badCount} {
		if decoded, err := DecodeArrowLogBatch(schema, encoded, allocator); err == nil {
			decoded.Release()
			t.Fatalf("DecodeArrowLogBatch(%x) succeeded", encoded)
		}
	}
	if _, err := EncodeArrowLogBatch(ArrowLogBatch{Magic: 0, Record: record, Changes: []ChangeType{Append, Insert, UpdateAfter, Delete}}, ArrowCompression(99), allocator); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid compression error = %v", err)
	}
	record.Release()
	allocator.AssertSize(t, 0)
}

func FuzzDecodeArrowLogBatch(f *testing.F) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	f.Add(make([]byte, logBatchV0HeaderSize))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > maxRowBytes+1 {
			t.Skip()
		}
		batch, _ := DecodeArrowLogBatch(schema, encoded, memory.DefaultAllocator)
		if batch != nil {
			batch.Release()
		}
	})
}

func BenchmarkArrowLogBatch(b *testing.B) {
	allocator := memory.NewGoAllocator()
	record := testArrowRecord(b, allocator, 1024)
	defer record.Release()
	batch := ArrowLogBatch{Magic: 0, AppendOnly: true, Record: record}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := EncodeArrowLogBatch(batch, ArrowCompressionZSTD, allocator); err != nil {
			b.Fatal(err)
		}
	}
}

func testArrowRecord(t testing.TB, allocator memory.Allocator, stringCapacity int) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4}, nil)
	values := []string{
		"a", string(bytes.Repeat([]byte("b"), stringCapacity)),
		"", string(bytes.Repeat([]byte("d"), stringCapacity)),
	}
	builder.Field(1).(*array.StringBuilder).AppendValues(values, []bool{true, true, false, true})
	return builder.NewRecordBatch()
}

func java091ArrowRecord(t testing.TB, allocator memory.Allocator) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"one", "two"}, nil)
	return builder.NewRecordBatch()
}

func rewriteArrowCRC(encoded []byte, crcOffset, schemaOffset int) {
	binary.LittleEndian.PutUint32(encoded[crcOffset:], crc32.Checksum(encoded[schemaOffset:], crc32.MakeTable(crc32.Castagnoli)))
}
