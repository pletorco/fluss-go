package fgo

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestArrowEvolutionRequiresNonNullProjectedValues(t *testing.T) {
	source := evolutionSchema("old_name", true, StringType)
	target := Schema{
		Columns: []Column{
			{Name: "extra", Type: BigIntType, Nullable: true, ID: 3},
			{Name: "id", Type: IntType, ID: 1},
			{Name: "name", Type: StringType, ID: 2},
		},
		HighestFieldID: 3,
	}
	table := Table{ID: 9, SchemaID: 2, Path: appendWriterTable().Path, Kind: LogTable, BucketCount: 1, Schema: target}
	current := arrowEvolutionFixture(t, target, 2, 0, false)
	for _, null := range []bool{true, false} {
		t.Run(map[bool]string{true: "invalid null", false: "non-null historical value"}[null], func(t *testing.T) {
			historical := arrowEvolutionFixture(t, source, 1, 1, null)
			backend := scannerBackend(0)
			backend.fetches[0] = scannerFetch{records: append(append([]byte(nil), current...), historical...), highWatermark: 2}
			scanner, err := newLogScanner(t.Context(), resolvingLogScannerBackend{
				fakeLogScannerBackend: backend,
				resolver:              &mapSchemaResolver{schemas: map[int32]Schema{1: source, 2: target}},
			}, table, AtOffset(0), WithScanProjection("name", "extra"))
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			result, err := scanner.Poll(t.Context())
			defer result.Release()
			if null {
				if err != nil || len(result.ArrowBatches) != 0 || len(result.BucketErrors) != 1 ||
					!errors.Is(result.BucketErrors[0].Err, ErrInvalidSchema) {
					t.Fatalf("mixed Arrow fetch with null = %+v, %v", result, err)
				}
				return
			}
			if err != nil || len(result.BucketErrors) != 0 || len(result.ArrowBatches) != 2 {
				t.Fatalf("valid mixed Arrow fetch = %+v, %v", result, err)
			}
			record := result.ArrowBatches[1].Batch.Record
			if record.Schema().Field(0).Nullable || record.Column(0).NullN() != 0 ||
				record.Schema().Field(1).Name != "extra" || record.Column(1).NullN() != 1 {
				t.Fatalf("projected Arrow schema or values = %v", record)
			}
		})
	}
}

func arrowEvolutionFixture(t *testing.T, schema Schema, version int16, offset int64, null bool) []byte {
	t.Helper()
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	builder := array.NewRecordBuilder(allocator, arrowSchema)
	defer builder.Release()
	for _, field := range builder.Fields() {
		switch b := field.(type) {
		case *array.Int32Builder:
			b.Append(42)
		case *array.Int64Builder:
			b.AppendNull()
		case *array.StringBuilder:
			if null {
				b.AppendNull()
			} else {
				b.Append("value")
			}
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	out, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, BaseOffset: offset, SchemaID: version, Record: record, Changes: []ChangeType{Append},
	}, ArrowCompressionNone, allocator)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
