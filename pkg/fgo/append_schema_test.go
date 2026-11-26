package fgo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"
)

func TestAppendWriterOwnsSchemaMetadata(t *testing.T) {
	for _, format := range []LogFormat{LogFormatCompacted, LogFormatIndexed} {
		t.Run(string(format), func(t *testing.T) {
			schema, row, _ := mutableAppendInput()
			schema.PrimaryKey, schema.BucketKey = []string{"id"}, []string{"id"}
			schema.PartitionKey, schema.AutoIncrement = []string{"id"}, []string{"id"}
			table := appendWriterTable()
			table.Schema = schema
			table.Properties = map[string]string{"table.log.format": string(format)}
			wantSchema, err := json.Marshal(schema)
			if err != nil {
				t.Fatal(err)
			}
			want, err := (LogBatch{BaseOffset: -1, SchemaID: int16(table.SchemaID), AppendOnly: true,
				WriterID: 42, Records: []Record{{Value: row, Change: Append}}}).EncodeRows(schema, format != LogFormatIndexed)
			if err != nil {
				t.Fatal(err)
			}
			backend := logBackend(0)
			backend.partitionID = 88
			writer, err := newAppendWriter(context.Background(), backend, table,
				WithAppendLogFormat(format), WithAppendBatchTimeout(time.Hour), WithAppendPartition("1"))
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close(context.Background())
			// Each reference-bearing metadata field must be detached. Mutate
			// caller metadata concurrently with admission to also check ownership
			// under the race detector, without changing the input row values.
			var changed sync.WaitGroup
			changed.Add(1)
			go func() {
				defer changed.Done()
				for range 100 {
					mutateOwnedSchema(schema)
					table.Properties["table.log.format"] = "ARROW"
				}
			}()
			future := writer.Append(context.Background(), row)
			changed.Wait()
			if err := writer.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if result := future.Await(context.Background()); result.Err != nil {
				t.Fatal(result.Err)
			}
			gotSchema, err := json.Marshal(writer.table.Schema)
			if err != nil || !bytes.Equal(wantSchema, gotSchema) || writer.table.Properties["table.log.format"] != string(format) {
				t.Fatalf("writer metadata changed: schema=%s err=%v", gotSchema, err)
			}
			calls := backend.produced()
			if len(calls) != 1 || !bytes.Equal(calls[0].records, want) {
				t.Fatal("caller metadata mutation changed accepted bytes")
			}
		})
	}
}

func TestPreparedAppendPreservesRowValidation(t *testing.T) {
	schema, row, _ := mutableAppendInput()
	schema.BucketKey = []string{"id"}
	for _, format := range []LogFormat{LogFormatCompacted, LogFormatIndexed} {
		for _, test := range []struct {
			name string
			row  Row
		}{
			{"short", row[:1]},
			{"long", append(append(Row(nil), row...), int32(1))},
			{"null", replaceAppendValue(row, 0, nil)},
			{"wrong scalar", replaceAppendValue(row, 0, "wrong")},
			{"wrong nested", replaceAppendValue(row, 3, []any{Row{int32(1)}})},
			{"wrong map", replaceAppendValue(row, 4, Map{{Key: "wrong", Value: []any{[]byte("value")}}})},
			{"wrong decimal", replaceAppendValue(row, 2, "wrong")},
			{"decimal scale", replaceAppendValue(row, 2, big.NewRat(1, 1000))},
			{"decimal precision", replaceAppendValue(row, 2, new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), 100)))},
		} {
			t.Run(string(format)+"/"+test.name, func(t *testing.T) {
				encoding := compactedEncoding
				if format == LogFormatIndexed {
					encoding = indexedEncoding
				}
				_, want := encodeRow(schema, test.row, encoding)
				if want == nil {
					t.Fatal("fixture unexpectedly passed standalone validation")
				}
				table := appendWriterTable()
				table.Schema = schema
				backend := logBackend(0)
				writer, err := newAppendWriter(context.Background(), backend, table, WithAppendLogFormat(format))
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close(context.Background())
				got := writer.Append(context.Background(), test.row).Await(context.Background()).Err
				if got == nil || got.Error() != want.Error() || errors.Is(got, ErrInvalidRow) != errors.Is(want, ErrInvalidRow) {
					t.Fatalf("Append validation = %v, standalone = %v", got, want)
				}
				if err := writer.Flush(context.Background()); err != nil || len(backend.produced()) != 0 {
					t.Fatalf("invalid row reached backend or poisoned barrier: %v", err)
				}
			})
		}
	}
}

func replaceAppendValue(row Row, index int, value any) Row {
	cloned := append(Row(nil), row...)
	cloned[index] = value
	return cloned
}

func TestStandaloneCodecsStillValidateMutatedSchema(t *testing.T) {
	table := appendWriterTable()
	writer, err := newAppendWriter(context.Background(), logBackend(0), table)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.Background())
	table.Schema.Columns[0].Type = DataType("invalid")
	for _, encode := range []func(Schema, Row) ([]byte, error){EncodeCompactedRow, EncodeIndexedRow} {
		if _, err := encode(table.Schema, Row{int32(1), "value"}); !errors.Is(err, ErrInvalidSchema) {
			t.Fatalf("standalone codec accepted mutated schema: %v", err)
		}
	}
}
