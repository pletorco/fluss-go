package fgo

import (
	"errors"
	"reflect"
	"testing"
)

func TestPartialUpsertAutoIncrementSelectionAndWirePlaceholder(t *testing.T) {
	for _, required := range []bool{false, true} {
		name := "nullable generated column"
		if required {
			name = "required generated column"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			table := upsertWriterTable()
			table.Schema.Columns[2].Nullable = !required
			table.Schema.AutoIncrement = []string{"score"}
			backend := kvBackend(0)
			w, err := newUpsertWriter(ctx, backend, table, WithUpsertBatchTimeout(0))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close(ctx)
			rejected := w.PartialUpsert(ctx, []string{"id", "score"}, Row{int32(1), int64(3)}).Await(ctx)
			if !errors.Is(rejected.Err, ErrInvalidRow) || len(backend.putCalls()) != 0 {
				t.Fatalf("explicit auto-increment write was not rejected locally: %+v", rejected)
			}
			got := w.PartialUpsert(ctx, []string{"name", "id"}, Row{"new", int32(1)}).Await(ctx)
			calls := backend.putCalls()
			if required {
				if !errors.Is(got.Err, ErrUnsupportedAPI) || len(calls) != 0 {
					t.Fatalf("required auto-increment write = %+v, calls=%d", got, len(calls))
				}
				return
			}
			if got.Err != nil || len(calls) != 1 || !reflect.DeepEqual(calls[0].targets, []int32{1, 0}) {
				t.Fatalf("nullable auto-increment write = %+v, calls=%v", got, calls)
			}
			batch, err := DecodeKVBatch(calls[0].records)
			if err != nil || len(batch.Records) != 1 {
				t.Fatalf("batch = %+v, %v", batch, err)
			}
			row, err := DecodeCompactedRow(table.Schema, batch.Records[0].Value)
			if err != nil || !reflect.DeepEqual(row, Row{int32(1), "new", nil}) {
				t.Fatalf("full-width partial row = %v, %v", row, err)
			}
		})
	}
}
