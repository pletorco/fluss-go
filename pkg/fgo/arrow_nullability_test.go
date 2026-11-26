package fgo

import (
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestAppendArrowChecksRequiredData(t *testing.T) {
	integer := LogicalType{Root: "INTEGER"}
	for _, tc := range []struct {
		name, json string
		typ        LogicalType
	}{
		{"top level", `[{"value":null},{"value":7}]`, integer},
		{"nullable struct", `[{"value":{"n":null}},{"value":null},{"value":{"n":7}}]`, LogicalType{
			Root: "ROW", Nullable: true, Fields: []LogicalField{{Name: "n", Type: integer}},
		}},
		{"nullable list", `[{"value":[null]},{"value":null},{"value":[7]}]`, LogicalType{
			Root: "ARRAY", Nullable: true, Element: &integer,
		}},
		{"nullable map", `[{"value":[{"key":"k","value":{"n":null}}]},{"value":null},{"value":[{"key":"k","value":{"n":7}}]}]`, LogicalType{
			Root: "MAP", Nullable: true, Key: &LogicalType{Root: "STRING"},
			Value: &LogicalType{Root: "ROW", Fields: []LogicalField{{Name: "n", Type: integer}}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			table := appendWriterTable()
			table.Schema.Columns = []Column{logicalColumn("value", tc.typ)}
			backend := logBackend(0)
			w, err := newAppendWriter(ctx, backend, table, WithAppendLogFormat(LogFormatArrow), WithAppendBatchTimeout(0))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close(ctx)
			schema, err := table.Schema.ArrowSchema()
			if err != nil {
				t.Fatal(err)
			}
			record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(tc.json))
			if err != nil {
				t.Fatal(err)
			}
			defer record.Release()
			changes := make([]ChangeType, record.NumRows())
			for i := range changes {
				changes[i] = Append
			}
			if got := w.AppendArrow(ctx, 0, record, changes).Await(ctx); !errors.Is(got.Err, ErrInvalidRow) {
				t.Fatalf("invalid batch = %+v, want ErrInvalidRow", got)
			}
			valid := record.NewSlice(1, record.NumRows())
			defer valid.Release()
			if got := w.AppendArrow(ctx, 0, valid, changes[1:]).Await(ctx); got.Err != nil {
				t.Fatalf("valid slice = %+v", got)
			}
			if got := len(backend.produced()); got != 1 {
				t.Fatalf("produce calls = %d, want only the valid slice", got)
			}
		})
	}
}
