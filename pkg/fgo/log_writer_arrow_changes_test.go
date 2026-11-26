package fgo

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestAppendArrowRequiresAppendOnlyChanges(t *testing.T) {
	ctx := t.Context()
	table := appendWriterTable()
	table.Properties["table.log.format"] = "ARROW"
	backend := logBackend(0)
	writer, err := newAppendWriter(ctx, backend, table, WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	schema, err := table.Schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"one", "two"}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	for _, change := range []ChangeType{Insert, UpdateBefore, UpdateAfter, Delete, ChangeType(-1), ChangeType(99)} {
		t.Run(fmt.Sprint(change), func(t *testing.T) {
			if got := writer.AppendArrow(ctx, 0, record, []ChangeType{Append, change}).Await(ctx); !errors.Is(got.Err, ErrInvalidRow) {
				t.Fatalf("append mixed batch with change %d: got %+v, want ErrInvalidRow", change, got)
			}
		})
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(backend.produced()) != 0 {
		t.Fatal("invalid Arrow changes reached the backend")
	}

	changes := []ChangeType{Append, Append}
	if got := writer.AppendArrow(ctx, 0, record, changes).Await(ctx); got.Err != nil || got.Records != 2 {
		t.Fatalf("valid append = %+v", got)
	}
	calls := backend.produced()
	if len(calls) != 1 {
		t.Fatalf("produced %d batches; want one", len(calls))
	}
	batch, err := DecodeArrowLogBatch(schema, calls[0].records, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	if !batch.AppendOnly || !slices.Equal(batch.Changes, changes) || !array.RecordEqual(batch.Record, record) {
		t.Fatalf("unexpected append-only batch: appendOnly=%t changes=%v", batch.AppendOnly, batch.Changes)
	}
}
