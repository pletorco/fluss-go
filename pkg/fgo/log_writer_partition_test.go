package fgo

import (
	"errors"
	"testing"
)

func TestAppendWriterChecksCompositePhysicalPartition(t *testing.T) {
	ctx := t.Context()
	table := appendWriterTable()
	table.Schema.Columns = append(table.Schema.Columns, Column{Name: "day", Type: SmallIntType})
	table.Schema.PartitionKey = []string{"day", "name"} // Not the row's column order.
	backend := logBackend(0)
	backend.partitionID = 88
	w, err := newAppendWriter(ctx, backend, table, WithAppendPartition("2$east"), WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	for _, row := range []Row{
		{int32(1), "west", int16(2)},
		{int32(1), "east", int16(3)},
		{int32(1), nil, int16(2)},
	} {
		if result := w.Append(ctx, row).Await(ctx); !errors.Is(result.Err, ErrInvalidRow) {
			t.Fatalf("Append(%v) = %+v, want ErrInvalidRow", row, result)
		}
	}
	if result := w.Append(ctx, Row{int32(1), "east", int16(2)}).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	if calls := backend.produced(); len(calls) != 1 || calls[0].path.Partition != "2$east" {
		t.Fatalf("produce calls = %v, want one append to 2$east", calls)
	}
}
