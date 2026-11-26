package fgo

import (
	"context"
	"reflect"
	"testing"
)

func TestUpsertWriterOwnsCallerTableSchema(t *testing.T) {
	table := upsertWriterTable()
	table.Schema.Columns[2] = logicalColumn("scores", LogicalType{Root: "ARRAY", Element: &LogicalType{Root: "INTEGER"}})
	backend := kvBackend(0)
	ctx := t.Context()
	w, err := newUpsertWriter(ctx, backend, table, WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	table.Schema.Columns[1].Type = IntType
	table.Schema.Columns[2].LogicalType.Element.Root = "STRING"
	if result := w.Upsert(ctx, Row{int32(1), "original", []any{int32(2)}}).Await(ctx); result.Err != nil {
		t.Fatalf("original row after caller metadata edit: %v", result.Err)
	}
}

func TestLookuperOwnsCallerTableKeyOrder(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	key := PrimaryKey{"tenant-a", int32(8)}
	want := Row{"tenant-a", int32(8), "original"}
	putLookupValue(t, backend, table, key, want)
	l, err := newLookuper(t.Context(), backend, table)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	table.Schema.PrimaryKey[0], table.Schema.PrimaryKey[1] = table.Schema.PrimaryKey[1], table.Schema.PrimaryKey[0]
	if result := l.Lookup(t.Context(), key)[0]; result.Err != nil || !result.Found || !reflect.DeepEqual(result.Row, want) {
		t.Fatalf("lookup after caller key-order change = %+v", result)
	}
}

func TestLogScannerOwnsProjectedSchemaAndAccessorCopy(t *testing.T) {
	table := appendWriterTable()
	table.Schema.Columns[1] = logicalColumn("name", LogicalType{Root: "ARRAY", Element: &LogicalType{Root: "STRING"}})
	s, err := newLogScanner(t.Context(), scannerBackend(0), table, AtOffset(0), WithScanProjection("name", "id"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	accessor := s.Schema()
	if accessor.Columns[0].Name != "name" || accessor.Columns[1].Name != "id" {
		t.Fatalf("result shape does not follow projection order: %+v", accessor)
	}
	table.Schema.Columns[1].LogicalType.Element.Root = "INTEGER"
	accessor.Columns[0].Name = "accessor edit"
	accessor.Columns[0].LogicalType.Root = "BYTES"
	got := s.Schema().Columns[0]
	if got.Name != "name" || got.LogicalType.Root != "ARRAY" || got.LogicalType.Element.Root != "STRING" {
		t.Fatalf("projected schema after caller mutations = %+v / %+v", got, got.LogicalType)
	}
}

func TestBatchScannerOwnsCallerMetadata(t *testing.T) {
	table := appendWriterTable()
	table.Properties = map[string]string{"table.log.format": "COMPACTED"}
	encoded := encodedRows(t, table.Schema, 0, 7)
	backend := batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
		return true, encoded, nil
	})
	s, err := newBatchScanner(t.Context(), backend, nil, table, testTableBucket(table), WithBatchProjection("name"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	table.Properties["table.log.format"] = "INDEXED"
	table.Schema.Columns[1].Type = IntType
	got, err := s.Poll(t.Context())
	defer got.Release()
	if err != nil || !got.Done || !reflect.DeepEqual(got.Rows, []Row{{"row"}}) {
		t.Fatalf("batch after caller metadata mutation = %+v, %v", got, err)
	}
}

func TestSnapshotRequestAndScannerOwnIndependentTableCopies(t *testing.T) {
	table := upsertWriterTable()
	table.Properties = map[string]string{"test": "original"}
	var providerRequest SnapshotBatchRequest
	client := &Client{snapshotProvider: SnapshotBatchProviderFunc(func(_ context.Context, req SnapshotBatchRequest) (SnapshotBatchReader, error) {
		providerRequest = req
		return &fakeSnapshotBatchReader{batches: [][]Row{{{int32(1), "one", nil}}}, eofWithLast: true}, nil
	})}
	s, err := client.NewSnapshotBatchScanner(t.Context(), table, testTableBucket(table), 1, WithBatchProjection("name"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	table.Schema.Columns[0].Name = "caller edit"
	table.Properties["test"] = "caller edit"
	if providerRequest.Table.Schema.Columns[0].Name != "id" || providerRequest.Table.Properties["test"] != "original" {
		t.Fatal("snapshot provider retained caller-owned metadata")
	}
	providerRequest.Table.Schema.Columns[0].Name = "provider edit"
	providerRequest.Table.Properties["test"] = "provider edit"
	if s.table.Schema.Columns[0].Name != "id" || s.table.Properties["test"] != "original" {
		t.Fatal("snapshot provider can still mutate scanner metadata")
	}
	got, err := s.Poll(t.Context())
	if err != nil || !got.Done || !reflect.DeepEqual(got.Rows, []Row{{"one"}}) {
		t.Fatalf("snapshot projection after edits = %+v, %v", got, err)
	}
}
