//go:build integration

package integration_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testIndexedKVValues(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "indexed_kv_values"}
	schema := fgo.Schema{Columns: []fgo.Column{
		{Name: "id", Type: fgo.IntType}, {Name: "sub", Type: fgo.IntType},
		// Fluss 1.0 requires nullable non-key columns for partial updates.
		{Name: "name", Type: fgo.StringType, Nullable: true}, {Name: "note", Type: fgo.StringType, Nullable: true},
	}, PrimaryKey: []string{"id", "sub"}, BucketKey: []string{"id"}}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema: schema, BucketCount: 1,
		Properties: map[string]string{"table.kv.format": "INDEXED", "table.log.format": "COMPACTED"},
	}, false); err != nil {
		t.Fatal(err)
	}
	wire := readyWireKV(t, ctx, client, admin, fgo.PhysicalTablePath{TablePath: path})
	want := map[int32]fgo.Row{
		1: {int32(7), int32(1), "raw indexed row", nil},
		2: {int32(7), int32(2), "updated", "keep"},
	}
	// Bypass the Go writer to exercise the readers against a known indexed
	// value and an independently specified compacted physical key.
	value, err := fgo.EncodeIndexedRow(wire.table.Schema, want[1])
	if err != nil {
		t.Fatal(err)
	}
	wire.putEncoded(t, ctx, []byte{7, 1}, value)
	w, err := client.NewUpsertWriter(ctx, wire.table, fgo.WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	if got := w.Upsert(ctx, fgo.Row{int32(7), int32(2), "original", "keep"}).Await(ctx); got.Err != nil {
		t.Fatalf("indexed upsert: %v", got.Err)
	}
	if got := w.PartialUpsert(ctx, []string{"sub", "name", "id"}, fgo.Row{int32(2), "updated", int32(7)}).Await(ctx); got.Err != nil {
		t.Fatalf("indexed partial upsert: %v", got.Err)
	}
	wire.wantFound(t, ctx, []byte{7, 2}, true)
	assertIndexedKVReads(t, ctx, wire, want)
}

func assertIndexedKVReads(t *testing.T, ctx context.Context, wire wireKV, want map[int32]fgo.Row) {
	t.Helper()
	l, err := wire.client.NewLookuper(ctx, wire.table)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for sub, row := range want {
		got := l.Lookup(ctx, fgo.PrimaryKey{int32(7), sub})[0]
		if !got.Found || got.Err != nil || !reflect.DeepEqual(got.Row, row) {
			t.Fatalf("indexed point lookup %d = %+v, want %v", sub, got, row)
		}
	}
	prefix := l.PrefixLookup(ctx, fgo.PrimaryKey{int32(7)})[0]
	if prefix.Err != nil {
		t.Fatal(prefix.Err)
	}
	assertIndexedKVRows(t, prefix.Rows, want)
	s, err := wire.client.NewBatchScanner(ctx, wire.table, wire.bucket)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var rows []fgo.Row
	for !s.Done() {
		result, err := s.Poll(ctx)
		result.Release()
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, result.Rows...)
	}
	assertIndexedKVRows(t, rows, want)
}

func assertIndexedKVRows(t *testing.T, rows []fgo.Row, want map[int32]fgo.Row) {
	t.Helper()
	got := make(map[int32]fgo.Row, len(rows))
	for _, row := range rows {
		got[row[1].(int32)] = row
	}
	if len(rows) != len(want) || !reflect.DeepEqual(got, want) {
		t.Fatalf("indexed KV rows = %v, want %v", rows, want)
	}
}
