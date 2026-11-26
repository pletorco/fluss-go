//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testTimestampPrecision(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "timestamp_precision"}
	schema := fgo.Schema{Columns: []fgo.Column{
		{Name: "ts", Type: fgo.TimestampType, LogicalType: &fgo.LogicalType{Root: "TIMESTAMP_WITHOUT_TIME_ZONE", Precision: 3}},
		{Name: "ts_ltz", Type: fgo.TimestampLTZType, LogicalType: &fgo.LogicalType{Root: "TIMESTAMP_WITH_LOCAL_TIME_ZONE", Precision: 3}},
		{Name: "name", Type: fgo.StringType},
	}, PrimaryKey: []string{"ts"}}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{Schema: schema, BucketCount: 1}, false); err != nil {
		t.Fatal(err)
	}
	waitForTableReady(t, admin, path)
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := client.NewUpsertWriter(ctx, table, fgo.WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	first := time.Date(2026, time.August, 1, 14, 0, 0, 123000000, time.UTC)
	second := first.Add(time.Millisecond)
	good := []fgo.Row{{first, first, "first"}, {second, second, "second"}}
	for _, row := range good {
		if got := w.Upsert(ctx, row).Await(ctx); got.Err != nil {
			t.Fatal(got.Err)
		}
	}
	bad := first.Add(100 * time.Microsecond)
	for _, row := range []fgo.Row{{bad, first, "should not overwrite first"}, {second, bad, "should not overwrite second"}} {
		if got := w.Upsert(ctx, row).Await(ctx); !errors.Is(got.Err, fgo.ErrInvalidRow) {
			t.Fatalf("sub-millisecond update = %+v, want ErrInvalidRow", got)
		}
	}
	l, err := client.NewLookuper(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	results := l.Lookup(ctx, fgo.PrimaryKey{first}, fgo.PrimaryKey{second}, fgo.PrimaryKey{bad})
	for i, row := range good {
		got := results[i]
		if got.Err != nil || !got.Found || !got.Row[1].(time.Time).Equal(row[1].(time.Time)) || got.Row[2] != row[2] {
			t.Fatalf("lookup %d = %+v, want %v", i, got, row)
		}
	}
	if !errors.Is(results[2].Err, fgo.ErrInvalidRow) {
		t.Fatalf("sub-millisecond lookup = %+v, want ErrInvalidRow", results[2])
	}
}
