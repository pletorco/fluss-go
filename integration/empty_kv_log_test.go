//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testEmptyKVLogBatch(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "empty_kv_log"}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema:      fgo.Schema{Columns: []fgo.Column{{Name: "id", Type: fgo.IntType}}, PrimaryKey: []string{"id"}},
		BucketCount: 1, Properties: map[string]string{"table.log.format": "COMPACTED"},
	}, false); err != nil {
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
	if result := w.Delete(ctx, fgo.PrimaryKey{int32(999)}).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	end := admin.ListOffsets(ctx, table, fgo.PhysicalTablePath{TablePath: path}, -1, []int32{0}, fgo.Latest())[0]
	if end.Err != nil || end.Offset != 1 {
		t.Fatalf("missing-key delete end offset = %+v, want 1", end)
	}
	before := pollEmptyKVLog(t, ctx, client, table, end.Offset)
	if len(before) != 0 {
		t.Fatalf("empty batch emitted records: %v", before)
	}
	if result := w.Upsert(ctx, fgo.Row{int32(42)}).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	after := pollEmptyKVLog(t, ctx, client, table, end.Offset+1)
	if len(after) != 1 || after[0].Record.Offset != 1 || after[0].Record.Value[0] != int32(42) {
		t.Fatalf("scan after empty batch = %v, want key 42 at offset 1", after)
	}
}

func pollEmptyKVLog(t *testing.T, ctx context.Context, client *fgo.Client, table fgo.Table, stop int64) []fgo.ScanRecord {
	t.Helper()
	s, err := client.NewLogScanner(ctx, table, fgo.Earliest(), fgo.WithScanStoppingOffsets(map[int32]int64{0: stop}),
		fgo.WithLogFetchLimits(1<<20, 1<<20, 0, 100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var rows []fgo.ScanRecord
	for range 4 {
		result, err := s.Poll(ctx)
		result.Release()
		if err != nil || len(result.BucketErrors) != 0 {
			t.Fatalf("scan bounded at %d: %v / %v", stop, result.BucketErrors, err)
		}
		rows = append(rows, result.Records...)
		if result.Done {
			return rows
		}
	}
	t.Fatalf("scan did not reach exclusive offset %d", stop)
	return nil
}
