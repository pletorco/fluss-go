//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testKVScanRowLimit(t *testing.T, c *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "kv_scan_row_limit"}
	schema := fgo.Schema{Columns: []fgo.Column{
		{Name: "id", Type: fgo.IntType}, {Name: "name", Type: fgo.StringType},
	}, PrimaryKey: []string{"id"}}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{Schema: schema, BucketCount: 1}, false); err != nil {
		t.Fatal(err)
	}
	bucket := readyWireKV(t, ctx, c, admin, fgo.PhysicalTablePath{TablePath: path})
	const total = 1025
	w, err := c.NewUpsertWriter(ctx, bucket.table)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	writes := make([]*fgo.WriteFuture, total)
	for i := range writes {
		writes[i] = w.Upsert(ctx, fgo.Row{int32(i), "value"})
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for i, write := range writes {
		if got := write.Await(ctx); got.Err != nil {
			t.Fatalf("write %d: %+v", i, got)
		}
	}
	for _, tc := range []struct {
		name    string
		limit   int
		options []fgo.BatchScannerOption
	}{
		{"defaults", 1024, nil},
		{"local subdivision", 13, []fgo.BatchScannerOption{fgo.WithBatchLimit(13)}},
		{"server byte pages", 1024, []fgo.BatchScannerOption{fgo.WithBatchSizeBytes(1024)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scanner, err := c.NewBatchScanner(ctx, bucket.table, bucket.bucket, tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			seen := make(map[int32]bool, total)
			polls := 0
			for {
				result, err := scanner.Poll(ctx)
				if err != nil {
					t.Fatal(err)
				}
				polls++
				if len(result.Rows) > tc.limit {
					t.Fatalf("poll %d emitted %d rows; limit %d", polls, len(result.Rows), tc.limit)
				}
				for _, row := range result.Rows {
					id := row[0].(int32)
					if id < 0 || id >= total || seen[id] {
						t.Fatalf("unexpected or duplicate row %v", row)
					}
					seen[id] = true
				}
				result.Release()
				if scanner.Done() != result.Done {
					t.Fatalf("Poll.Done=%v, scanner.Done()=%v", result.Done, scanner.Done())
				}
				if result.Done {
					if len(seen) != total || polls < 2 {
						t.Fatalf("scan ended after %d/%d unique rows in %d polls", len(seen), total, polls)
					}
					break
				}
			}
		})
	}
}
