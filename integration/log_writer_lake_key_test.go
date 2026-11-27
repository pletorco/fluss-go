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

func testLakeBoundLogRouting(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	for _, tc := range []struct {
		name, format        string
		lake, keyed, reject bool
	}{
		{name: "lake_keyed", format: "COMPACTED", lake: true, keyed: true, reject: true},
		{name: "plain_keyed", format: "COMPACTED", keyed: true},
		{name: "lake_unkeyed", format: "COMPACTED", lake: true},
		{name: "lake_explicit_arrow", format: "ARROW", lake: true, keyed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			path := fgo.TablePath{Database: database, Table: tc.name}
			schema := fgo.Schema{Columns: []fgo.Column{
				{Name: "id", Type: fgo.IntType}, {Name: "message", Type: fgo.StringType},
			}}
			if tc.keyed {
				schema.BucketKey = []string{"id"}
			}
			props := map[string]string{"table.log.format": tc.format}
			if tc.lake {
				props["table.datalake.format"], props["table.datalake.enabled"] = "paimon", "false"
			}
			if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
				Schema: schema, BucketCount: 8, Properties: props,
			}, false); err != nil {
				t.Fatal(err)
			}
			waitForTableReady(t, admin, path)
			table, err := client.GetTable(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.lake && (table.Properties["table.datalake.format"] != "paimon" || table.Properties["table.datalake.enabled"] != "false") {
				t.Fatalf("unexpected pre-bound table metadata: %v", table.Properties)
			}
			w, err := client.NewAppendWriter(ctx, table, fgo.WithAppendBatchTimeout(0))
			if w != nil {
				defer w.Close(ctx)
			}
			if tc.reject {
				if w != nil || !errors.Is(err, fgo.ErrUnsupportedAPI) {
					t.Fatalf("open writer: got %v, want ErrUnsupportedAPI", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.format == "ARROW" {
				// The caller can still supply a lake-compatible explicit bucket.
				return
			}
			got := w.Append(ctx, fgo.Row{int32(1), "first"}).Await(ctx)
			if got.Err != nil || !got.OffsetKnown || got.BaseOffset != 0 {
				t.Fatalf("first append: %+v", got)
			}
			if tc.keyed && got.Bucket != 4 {
				t.Fatalf("ordinary key 1 went to bucket %d, want Java's bucket 4", got.Bucket)
			}
		})
	}
}
