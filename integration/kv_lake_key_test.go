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

func testLakeBoundKVKeyGuard(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "lake_bound_kv_keys"}
	schema := fgo.Schema{Columns: []fgo.Column{
		{Name: "id", Type: fgo.IntType}, {Name: "name", Type: fgo.StringType},
	}, PrimaryKey: []string{"id"}}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema: schema, BucketCount: 1,
		Properties: map[string]string{
			"table.datalake.format": "paimon", "table.datalake.enabled": "false", "table.log.format": "COMPACTED",
		},
	}, false); err != nil {
		t.Fatal(err)
	}
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if table.Properties["table.kv.format-version"] != "2" || table.Properties["table.datalake.format"] != "paimon" {
		t.Fatalf("unexpected lake-bound KV metadata: %v", table.Properties)
	}
	if w, err := client.NewUpsertWriter(ctx, table); !errors.Is(err, fgo.ErrUnsupportedAPI) {
		if w != nil {
			_ = w.Close(ctx)
		}
		t.Fatalf("NewUpsertWriter: got %v, want ErrUnsupportedAPI", err)
	}
	if l, err := client.NewLookuper(ctx, table); !errors.Is(err, fgo.ErrUnsupportedAPI) {
		if l != nil {
			_ = l.Close()
		}
		t.Fatalf("NewLookuper: got %v, want ErrUnsupportedAPI", err)
	}
}
