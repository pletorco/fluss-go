//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testSchemaAfterTableRecreation(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "schema_recreated"}
	create := func(column string) {
		t.Helper()
		err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
			Schema:      fgo.Schema{Columns: []fgo.Column{{Name: column, Type: fgo.IntType}}},
			BucketCount: 1,
		}, false)
		if err != nil {
			t.Fatal(err)
		}
	}
	create("old_column")
	old, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.DropTable(ctx, path, false); err != nil {
		t.Fatal(err)
	}
	create("new_column")
	current, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if old.ID == current.ID || old.SchemaID != current.SchemaID {
		t.Fatalf("expected a new table ID with a reused schema ID, got table %d/%d and schema %d/%d",
			old.ID, current.ID, old.SchemaID, current.SchemaID)
	}
	if got := current.Schema.Columns[0].Name; got != "new_column" {
		t.Fatalf("recreated table column = %q, want new_column", got)
	}
}

func testSchemaMetadataOwnership(t *testing.T, ctx context.Context, client *fgo.Client, path fgo.TablePath) {
	t.Helper()
	mutable, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(mutable.Schema)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	mutable.Schema.Columns[0].Name = "caller_changed"
	mutable.Schema.Columns[0].LogicalType.Root = "STRING"
	for _, source := range []string{"previous result", "cache hit"} {
		table := reader
		if source == "cache hit" {
			table, err = client.GetTable(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
		}
		got, err := json.Marshal(table.Schema)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s schema changed after caller mutation: got=%s want=%s err=%v", source, got, want, err)
		}
	}
	rows := scanSchemaEvolutionRows(t, ctx, client, reader)
	if len(rows) != 2 || rows[1][1] != "old" || rows[1][2] != nil || rows[2][1] != "new" || rows[2][2] != int64(9) {
		t.Fatalf("caller metadata mutation changed historical/current decoding: %#v", rows)
	}
}

func testActiveLogScannerMetadataOwnership(t *testing.T, ctx context.Context, client *fgo.Client, path fgo.TablePath) {
	t.Helper()
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := client.NewLogScanner(ctx, table, fgo.Earliest(),
		fgo.WithScanProjection("message", "id"), fgo.WithScanRowLimit(2))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	table.Schema.Columns[1].LogicalType.Root = "BIGINT"
	s.Schema().Columns[0].LogicalType.Root = "BYTES"
	if got := s.Schema().Columns[0]; got.LogicalType.Root != "STRING" {
		t.Fatalf("scanner schema changed to %v", got.LogicalType)
	}
	rows := make(map[int32]string)
	for !s.Done() {
		result, err := s.Poll(ctx)
		result.Release()
		if err != nil || len(result.BucketErrors) != 0 {
			t.Fatalf("poll after metadata edits = %v, %v", result.BucketErrors, err)
		}
		for _, item := range result.Records {
			rows[item.Record.Value[1].(int32)] = item.Record.Value[0].(string)
		}
	}
	if len(rows) != 2 || rows[1] != "old" || rows[2] != "new" {
		t.Fatalf("projected rows after metadata edits = %v", rows)
	}
}
