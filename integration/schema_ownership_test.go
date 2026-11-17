//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fgo"
)

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
