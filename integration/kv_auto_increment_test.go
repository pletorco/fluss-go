//go:build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
)

func testPartialUpsertAutoIncrementBoundary(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	for _, tc := range []struct {
		name     string
		nullable bool
	}{{"nullable", true}, {"required", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			path := fgo.TablePath{Database: database, Table: "autoinc_" + tc.name}
			schema := fgo.Schema{Columns: []fgo.Column{
				{Name: "id", Type: fgo.IntType}, {Name: "name", Type: fgo.StringType, Nullable: true},
				{Name: "generated_id", Type: fgo.BigIntType, Nullable: tc.nullable},
			}, PrimaryKey: []string{"id"}, AutoIncrement: []string{"generated_id"}}
			if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{Schema: schema, BucketCount: 1}, false); err != nil {
				t.Fatal(err)
			}
			wire := readyWireKV(t, ctx, client, admin, fgo.PhysicalTablePath{TablePath: path})
			w, err := client.NewUpsertWriter(ctx, wire.table, fgo.WithUpsertBatchTimeout(0))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close(ctx)
			if r := w.PartialUpsert(ctx, []string{"id", "generated_id"}, fgo.Row{int32(9), int64(42)}).Await(ctx); !errors.Is(r.Err, fgo.ErrInvalidRow) {
				t.Fatalf("explicit generated column selection = %+v, want ErrInvalidRow", r)
			}
			insert := w.PartialUpsert(ctx, []string{"id", "name"}, fgo.Row{int32(9), "first"}).Await(ctx)
			if !tc.nullable {
				if !errors.Is(insert.Err, fgo.ErrUnsupportedAPI) {
					t.Fatalf("required generated column = %+v, want ErrUnsupportedAPI", insert)
				}
				assertFlussRejectsRequiredAutoIncrement(t, ctx, wire)
				wire.wantFound(t, ctx, []byte{9}, false)
				return
			}
			if insert.Err != nil {
				t.Fatal(insert.Err)
			}
			l, err := client.NewLookuper(ctx, wire.table)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			generated := readPartialAutoIncrement(t, ctx, l, "first")
			if r := w.PartialUpsert(ctx, []string{"name", "id"}, fgo.Row{"updated", int32(9)}).Await(ctx); r.Err != nil {
				t.Fatal(r.Err)
			}
			if got := readPartialAutoIncrement(t, ctx, l, "updated"); got != generated {
				t.Fatalf("partial update changed generated value from %d to %d", generated, got)
			}
		})
	}
}

func readPartialAutoIncrement(t *testing.T, ctx context.Context, l *fgo.Lookuper, name string) int64 {
	t.Helper()
	result := l.Lookup(ctx, fgo.PrimaryKey{int32(9)})[0]
	if result.Err != nil || !result.Found || len(result.Row) != 3 || result.Row[1] != name {
		t.Fatalf("lookup = %+v, want name %s", result, name)
	}
	value, ok := result.Row[2].(int64)
	if !ok {
		t.Fatalf("server did not fill generated value: %+v", result)
	}
	return value
}

func assertFlussRejectsRequiredAutoIncrement(t *testing.T, ctx context.Context, wire wireKV) {
	t.Helper()
	// Bypass the Go validation with a correctly framed full-width row. Only
	// field 2 is null; the target columns do not include that generated field.
	value := []byte{0x04, 9, 1, 'x'} // Null bitmap, varint INT key, and compacted STRING.
	result := wire.putEncodedWithTargets(t, ctx, []byte{9}, value, []int32{0, 1})
	if result.GetErrorCode() != int32(fmsg.ErrorCodeInvalidTargetColumn) ||
		!strings.Contains(result.GetErrorMessage(), "column generated_id is NOT NULL") {
		t.Fatalf("raw partial write response = %v, want INVALID_TARGET_COLUMN for generated_id", result)
	}
}
