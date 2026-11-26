//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testDDLZeroParameters(t *testing.T, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "zero_schema_parameters"}
	columns := []fgo.Column{
		{Name: "whole", Type: fgo.DecimalType, LogicalType: &fgo.LogicalType{Root: "DECIMAL", Precision: 20}},
		{Name: "clock", Type: fgo.TimeType, LogicalType: &fgo.LogicalType{Root: "TIME_WITHOUT_TIME_ZONE"}},
		{Name: "stamp", Type: fgo.TimestampType, LogicalType: &fgo.LogicalType{Root: "TIMESTAMP_WITHOUT_TIME_ZONE"}},
	}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{Schema: fgo.Schema{Columns: columns}, BucketCount: 1}, false); err != nil {
		t.Fatalf("CreateTable with zero parameters: %v", err)
	}
	if err := admin.AlterTable(ctx, path, fadm.AlterTable{Add: []fadm.AddColumn{{
		Name: "whole_added", Type: fgo.LogicalType{Root: "DECIMAL", Precision: 20, Nullable: true},
	}}}, false); err != nil {
		t.Fatalf("AlterTable with decimal scale zero: %v", err)
	}
}

func testDDLReusesNestedSchema(t *testing.T, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	nested := fgo.LogicalType{Root: "ROW", Fields: []fgo.LogicalField{{
		Name: "count", Type: fgo.LogicalType{Root: "INTEGER"},
	}}}
	desc := fadm.TableDescriptor{
		Schema: fgo.Schema{Columns: []fgo.Column{
			{Name: "id", Type: fgo.IntType},
			{Name: "details", Type: fgo.RowType, LogicalType: &nested},
		}},
		BucketCount: 1,
	}
	for _, name := range []string{"initial_nested_schema", "reused_nested_schema"} {
		path := fgo.TablePath{Database: database, Table: name}
		if err := admin.CreateTable(ctx, path, desc, false); err != nil {
			t.Fatalf("CreateTable using shared descriptor (%s): %v", name, err)
		}
	}
	if nested.Fields[0].ID != 0 {
		t.Fatal("CreateTable changed the caller's nested schema")
	}
}
