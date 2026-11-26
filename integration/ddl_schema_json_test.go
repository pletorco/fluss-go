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
