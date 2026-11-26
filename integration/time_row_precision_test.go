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

func testTimeRowPrecision(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "time_precision"}
	schema := fgo.Schema{Columns: []fgo.Column{{
		Name: "clock", Type: fgo.TimeType, LogicalType: &fgo.LogicalType{Root: "TIME_WITHOUT_TIME_ZONE", Precision: 9},
	}}}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{Schema: schema, BucketCount: 1,
		Properties: map[string]string{"table.log.format": "COMPACTED"}}, false); err != nil {
		t.Fatal(err)
	}
	waitForTableReady(t, admin, path)
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := client.NewAppendWriter(ctx, table, fgo.WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	clock := time.Date(1970, 1, 1, 12, 34, 56, 123456789, time.UTC)
	if result := writer.Append(ctx, fgo.Row{clock}).Await(ctx); !errors.Is(result.Err, fgo.ErrInvalidRow) {
		t.Fatalf("sub-millisecond TIME(9) append = %+v", result)
	}
	clock = clock.Truncate(time.Millisecond)
	if result := writer.Append(ctx, fgo.Row{clock}).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	scanner, err := client.NewLogScanner(ctx, table, fgo.Earliest(), fgo.WithScanRowLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	for !scanner.Done() {
		result, err := scanner.Poll(ctx)
		result.Release()
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Records) == 1 && result.Records[0].Record.Value[0].(time.Time).Equal(clock) {
			return
		}
	}
	t.Fatal("millisecond TIME(9) did not round-trip")
}
