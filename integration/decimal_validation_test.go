//go:build integration

package integration_test

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testDecimalInputs(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	for _, format := range []string{"compacted", "indexed", "kv"} {
		t.Run(format, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			schema := fgo.Schema{Columns: []fgo.Column{
				{Name: "id", Type: fgo.IntType},
				{Name: "amount", Type: fgo.DecimalType, Nullable: true,
					LogicalType: &fgo.LogicalType{Root: "DECIMAL", Precision: 8, Scale: 2, Nullable: true}},
				{Name: "wide", Type: fgo.DecimalType, Nullable: true,
					LogicalType: &fgo.LogicalType{Root: "DECIMAL", Precision: 20, Scale: 2, Nullable: true}},
			}}
			descriptor := fadm.TableDescriptor{Schema: schema, BucketCount: 1}
			if format == "kv" {
				descriptor.Schema.PrimaryKey = []string{"id"}
				descriptor.Schema.BucketKey = []string{"id"}
			} else {
				descriptor.Properties = map[string]string{"table.log.format": strings.ToUpper(format)}
			}
			path := fgo.TablePath{Database: database, Table: "decimals_" + format}
			if err := admin.CreateTable(ctx, path, descriptor, false); err != nil {
				t.Fatal(err)
			}
			waitForTableReady(t, admin, path)
			table, err := client.GetTable(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			rows := []fgo.Row{
				{int32(1), new(big.Rat), new(big.Rat)},
				{int32(2), nil, nil},
				{int32(3), big.NewRat(-123, 100), big.NewRat(1234567890123456789, 100)},
			}
			if format == "kv" {
				testDecimalKV(t, ctx, client, table, rows)
			} else {
				testDecimalLog(t, ctx, client, table, fgo.LogFormat(format), rows)
			}
		})
	}
}

func testDecimalLog(t *testing.T, ctx context.Context, client *fgo.Client, table fgo.Table, format fgo.LogFormat, rows []fgo.Row) {
	t.Helper()
	writer, err := client.NewAppendWriter(ctx, table, fgo.WithAppendLogFormat(format), fgo.WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.Background())
	for _, invalid := range []fgo.Row{{int32(99), (*big.Rat)(nil), nil}, {int32(99), nil, (*big.Rat)(nil)}} {
		if result := writer.Append(ctx, invalid).Await(ctx); !errors.Is(result.Err, fgo.ErrInvalidRow) {
			t.Fatalf("invalid decimal append = %+v", result)
		}
	}
	for _, row := range rows {
		if result := writer.Append(ctx, row).Await(ctx); result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	scanner, err := client.NewLogScanner(ctx, table, fgo.Earliest(), fgo.WithScanRowLimit(3))
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	for count := 0; count < len(rows); {
		result, err := scanner.Poll(ctx)
		if err != nil || len(result.BucketErrors) != 0 {
			result.Release()
			t.Fatalf("decimal scan = %v, %v", result.BucketErrors, err)
		}
		for _, record := range result.Records {
			if count >= len(rows) {
				result.Release()
				t.Fatal("rejected decimal input was stored")
			}
			assertDecimalRow(t, record.Record.Value, rows[count])
			count++
		}
		result.Release()
	}
}

func testDecimalKV(t *testing.T, ctx context.Context, client *fgo.Client, table fgo.Table, rows []fgo.Row) {
	t.Helper()
	writer, err := client.NewUpsertWriter(ctx, table, fgo.WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.Background())
	invalid := fgo.Row{int32(99), (*big.Rat)(nil), nil}
	for _, future := range []*fgo.WriteFuture{
		writer.Upsert(ctx, invalid),
		writer.PartialUpsert(ctx, []string{"id", "amount"}, invalid[:2]),
		writer.Upsert(ctx, fgo.Row{int32(99), nil, (*big.Rat)(nil)}),
	} {
		if result := future.Await(ctx); !errors.Is(result.Err, fgo.ErrInvalidRow) {
			t.Fatalf("invalid decimal upsert = %+v", result)
		}
	}
	for _, row := range rows {
		if result := writer.Upsert(ctx, row).Await(ctx); result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	lookup, err := client.NewLookuper(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	for _, row := range rows {
		result := lookup.Lookup(ctx, fgo.PrimaryKey{row[0]})
		if len(result) != 1 || result[0].Err != nil || !result[0].Found {
			t.Fatalf("decimal lookup = %+v", result)
		}
		assertDecimalRow(t, result[0].Row, row)
	}
	result := lookup.Lookup(ctx, fgo.PrimaryKey{int32(99)})
	if len(result) != 1 || !errors.Is(result[0].Err, fgo.ErrNotFound) || result[0].Found {
		t.Fatalf("rejected decimal input lookup = %+v", result)
	}
}

func assertDecimalRow(t *testing.T, got, want fgo.Row) {
	t.Helper()
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("decimal row = %v; want %v", got, want)
	}
	for i := 1; i < len(want); i++ {
		if want[i] == nil {
			if got[i] != nil {
				t.Fatalf("decimal column %d = %v; want NULL", i, got[i])
			}
		} else if decimal, ok := got[i].(*big.Rat); !ok || decimal == nil || decimal.Cmp(want[i].(*big.Rat)) != 0 {
			t.Fatalf("decimal column %d = %v; want %v", i, got[i], want[i])
		}
	}
}
