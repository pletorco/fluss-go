//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
)

func testInterruptedKVPage(t *testing.T, address string, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "kv_decode_retry"}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema: fgo.Schema{
			Columns:    []fgo.Column{{Name: "id", Type: fgo.IntType}, {Name: "name", Type: fgo.StringType}},
			PrimaryKey: []string{"id"},
		},
		BucketCount: 1,
	}, false); err != nil {
		t.Fatal(err)
	}
	waitForTableReady(t, admin, path)
	oldTable, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := client.NewUpsertWriter(ctx, oldTable, fgo.WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.Background())
	upsertRows(t, ctx, writer, []fgo.Row{{int32(1), "one"}, {int32(2), "two"}, {int32(3), "three"}})
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := admin.AlterTable(ctx, path, fadm.AlterTable{Add: []fadm.AddColumn{{
		Name: "extra", Type: fgo.LogicalType{Root: "BIGINT", Nullable: true},
	}}}, false); err != nil {
		t.Fatal(err)
	}
	_ = waitForSchemaChange(t, ctx, client, path, oldTable.SchemaID)
	for _, batchBytes := range []int32{1, 1 << 20} {
		t.Run(fmt.Sprintf("batch_bytes_%d", batchBytes), func(t *testing.T) {
			pollCtx, cancelPoll := context.WithCancel(ctx)
			defer cancelPoll()
			var armed atomic.Bool
			var scanCalls atomic.Int32
			scanClient := openClient(t, []string{address}, fgo.WithMetricsObserver(fgo.MetricsObserverFunc(func(event fgo.MetricEvent) {
				if event.Kind != fgo.MetricRequest || event.Failed {
					return
				}
				if event.APIKey == fmsg.APIKeyScanKv {
					scanCalls.Add(1)
				}
				if event.APIKey == fmsg.APIKeyGetTableSchema && armed.CompareAndSwap(true, false) {
					cancelPoll()
				}
			})))
			defer scanClient.Close()
			// This fresh client caches only the current schema, forcing Poll to
			// resolve the historical schema carried by the stored KV records.
			table, err := scanClient.GetTable(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			buckets, err := scanClient.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{TablePath: path})
			if err != nil || len(buckets) != 1 {
				t.Fatalf("buckets = %#v, %v", buckets, err)
			}
			scanner, err := scanClient.NewBatchScanner(ctx, table, buckets[0],
				fgo.WithBatchSizeBytes(batchBytes), fgo.WithBatchProjection("name", "id", "extra"))
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			armed.Store(true)
			interrupted, err := scanner.Poll(pollCtx)
			defer interrupted.Release()
			if !errors.Is(err, context.Canceled) || len(interrupted.Rows) != 0 || scanner.Done() || armed.Load() {
				t.Fatalf("interrupted Poll = %#v, %v; done=%v, armed=%v", interrupted, err, scanner.Done(), armed.Load())
			}
			if got := scanCalls.Load(); got != 1 {
				t.Fatalf("interrupted Poll made %d ScanKv requests, want 1", got)
			}
			first, err := scanner.Poll(ctx)
			defer first.Release()
			if err != nil || len(first.Rows) == 0 || scanCalls.Load() != 1 || batchBytes > 1 && !first.Done {
				t.Fatalf("retained-page Poll = %#v, %v; requests=%d", first, err, scanCalls.Load())
			}
			rows := append([]fgo.Row(nil), first.Rows...)
			for !scanner.Done() && ctx.Err() == nil {
				result, err := scanner.Poll(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rows = append(rows, result.Rows...)
				result.Release()
			}
			seen := make(map[int32]string)
			for _, row := range rows {
				if len(row) != 3 || row[2] != nil {
					t.Fatalf("projected/evolved row = %#v", row)
				}
				seen[row[1].(int32)] = row[0].(string)
			}
			if len(rows) != 3 || seen[1] != "one" || seen[2] != "two" || seen[3] != "three" {
				t.Fatalf("retried scan lost or duplicated rows: %#v; context=%v", rows, ctx.Err())
			}
			requests := scanCalls.Load()
			if err := scanner.Close(); err != nil || scanCalls.Load() != requests {
				t.Fatalf("Close contacted terminal session: %v; requests %d -> %d", err, requests, scanCalls.Load())
			}
		})
	}
}
