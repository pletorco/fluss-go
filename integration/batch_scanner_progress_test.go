//go:build integration

package integration_test

import (
	"context"
	"errors"
	"sync"
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
	for _, test := range []struct {
		name          string
		batchBytes    int32
		interruptPage int32
	}{
		{name: "decode paginated", batchBytes: 1},
		{name: "decode terminal", batchBytes: 1 << 20},
		{name: "initial reply", batchBytes: 1, interruptPage: 1},
		{name: "continuation reply", batchBytes: 1, interruptPage: 2},
		{name: "terminal reply", batchBytes: 1 << 20, interruptPage: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			pollCtx, cancelPoll := context.WithCancel(ctx)
			defer cancelPoll()
			resume := make(chan struct{})
			resumeReply := sync.OnceFunc(func() { close(resume) })
			var armed atomic.Bool
			var scanCalls atomic.Int32
			scanClient := openClient(t, []string{address}, fgo.WithMetricsObserver(fgo.MetricsObserverFunc(func(event fgo.MetricEvent) {
				if event.Kind != fgo.MetricRequest || event.Failed {
					return
				}
				if event.APIKey == fmsg.APIKeyScanKv {
					if scanCalls.Add(1) == test.interruptPage && armed.CompareAndSwap(true, false) {
						// Fluss already produced the response. Hold it before the
						// scanner sees it, even after the caller's Poll is canceled.
						cancelPoll()
						select {
						case <-resume:
						case <-ctx.Done():
						}
					}
				}
				if test.interruptPage == 0 && event.APIKey == fmsg.APIKeyGetTableSchema && armed.CompareAndSwap(true, false) {
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
				fgo.WithBatchSizeBytes(test.batchBytes), fgo.WithBatchProjection("name", "id", "extra"))
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			defer resumeReply()
			var rows []fgo.Row
			requestsBefore := max(test.interruptPage-1, 0)
			for range requestsBefore {
				result, err := scanner.Poll(ctx)
				defer result.Release()
				if err != nil || result.Done || len(result.Rows) == 0 {
					t.Fatalf("page before interruption = %#v, %v", result, err)
				}
				rows = append(rows, result.Rows...)
			}
			armed.Store(true)
			interrupted, err := scanner.Poll(pollCtx)
			defer interrupted.Release()
			if !errors.Is(err, context.Canceled) || len(interrupted.Rows) != 0 || scanner.Done() || armed.Load() {
				t.Fatalf("interrupted Poll = %#v, %v; done=%v, armed=%v", interrupted, err, scanner.Done(), armed.Load())
			}
			wantCalls := requestsBefore + 1
			if got := scanCalls.Load(); got != wantCalls {
				t.Fatalf("interrupted Poll made %d ScanKv requests, want %d", got, wantCalls)
			}
			resumeReply()
			first, err := scanner.Poll(ctx)
			defer first.Release()
			if err != nil || len(first.Rows) == 0 || scanCalls.Load() != wantCalls || test.batchBytes > 1 && !first.Done {
				t.Fatalf("retained-page Poll = %#v, %v; requests=%d", first, err, scanCalls.Load())
			}
			rows = append(rows, first.Rows...)
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
