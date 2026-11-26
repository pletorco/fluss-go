package fgo

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestKVPressureDoesNotReserveRPCSlots(t *testing.T) {
	for _, barrier := range []string{"flush respects throttle", "close skips throttle"} {
		t.Run(barrier, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				table := upsertWriterTable()
				table.BucketCount = 2
				backend := kvBackend(0, 1)
				backend.putResults = []kvPutResult{{logEnd: 1, pressure: 0.5, pressureKnown: true}}
				writer, err := newUpsertWriter(ctx, backend, table, WithUpsertBatchTimeout(0),
					WithUpsertBackpressureMaxThrottle(time.Second), WithUpsertConcurrency(1))
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close(ctx)
				keys := testPressureBucketKeys(t, table.Schema)
				prime := writer.Upsert(ctx, Row{keys[0], "prime", nil}).Await(ctx)
				if prime.Err != nil || !prime.PressureKnown || prime.Bucket != 0 {
					t.Fatalf("prime = %+v", prime)
				}
				start := time.Now()
				early := writer.Upsert(ctx, Row{keys[0], "delayed-1", nil})
				late := writer.Upsert(ctx, Row{keys[0], "delayed-2", nil})
				synctest.Wait()
				healthy := writer.Upsert(ctx, Row{keys[1], "eligible", nil}).Await(ctx)
				if healthy.Err != nil || healthy.Bucket != 1 || time.Since(start) != 0 || len(backend.putCalls()) != 2 {
					t.Fatalf("wanted immediate healthy RPC; result=%+v elapsed=%v count=%d", healthy, time.Since(start), len(backend.putCalls()))
				}
				if barrier == "flush respects throttle" {
					if err := writer.Flush(ctx); err != nil || time.Since(start) != 250*time.Millisecond {
						t.Fatalf("flush after %v: %v", time.Since(start), err)
					}
				} else if err := writer.Close(ctx); err != nil || time.Since(start) != 0 {
					t.Fatalf("close after %v: %v", time.Since(start), err)
				}
				for _, result := range []WriteResult{early.Await(ctx), late.Await(ctx)} {
					if result.Err != nil || result.Bucket != 0 {
						t.Fatalf("throttled write = %+v", result)
					}
				}
				calls := backend.putCalls()
				if len(calls) != 4 {
					t.Fatalf("RPCs = %d, want 4", len(calls))
				}
				for i, want := range []struct{ bucket, sequence int32 }{{0, 0}, {1, 0}, {0, 1}, {0, 2}} {
					batch, err := DecodeKVBatch(calls[i].records)
					if err != nil || calls[i].bucket != want.bucket || batch.BatchSequence != want.sequence {
						t.Fatalf("RPC %d bucket=%d batch=%+v: %v", i, calls[i].bucket, batch, err)
					}
				}
			})
		})
	}
}

func testPressureBucketKeys(t *testing.T, schema Schema) map[int32]int32 {
	t.Helper()
	keys := make(map[int32]int32)
	for id := int32(0); id < 100 && len(keys) < 2; id++ {
		key, err := EncodePrimaryKey(schema, PrimaryKey{id})
		if err != nil {
			t.Fatal(err)
		}
		bucket, err := flussBucket(key, 2)
		if err != nil {
			t.Fatal(err)
		}
		keys[bucket] = id
	}
	if len(keys) != 2 {
		t.Fatal("fixture could not route to two buckets")
	}
	return keys
}
