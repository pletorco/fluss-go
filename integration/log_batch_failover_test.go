//go:build integration

package integration_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fgo"
)

func openBatchScanOnLeader(
	t *testing.T, ctx context.Context, client *fgo.Client, table fgo.Table, leader int32,
) (*fgo.BatchScanner, int32) {
	t.Helper()
	buckets, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{TablePath: table.Path})
	if err != nil {
		t.Fatal(err)
	}
	for _, bucket := range buckets {
		if bucket.Leader.ID != leader {
			continue
		}
		scanner, err := client.NewBatchScanner(ctx, table, bucket, fgo.WithBatchLimit(1))
		if err != nil {
			t.Fatal(err)
		}
		return scanner, bucket.BucketID
	}
	t.Fatalf("tablet %d leads no buckets: %v", leader, buckets)
	return nil, 0
}

func assertBatchScanAfterFailover(
	t *testing.T, ctx context.Context, client *fgo.Client, table fgo.Table,
	oldScan *fgo.BatchScanner, bucketID, oldLeader int32,
) {
	t.Helper()
	buckets, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{TablePath: table.Path})
	if err != nil {
		t.Fatal(err)
	}
	bucket := buckets[bucketID]
	if bucket.BucketID != bucketID || bucket.Leader.ID == oldLeader {
		t.Fatalf("expected a new leader for bucket %d: %+v", bucketID, bucket)
	}
	fresh, err := client.NewBatchScanner(ctx, table, bucket, fgo.WithBatchLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	control, err := fresh.Poll(ctx)
	defer control.Release()
	if err != nil || !control.Done || len(control.Rows) != 1 {
		t.Fatalf("fresh scan = %+v, %v", control, err)
	}
	result, err := oldScan.Poll(ctx)
	defer result.Release()
	if err != nil || !result.Done || !reflect.DeepEqual(result.Rows, control.Rows) {
		t.Fatalf("original scan = %+v, %v, want fresh scan rows %v", result, err, control.Rows)
	}
}
