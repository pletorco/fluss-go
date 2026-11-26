//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testPartitionBucketRescale(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "rescale_events"}
	schema := fgo.Schema{
		Columns:      []fgo.Column{{Name: "id", Type: fgo.IntType}, {Name: "region", Type: fgo.StringType}},
		PartitionKey: []string{"region"},
		BucketKey:    []string{"id"},
	}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{Schema: schema, BucketCount: 1}, false); err != nil {
		t.Fatal(err)
	}
	if err := admin.CreatePartition(ctx, path, fadm.PartitionSpec{"region": "old"}, false); err != nil {
		t.Fatal(err)
	}
	count := int32(2)
	if err := admin.AlterTable(ctx, path, fadm.AlterTable{BucketCount: &count}, false); err != nil {
		t.Fatalf("change the default for future partitions: %v", err)
	}
	if err := admin.CreatePartition(ctx, path, fadm.PartitionSpec{"region": "new"}, false); err != nil {
		t.Fatal(err)
	}
	table, err := client.GetTable(ctx, path)
	if err != nil || table.BucketCount != 2 || !table.BucketCountEpochKnown || table.BucketCountEpoch <= 0 {
		t.Fatalf("table after rescale: buckets=%d epoch=%d known=%v, error=%v", table.BucketCount, table.BucketCountEpoch, table.BucketCountEpochKnown, err)
	}
	for _, test := range []struct {
		partition string
		count     int
	}{{"old", 1}, {"new", 2}} {
		t.Run(test.partition, func(t *testing.T) {
			physical := fgo.PhysicalTablePath{TablePath: path, Partition: test.partition}
			buckets := awaitRescalePartition(t, client, admin, table, physical, test.count)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			writer, err := client.NewAppendWriter(ctx, table, fgo.WithAppendPartition(test.partition), fgo.WithAppendBatchTimeout(0))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := writer.Close(ctx); err != nil {
					t.Errorf("close writer: %v", err)
				}
			}()
			written := make(map[int32][]int32, len(buckets))
			for id := range int32(8) {
				result := writer.Append(ctx, fgo.Row{id, test.partition}).Await(ctx)
				if result.Err != nil || !result.OffsetKnown || result.Bucket < 0 || int(result.Bucket) >= len(buckets) {
					t.Fatalf("append row %d: %+v", id, result)
				}
				written[result.Bucket] = append(written[result.Bucket], id)
			}
			if len(written) != len(buckets) {
				t.Fatalf("sample keys used %d buckets, expected %d", len(written), len(buckets))
			}
		})
	}
}

func awaitRescalePartition(
	t *testing.T, client *fgo.Client, admin *fadm.Client, table fgo.Table, path fgo.PhysicalTablePath, count int,
) []fgo.TableBucket {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	var buckets []fgo.TableBucket
	err := awaitTableReadiness(ctx, func(ctx context.Context) error {
		var err error
		buckets, err = client.ResolveTableBuckets(ctx, path)
		if err != nil {
			return err
		}
		if len(buckets) != count || buckets[0].BucketCount != int32(count) {
			return fmt.Errorf("partition %s returned %d buckets, want layout of %d", path, len(buckets), count)
		}
		ids := make([]int32, count)
		for index, bucket := range buckets {
			ids[index] = bucket.BucketID
		}
		return tableOffsetReadinessError(count, admin.ListOffsets(ctx, table, path, buckets[0].PartitionID, ids, fgo.Earliest()))
	})
	if err != nil {
		t.Fatalf("partition %s did not become ready: %v", path, err)
	}
	return buckets
}
