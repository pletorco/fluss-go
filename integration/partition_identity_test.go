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

func testPartitionAfterTableRecreation(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	path := fgo.TablePath{Database: database, Table: "partition_recreated"}
	physical := fgo.PhysicalTablePath{TablePath: path, Partition: "kr"}
	schema := fgo.Schema{
		Columns:      []fgo.Column{{Name: "id", Type: fgo.IntType}, {Name: "region", Type: fgo.StringType}},
		PartitionKey: []string{"region"},
	}
	create := func() fgo.Table {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{Schema: schema, BucketCount: 1}, false); err != nil {
			t.Fatal(err)
		}
		if err := admin.CreatePartition(ctx, path, fadm.PartitionSpec{"region": physical.Partition}, false); err != nil {
			t.Fatal(err)
		}
		table, err := client.GetTable(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		return table
	}
	old := create()
	oldBucket := awaitPartitionReady(t, client, admin, old, physical, 1)[0]
	dropCtx, cancelDrop := context.WithTimeout(t.Context(), 30*time.Second)
	err := admin.DropTable(dropCtx, path, false)
	cancelDrop()
	if err != nil {
		t.Fatal(err)
	}
	current := create()
	bucket := awaitPartitionReady(t, client, admin, current, physical, 1)[0]
	if old.ID == current.ID || oldBucket.TableID != old.ID || bucket.TableID != current.ID {
		t.Fatalf("old table/bucket IDs %d/%d, current IDs %d/%d", old.ID, oldBucket.TableID, current.ID, bucket.TableID)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if writer, err := client.NewAppendWriter(ctx, old, fgo.WithAppendPartition(physical.Partition)); !errors.Is(err, fgo.ErrMetadata) || writer != nil {
		if writer != nil {
			_ = writer.Close(ctx)
		}
		t.Fatalf("append writer with stale table: %v, want ErrMetadata", err)
	}
	if scanner, err := client.NewLogScanner(ctx, old, fgo.AtOffset(0), fgo.WithScanPartition(physical.Partition)); !errors.Is(err, fgo.ErrMetadata) || scanner != nil {
		if scanner != nil {
			_ = scanner.Close()
		}
		t.Fatalf("log scanner with stale table: %v, want ErrMetadata", err)
	}
	writer, err := client.NewAppendWriter(ctx, current, fgo.WithAppendPartition(physical.Partition), fgo.WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := writer.Close(ctx); err != nil {
			t.Errorf("close append writer: %v", err)
		}
	}()
	if result := writer.Append(ctx, fgo.Row{int32(2), physical.Partition}).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	assertLogBucketRows(t, ctx, client, current, bucket, physical.Partition, []int32{2})
}
