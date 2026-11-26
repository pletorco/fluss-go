//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testArrowPartitionAppend(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "arrow_partition_append"}
	schema := fgo.Schema{
		Columns:      []fgo.Column{{Name: "id", Type: fgo.IntType}, {Name: "region", Type: fgo.StringType}},
		PartitionKey: []string{"region"},
	}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema: schema, BucketCount: 1, Properties: map[string]string{"table.log.format": "ARROW"},
	}, false); err != nil {
		t.Fatal(err)
	}
	if err := admin.CreatePartition(ctx, path, fadm.PartitionSpec{"region": "east"}, false); err != nil {
		t.Fatal(err)
	}
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	physical := fgo.PhysicalTablePath{TablePath: path, Partition: "east"}
	awaitPartitionReady(t, client, admin, table, physical, 1)
	w, err := client.NewAppendWriter(ctx, table, fgo.WithAppendPartition("east"), fgo.WithAppendBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	arrowSchema, err := table.Schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{7, 8}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"east", "west"}, nil)
	mixed := builder.NewRecordBatch()
	defer mixed.Release()
	if got := w.AppendArrow(ctx, 0, mixed, []fgo.ChangeType{fgo.Append, fgo.Append}).Await(ctx); !errors.Is(got.Err, fgo.ErrInvalidRow) {
		t.Fatalf("mixed batch: got %v, want ErrInvalidRow", got.Err)
	}
	builder.Field(0).(*array.Int32Builder).Append(9)
	builder.Field(1).(*array.StringBuilder).Append("east")
	valid := builder.NewRecordBatch()
	defer valid.Release()
	got := w.AppendArrow(ctx, 0, valid, []fgo.ChangeType{fgo.Append}).Await(ctx)
	if got.Err != nil || !got.OffsetKnown || got.BaseOffset != 0 || got.Records != 1 {
		t.Fatalf("expected first accepted write at offset zero: %+v", got)
	}
	scanner, err := client.NewLogScanner(ctx, table, fgo.Earliest(), fgo.WithScanPartition("east"), fgo.WithScanRowLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	result, err := scanner.Poll(ctx)
	defer result.Release()
	if err != nil || len(result.BucketErrors) != 0 || len(result.ArrowBatches) != 1 {
		t.Fatalf("poll valid record: batches=%d bucketErrors=%v err=%v", len(result.ArrowBatches), result.BucketErrors, err)
	}
	record := result.ArrowBatches[0].Batch.Record
	if record.NumRows() != 1 || record.Column(0).(*array.Int32).Value(0) != 9 || record.Column(1).(*array.String).Value(0) != "east" {
		t.Fatal("partition scan did not return the expected row")
	}
}
