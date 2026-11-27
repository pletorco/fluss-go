//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testReadyLogBucket(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "ready_log_bucket"}
	schema := fgo.Schema{Columns: []fgo.Column{{Name: "id", Type: fgo.IntType}}}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema: schema, BucketCount: 2, Properties: map[string]string{"table.log.format": "COMPACTED"},
	}, false); err != nil {
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
	write := writer.Append(ctx, fgo.Row{int32(99)}).Await(ctx)
	if write.Err != nil {
		t.Fatal(write.Err)
	}
	scanner, err := client.NewLogScanner(ctx, table, fgo.Earliest())
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	poll, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	result, err := scanner.Poll(poll)
	defer result.Release()
	if err != nil || len(result.BucketErrors) != 0 || len(result.Records) != 1 ||
		result.Records[0].Bucket != write.Bucket || result.Records[0].Record.Value[0] != int32(99) {
		t.Fatalf("Poll = %+v, %v; want the ready bucket %d within the caller deadline", result, err, write.Bucket)
	}
}
