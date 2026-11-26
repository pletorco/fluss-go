//go:build integration

package integration_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testKVMutationOffsets(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "kv_mutation_offsets"}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema:      fgo.Schema{Columns: []fgo.Column{{Name: "id", Type: fgo.IntType}, {Name: "name", Type: fgo.StringType}}, PrimaryKey: []string{"id"}},
		BucketCount: 1, Properties: map[string]string{"table.log.format": "COMPACTED"},
	}, false); err != nil {
		t.Fatal(err)
	}
	waitForTableReady(t, admin, path)
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := client.NewUpsertWriter(ctx, table, fgo.WithUpsertBatchLimits(1<<20, 2), fgo.WithUpsertBatchTimeout(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	for _, value := range []string{"old", "new"} {
		first, second := w.Upsert(ctx, fgo.Row{int32(1), value}), w.Upsert(ctx, fgo.Row{int32(2), value})
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		assertKVAckWithoutOffset(t, first.Await(ctx))
		assertKVAckWithoutOffset(t, second.Await(ctx))
	}
	s, err := client.NewLogScanner(ctx, table, fgo.Earliest(), fgo.WithScanRowLimit(6))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var offsets = map[int32][]int64{}
	for !s.Done() {
		result, err := s.Poll(ctx)
		result.Release()
		if err != nil || len(result.BucketErrors) != 0 {
			t.Fatalf("CDC read failed: %v / %v", result.BucketErrors, err)
		}
		for _, record := range result.Records {
			id := record.Record.Value[0].(int32)
			offsets[id] = append(offsets[id], record.Record.Offset)
		}
	}
	if want := (map[int32][]int64{1: {0, 2, 3}, 2: {1, 4, 5}}); !reflect.DeepEqual(offsets, want) {
		t.Fatalf("CDC offsets = %v, want %v", offsets, want)
	}
	deleted := w.Delete(ctx, fgo.PrimaryKey{int32(999)})
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	assertKVAckWithoutOffset(t, deleted.Await(ctx))
}

func assertKVAckWithoutOffset(t *testing.T, result fgo.WriteResult) {
	t.Helper()
	if result.Err != nil || result.Records != 1 || result.OffsetKnown || result.BaseOffset != 0 {
		t.Fatalf("KV write = %+v, want acknowledgment without mutation offset", result)
	}
}
