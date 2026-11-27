//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testKVSnapshotResumeOffset(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "kv_snapshot_resume"}
	schema := fgo.Schema{Columns: []fgo.Column{
		{Name: "id", Type: fgo.IntType}, {Name: "name", Type: fgo.StringType},
	}, PrimaryKey: []string{"id"}}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema: schema, BucketCount: 1, Properties: map[string]string{"table.log.format": "COMPACTED"},
	}, false); err != nil {
		t.Fatal(err)
	}
	wire := readyWireKV(t, ctx, client, admin, fgo.PhysicalTablePath{TablePath: path})
	w, err := client.NewUpsertWriter(ctx, wire.table, fgo.WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	if got := w.Upsert(ctx, fgo.Row{int32(1), "in the snapshot"}).Await(ctx); got.Err != nil {
		t.Fatal(got.Err)
	}

	var snapshot fadm.KVSnapshot
	err = waitForCondition(ctx, 250*time.Millisecond, func() (bool, error) {
		latest, err := admin.GetLatestKVSnapshots(ctx, path, "")
		if err != nil || len(latest.Snapshots) != 1 {
			return false, nil
		}
		snapshot = latest.Snapshots[0]
		return snapshot.Available && snapshot.LogOffset > 0, nil
	})
	if err != nil {
		t.Fatalf("wait for a snapshot covering the first write: %v", err)
	}
	metadata, err := admin.GetKVSnapshotMetadata(ctx, wire.table.ID, -1, 0, snapshot.SnapshotID)
	if err != nil || len(metadata.Files) == 0 || metadata.LogOffset != snapshot.LogOffset || metadata.LogOffset != 1 {
		t.Fatalf("snapshot=%+v metadata=%+v err=%v; want an immutable snapshot with next log offset 1", snapshot, metadata, err)
	}

	// This write occurs after the immutable snapshot was published.
	if got := w.Upsert(ctx, fgo.Row{int32(99), "after the snapshot"}).Await(ctx); got.Err != nil {
		t.Fatal(got.Err)
	}
	readFrom := func(offset int64) []fgo.ScanRecord {
		t.Helper()
		scanner, err := client.NewLogScanner(ctx, wire.table, fgo.AtOffset(offset),
			fgo.WithLogFetchLimits(1<<20, 1<<20, 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		defer scanner.Close()
		result, err := scanner.Poll(ctx)
		defer result.Release()
		if err != nil || len(result.BucketErrors) != 0 {
			t.Fatalf("poll from %d: err=%v bucket errors=%v", offset, err, result.BucketErrors)
		}
		return result.Records
	}
	rows := readFrom(metadata.LogOffset)
	if len(rows) != 1 || rows[0].Record.Offset != metadata.LogOffset || rows[0].Record.Value[0] != int32(99) {
		t.Fatalf("resume at snapshot log offset %d: got %v, want the first post-snapshot write", metadata.LogOffset, rows)
	}
	if rows := readFrom(metadata.LogOffset + 1); len(rows) != 0 {
		t.Fatalf("control (+1) returned rows: %v", rows)
	}
}
