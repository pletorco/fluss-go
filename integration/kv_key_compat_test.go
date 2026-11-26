//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func testPartitionedKVWireKeys(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "partitioned_key_compat"}
	schema := fgo.Schema{
		Columns: []fgo.Column{
			{Name: "region", Type: fgo.StringType},
			{Name: "tenant", Type: fgo.IntType},
			{Name: "session", Type: fgo.IntType},
			{Name: "name", Type: fgo.StringType, Nullable: true},
		},
		PrimaryKey:   []string{"region", "tenant", "session"},
		PartitionKey: []string{"region"},
		BucketKey:    []string{"tenant"},
	}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{Schema: schema, BucketCount: 1}, false); err != nil {
		t.Fatal(err)
	}
	if err := admin.CreatePartition(ctx, path, fadm.PartitionSpec{"region": "apac"}, false); err != nil {
		t.Fatal(err)
	}
	wire := readyWireKV(t, ctx, client, admin, fgo.PhysicalTablePath{TablePath: path, Partition: "apac"})
	writer, err := client.NewUpsertWriter(ctx, wire.table, fgo.WithUpsertPartition("apac"), fgo.WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	lookup, err := client.NewLookuper(ctx, wire.table, fgo.WithLookupPartition("apac"))
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()

	// The Fluss physical key is (tenant, session), using positive single-byte
	// compacted INTs. Keep these fixtures independent of the Go key encoder.
	first, second := []byte{7, 42}, []byte{7, 43}
	if result := writer.Upsert(ctx, fgo.Row{"apac", int32(7), int32(42), "go"}).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	wire.wantFound(t, ctx, first, true)
	wire.put(t, ctx, second, fgo.Row{"apac", int32(7), int32(43), "wire"})
	goKey := fgo.PrimaryKey{"apac", int32(7), int32(43)}
	if result := lookup.Lookup(ctx, goKey)[0]; result.Err != nil || !result.Found || result.Row[3] != "wire" {
		t.Fatalf("Go lookup of wire-written key: %#v", result)
	}
	prefix := lookup.PrefixLookup(ctx, fgo.PrimaryKey{"apac", int32(7)})[0]
	if prefix.Err != nil || len(prefix.Rows) != 2 || prefix.Rows[0][2] != int32(42) || prefix.Rows[1][2] != int32(43) {
		t.Fatalf("Go prefix lookup: %#v", prefix)
	}
	if result := writer.Delete(ctx, goKey).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	wire.wantFound(t, ctx, second, false)
}

func testWideDecimalKVWireKeys(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "decimal_key_compat"}
	schema := fgo.Schema{
		Columns: []fgo.Column{
			{Name: "amount", Type: fgo.DecimalType, LogicalType: &fgo.LogicalType{Root: "DECIMAL", Precision: 20, Scale: 2}},
			{Name: "name", Type: fgo.StringType, Nullable: true},
		},
		PrimaryKey: []string{"amount"},
	}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{Schema: schema, BucketCount: 1}, false); err != nil {
		t.Fatal(err)
	}
	wire := readyWireKV(t, ctx, client, admin, fgo.PhysicalTablePath{TablePath: path})
	writer, err := client.NewUpsertWriter(ctx, wire.table, fgo.WithUpsertBatchTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	lookup, err := client.NewLookuper(ctx, wire.table)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()

	// Use Java's minimal two's-complement form for the unscaled values.
	first, second := []byte{0x01, 0x80}, []byte{0x02, 0x80, 0x00}
	if result := writer.Upsert(ctx, fgo.Row{big.NewRat(-128, 100), "go"}).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	wire.wantFound(t, ctx, first, true)
	wire.wantFound(t, ctx, []byte{0x02, 0xff, 0x80}, false)

	amount := big.NewRat(-32768, 100)
	wire.put(t, ctx, second, fgo.Row{amount, "wire"})
	key := fgo.PrimaryKey{amount}
	if result := lookup.Lookup(ctx, key)[0]; result.Err != nil || !result.Found || result.Row[1] != "wire" {
		t.Fatalf("Go lookup of wire-written decimal key: %#v", result)
	}
	if result := writer.Delete(ctx, key).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	wire.wantFound(t, ctx, second, false)
}

// wireKV drives a one-bucket fixture through raw RPCs without the Go key
// encoder. The same fixtures can exercise both directions of interoperability.
type wireKV struct {
	client *fgo.Client
	path   fgo.PhysicalTablePath
	table  fgo.Table
	bucket fgo.TableBucket
}

func readyWireKV(t *testing.T, ctx context.Context, client *fgo.Client, admin *fadm.Client, path fgo.PhysicalTablePath) wireKV {
	t.Helper()
	table, err := client.GetTable(ctx, path.TablePath)
	if err != nil {
		t.Fatal(err)
	}
	wire := wireKV{client: client, path: path, table: table}
	if err := awaitTableReadiness(ctx, func(probeCtx context.Context) error {
		buckets, err := client.ResolveTableBuckets(probeCtx, path)
		if err != nil {
			return err
		}
		if len(buckets) != 1 {
			return fmt.Errorf("want one fixture bucket; got %d", len(buckets))
		}
		wire.bucket = buckets[0]
		results := admin.ListOffsets(probeCtx, table, path, wire.bucket.PartitionID, []int32{wire.bucket.BucketID}, fgo.Earliest())
		return tableOffsetReadinessError(1, results)
	}); err != nil {
		t.Fatalf("wire fixture did not become ready: %v", err)
	}
	return wire
}

func (w wireKV) partitionID() *int64 {
	if w.path.Partition != "" {
		return proto.Int64(w.bucket.PartitionID)
	}
	return nil
}

func (w wireKV) wantFound(t *testing.T, ctx context.Context, key []byte, want bool) {
	t.Helper()
	request, err := fmsg.NewRequest(fmsg.APIKeyLookup, 0)
	if err != nil {
		t.Fatal(err)
	}
	message := request.Message().(*fmsg.LookupRequest)
	message.TableId = proto.Int64(w.table.ID)
	message.BucketsReq = []*fmsg.PbLookupReqForBucket{{
		BucketId: proto.Int32(w.bucket.BucketID), PartitionId: w.partitionID(), Keys: [][]byte{key},
	}}
	response, err := w.client.RequestBucket(ctx, w.path, w.bucket.BucketID, request)
	if err != nil {
		t.Fatal(err)
	}
	buckets := response.Message().(*fmsg.LookupResponse).GetBucketsResp()
	if len(buckets) != 1 {
		t.Fatalf("wire lookup returned %d buckets", len(buckets))
	}
	bucket := buckets[0]
	if err := fgo.ResponseError(bucket.GetErrorCode(), bucket.GetErrorMessage(), fmsg.APIKeyLookup); err != nil {
		t.Fatal(err)
	}
	values := bucket.GetValues()
	if len(values) != 1 {
		t.Fatalf("wire lookup returned %d results", len(values))
	}
	if found := len(values[0].GetValues()) > 0; found != want {
		t.Fatalf("wire lookup key %x found = %t; want %t", key, found, want)
	}
}

func (w wireKV) put(t *testing.T, ctx context.Context, key []byte, row fgo.Row) {
	t.Helper()
	value, err := fgo.EncodeCompactedRow(w.table.Schema, row)
	if err != nil {
		t.Fatal(err)
	}
	w.putEncoded(t, ctx, key, value)
}

func (w wireKV) putEncoded(t *testing.T, ctx context.Context, key, value []byte) {
	t.Helper()
	init, err := fmsg.NewRequest(fmsg.APIKeyInitWriter, 0)
	if err != nil {
		t.Fatal(err)
	}
	init.Message().(*fmsg.InitWriterRequest).TablePath = []*fmsg.PbTablePath{{
		DatabaseName: proto.String(w.path.Database), TableName: proto.String(w.path.Table),
	}}
	response, err := w.client.RequestBucket(ctx, w.path, w.bucket.BucketID, init)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (fgo.KVBatch{
		SchemaID: int16(w.table.SchemaID), WriterID: response.Message().(*fmsg.InitWriterResponse).GetWriterId(),
		Records: []fgo.KVRecord{{Key: key, Value: value}},
	}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyPutKv, 0)
	if err != nil {
		t.Fatal(err)
	}
	message := request.Message().(*fmsg.PutKvRequest)
	message.TableId, message.Acks, message.TimeoutMs = proto.Int64(w.table.ID), proto.Int32(-1), proto.Int32(5000)
	message.BucketsReq = []*fmsg.PbPutKvReqForBucket{{
		BucketId: proto.Int32(w.bucket.BucketID), PartitionId: w.partitionID(), Records: batch,
	}}
	response, err = w.client.RequestBucket(ctx, w.path, w.bucket.BucketID, request)
	if err != nil {
		t.Fatal(err)
	}
	buckets := response.Message().(*fmsg.PutKvResponse).GetBucketsResp()
	if len(buckets) != 1 {
		t.Fatalf("wire put returned %d buckets", len(buckets))
	}
	if err := fgo.ResponseError(buckets[0].GetErrorCode(), buckets[0].GetErrorMessage(), fmsg.APIKeyPutKv); err != nil {
		t.Fatal(err)
	}
}
