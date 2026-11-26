package fgo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestKeyedOperationsDoNotRemapBucketsWithoutLeaders(t *testing.T) {
	for _, partition := range []string{"", "day=2026-07-30"} {
		name := "table"
		if partition != "" {
			name = "partition"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			table := lookupTable()
			table.BucketCount = 1 // The handle is stale; routing must use fresh physical metadata.
			metadata := metadataResponse(table.Path)
			metadata.TableMetadata[0].TableId = proto.Int64(table.ID)
			metadata.PartitionMetadata[0].TableId = proto.Int64(table.ID)
			full := []*fmsg.PbBucketMetadata{{BucketId: proto.Int32(0), LeaderId: proto.Int32(2)}, {BucketId: proto.Int32(1)}}
			metadata.TableMetadata[0].BucketMetadata = full
			metadata.PartitionMetadata[0].BucketMetadata = append(append([]*fmsg.PbBucketMetadata(nil), full...), &fmsg.PbBucketMetadata{BucketId: proto.Int32(2)})
			lookupOptions := []LookupOption{}
			writerOptions := []UpsertWriterOption{WithUpsertBatchTimeout(0)}
			count, partitionID := int32(2), int64(-1)
			if partition != "" {
				count, partitionID = 3, metadata.PartitionMetadata[0].GetPartitionId()
				lookupOptions = append(lookupOptions, WithLookupPartition(partition))
				writerOptions = append(writerOptions, WithUpsertPartition(partition))
			}
			client := newBucketRoutingTestClient(t, metadata, func(_ context.Context, req fmsg.Request) (fmsg.Response, error) {
				response, err := fmsg.NewResponse(req.APIKey(), req.Version())
				if err != nil {
					return nil, err
				}
				var bucket, routingCount int32
				var routedPartition int64
				switch input := req.(*fmsg.MessageRequest).Message().(type) {
				case *fmsg.InitWriterRequest:
					response.Message().(*fmsg.InitWriterResponse).WriterId = proto.Int64(1)
					return response, nil
				case *fmsg.LookupRequest:
					b := input.GetBucketsReq()[0]
					bucket, routingCount, routedPartition = b.GetBucketId(), b.GetRoutingBucketCount(), b.GetPartitionId()
					response.Message().(*fmsg.LookupResponse).BucketsResp = []*fmsg.PbLookupRespForBucket{{BucketId: b.BucketId, Values: []*fmsg.PbValue{{}}}}
				case *fmsg.PrefixLookupRequest:
					b := input.GetBucketsReq()[0]
					bucket, routingCount, routedPartition = b.GetBucketId(), b.GetRoutingBucketCount(), b.GetPartitionId()
					response.Message().(*fmsg.PrefixLookupResponse).BucketsResp = []*fmsg.PbPrefixLookupRespForBucket{{BucketId: b.BucketId, ValueLists: []*fmsg.PbValueList{{}}}}
				case *fmsg.PutKvRequest:
					b := input.GetBucketsReq()[0]
					bucket, routingCount, routedPartition = b.GetBucketId(), b.GetRoutingBucketCount(), b.GetPartitionId()
					response.Message().(*fmsg.PutKvResponse).BucketsResp = []*fmsg.PbPutKvRespForBucket{{BucketId: b.BucketId}}
				default:
					return nil, fmt.Errorf("unexpected tablet API %v", req.APIKey())
				}
				if bucket != 0 || routingCount != count || routedPartition != max(0, partitionID) {
					return nil, fmt.Errorf("tablet received bucket=%d count=%d partition=%d", bucket, routingCount, routedPartition)
				}
				return response, nil
			})
			lookup, err := client.NewLookuper(ctx, table, lookupOptions...)
			if err != nil {
				t.Fatal(err)
			}
			defer lookup.Close()
			writer, err := client.NewUpsertWriter(ctx, table, writerOptions...)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close(ctx)
			missing := tenantForBucket(t, table.Schema, count, count-1)
			available := tenantForBucket(t, table.Schema, count, 0)
			if got := lookup.Lookup(ctx, PrimaryKey{missing, int32(1)})[0]; !errors.Is(got.Err, ErrNoBucketLeader) || got.Bucket != count-1 {
				t.Fatalf("point lookup on unavailable bucket: %+v", got)
			}
			if got := lookup.PrefixLookup(ctx, PrimaryKey{missing})[0]; !errors.Is(got.Err, ErrNoBucketLeader) || got.Bucket != count-1 {
				t.Fatalf("prefix lookup on unavailable bucket: %+v", got)
			}
			if got := writer.Upsert(ctx, Row{missing, int32(1), "value"}).Await(ctx); !errors.Is(got.Err, ErrNoBucketLeader) || got.Bucket != count-1 {
				t.Fatalf("upsert on unavailable bucket: %+v", got)
			}
			if got := lookup.Lookup(ctx, PrimaryKey{available, int32(1)})[0]; !errors.Is(got.Err, ErrNotFound) || got.Bucket != 0 {
				t.Fatalf("point lookup on available bucket: %+v", got)
			}
			if got := lookup.PrefixLookup(ctx, PrimaryKey{available})[0]; got.Err != nil || got.Bucket != 0 {
				t.Fatalf("prefix lookup on available bucket: %+v", got)
			}
			if got := writer.Upsert(ctx, Row{available, int32(1), "value"}).Await(ctx); got.Err != nil || got.Bucket != 0 {
				t.Fatalf("upsert on available bucket: %+v", got)
			}
		})
	}
}

func TestKeyedRequestRetainsOriginalBucketCountOnReroute(t *testing.T) {
	path := appendWriterTable().Path
	metadata := metadataResponse(path)
	metadata.TableMetadata[0].BucketMetadata = append(metadata.TableMetadata[0].BucketMetadata, &fmsg.PbBucketMetadata{BucketId: proto.Int32(1)})
	var counts []int32
	client := newBucketRoutingTestClient(t, metadata, func(_ context.Context, req fmsg.Request) (fmsg.Response, error) {
		input := req.(*fmsg.MessageRequest).Message().(*fmsg.LookupRequest)
		counts = append(counts, input.GetBucketsReq()[0].GetRoutingBucketCount())
		return nil, ErrMetadata
	})
	backend := clientLookupBackend{client: client}
	_, err := backend.lookup(context.Background(), lookupRequest{
		path: PhysicalTablePath{TablePath: path}, tableID: appendWriterTable().ID, bucketCount: 1, partitionID: -1,
	})
	if !errors.Is(err, ErrMetadata) || len(counts) != 2 || counts[0] != 1 || counts[1] != 1 {
		t.Fatalf("keyed request count on original and refreshed route = %v, error = %v", counts, err)
	}
}

func newBucketRoutingTestClient(t *testing.T, metadata *fmsg.MetadataResponse, tabletHandler requesterFunc) *Client {
	t.Helper()
	client := routedWriterClient(t, func(_ context.Context, req fmsg.Request) (fmsg.Response, error) {
		response, err := fmsg.NewResponse(req.APIKey(), req.Version())
		if err == nil {
			proto.Merge(response.Message(), metadata)
		}
		return response, err
	}, tabletHandler)
	t.Cleanup(func() { _ = client.Close() })
	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", serverType: TabletServer}]
	tablet.versions[fmsg.APIKeyLookup] = 1
	tablet.versions[fmsg.APIKeyPrefixLookup] = 1
	tablet.versions[fmsg.APIKeyPutKv] = 3
	return client
}

func tenantForBucket(t *testing.T, schema Schema, count, want int32) string {
	t.Helper()
	for i := range 100 {
		tenant := strconv.Itoa(i)
		encoded, err := encodeKeyColumns(schema, []string{"tenant"}, PrimaryKey{tenant})
		if err != nil {
			t.Fatal(err)
		}
		if bucket, err := flussBucket(encoded, int(count)); err == nil && bucket == want {
			return tenant
		}
	}
	t.Fatalf("could not find fixture key for bucket %d of %d", want, count)
	return ""
}
