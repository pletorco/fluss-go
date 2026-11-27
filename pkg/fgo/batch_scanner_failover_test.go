package fgo

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestLogBatchScanRefreshPreservesPhysicalIdentity(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		tableID, partitionID int64
		count                int32
		wantErr              bool
	}{
		{"same partition", 9, 10, 3, false},
		{"recreated table", 99, 10, 3, true},
		{"recreated partition", 9, 11, 3, true},
		{"rescaled partition", 9, 10, 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := appendWriterTable()
			table.ID = 9
			table.Schema.PartitionKey = []string{"day"}
			metadata := metadataResponse(table.Path)
			metadata.PartitionMetadata[0].PartitionName = proto.String("2026-07-30")
			metadata.PartitionMetadata[0].TableId = proto.Int64(tc.tableID)
			metadata.PartitionMetadata[0].PartitionId = proto.Int64(tc.partitionID)
			metadata.PartitionMetadata[0].BucketCount = proto.Int32(tc.count)
			client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
				response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
				switch message := request.(*fmsg.MessageRequest).Message().(type) {
				case *fmsg.ListPartitionInfosRequest:
					if !message.GetIncludeSystemPartitions() {
						t.Fatal("system partitions must be resolvable by ID")
					}
					response.Message().(*fmsg.ListPartitionInfosResponse).PartitionsInfo = []*fmsg.PbPartitionInfo{{
						PartitionId: proto.Int64(10), PartitionSpec: &fmsg.PbPartitionSpec{
							PartitionKeyValues: []*fmsg.PbKeyValue{{Key: proto.String("day"), Value: proto.String("2026-07-30")}},
						},
					}}
				case *fmsg.MetadataRequest:
					if got := message.GetPartitionsPath()[0].GetPartitionName(); got != "2026-07-30" {
						t.Fatalf("requested partition %q", got)
					}
					proto.Merge(response.Message(), metadata)
				default:
					t.Fatalf("unexpected request %T", message)
				}
				return response, nil
			}), nil)
			client.versions[fmsg.APIKeyGetMetadata], client.versions[fmsg.APIKeyListPartitionInfos] = 0, 0
			bucket := TableBucket{TableID: 9, PartitionID: 10, BucketID: 1, BucketCount: 3}
			rerouted, err := (clientBatchScanBackend{client}).refreshLogScanBucket(t.Context(), table, bucket)
			if tc.wantErr {
				if !errors.Is(err, ErrMetadata) {
					t.Fatalf("changed identity: %v", err)
				}
			} else if err != nil || rerouted.Leader.ID != 2 || rerouted.TableID != 9 ||
				rerouted.PartitionID != 10 || rerouted.BucketID != 1 || rerouted.BucketCount != 3 {
				t.Fatalf("refreshed route = %+v, %v", rerouted, err)
			}
		})
	}
}

func TestLogBatchScanRetriesOnceAfterStaleLeader(t *testing.T) {
	table := appendWriterTable()
	table.ID = 9
	bucket := testTableBucket(table)
	bucket.BucketCount = 1
	bucket.Leader.Address = "old:9123"
	newLeader := ServerNode{ID: 2, Address: "tablet:9123", ServerType: TabletServer}
	for _, oldErr := range []error{io.EOF, ErrMetadata, ErrAuthorization} {
		for _, refreshedID := range []int64{9, 99} {
			metadata := metadataResponse(table.Path)
			metadata.TableMetadata[0].TableId = proto.Int64(refreshedID)
			metaCalls, oldCalls, newCalls := 0, 0, 0
			coordinator := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
				metaCalls++
				response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
				proto.Merge(response.Message(), metadata)
				return response, nil
			}), nil)
			coordinator.versions[fmsg.APIKeyGetMetadata] = 0
			client := batchBackendClient(bucket.Leader, func(context.Context, fmsg.Request) (fmsg.Response, error) {
				oldCalls++
				return nil, oldErr
			})
			client.address, client.serverType = "coordinator:9123", Coordinator
			client.manager.clients[connectionKey{address: client.address, serverType: Coordinator}] = coordinator
			fresh := batchBackendClient(newLeader, func(_ context.Context, req fmsg.Request) (fmsg.Response, error) {
				newCalls++
				message := req.(*fmsg.MessageRequest).Message().(*fmsg.LimitScanRequest)
				if message.GetTableId() != bucket.TableID || message.GetBucketId() != bucket.BucketID ||
					message.GetRoutingBucketCount() != 1 || message.GetLimit() != 2 {
					t.Fatalf("changed retry request: %v", message)
				}
				return nil, ErrStorage // A second failure must be returned as-is.
			})
			for key, node := range fresh.manager.clients {
				client.manager.clients[key] = node
			}
			_, _, err := (clientBatchScanBackend{client}).limitLogScan(t.Context(), table, bucket, 2)
			switch {
			case oldErr == ErrAuthorization:
				if !errors.Is(err, oldErr) || metaCalls != 0 || newCalls != 0 {
					t.Fatalf("retried an authorization failure: %v (%d/%d)", err, metaCalls, newCalls)
				}
			case refreshedID != table.ID:
				if !errors.Is(err, ErrMetadata) || !errors.Is(err, oldErr) || metaCalls != 1 || newCalls != 0 {
					t.Fatalf("rerouted to a recreated table: %v (%d/%d)", err, metaCalls, newCalls)
				}
			default:
				if !errors.Is(err, ErrStorage) || metaCalls != 1 || newCalls != 1 {
					t.Fatalf("retry error = %v (%d/%d)", err, metaCalls, newCalls)
				}
			}
			if oldCalls != 1 {
				t.Fatalf("old leader called %d times", oldCalls)
			}
		}
	}
}
