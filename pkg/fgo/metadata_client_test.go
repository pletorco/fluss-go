package fgo

import (
	"context"
	"errors"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestMetadataResponseConversion(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	response := metadataResponse(path)
	table, err := tableMetadataFromResponse(response, path)
	if err != nil {
		t.Fatal(err)
	}
	if table.ID != 9 || table.SchemaID != 3 || table.Buckets[0].Address != "tablet:9123" ||
		table.BucketCount != 1 || !table.BucketCountEpochKnown || table.BucketCountEpoch != 8 ||
		table.RemoteDataDirectory != "s3://bucket/table" {
		t.Fatalf("table metadata = %#v", table)
	}
	detail := table.BucketDetails[0]
	if detail.Leader == nil || detail.Leader.ID != 2 || !detail.LeaderEpochKnown || detail.LeaderEpoch != 4 ||
		!detail.BucketEpochKnown || detail.BucketEpoch != 5 || len(detail.Replicas) != 2 || len(detail.ISR) != 1 {
		t.Fatalf("bucket metadata = %#v", detail)
	}
	partition, ok := table.Partitions[physicalTableKey(PhysicalTablePath{TablePath: path, Partition: "day=2026-07-30"})]
	if !ok || partition.ID != 10 || !partition.parentTableIDKnown || partition.parentTableID != table.ID ||
		partition.Buckets[1].ID != 2 || partition.BucketCount != 3 {
		t.Fatalf("partition metadata = %#v", table.Partitions)
	}

	got, err := partitionMetadataFromResponse(response, PhysicalTablePath{TablePath: path, Partition: "day=2026-07-30"})
	if err != nil || got.ID != 10 || !got.parentTableIDKnown || got.parentTableID != table.ID {
		t.Fatalf("partitionMetadataFromResponse() = %#v, %v", got, err)
	}
}

func TestMetadataRejectsMissingOrInvalidPartitionIdentity(t *testing.T) {
	path := PhysicalTablePath{TablePath: appendWriterTable().Path, Partition: "day=2026-07-30"}
	for _, test := range []struct {
		name                 string
		tableID, partitionID *int64
	}{
		{"missing parent", nil, proto.Int64(0)},
		{"invalid parent", proto.Int64(-1), proto.Int64(0)},
		{"missing partition", proto.Int64(0), nil},
		{"invalid partition", proto.Int64(0), proto.Int64(-1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := metadataResponse(path.TablePath)
			partition := response.PartitionMetadata[0]
			partition.TableId, partition.PartitionId = test.tableID, test.partitionID
			if _, err := partitionMetadataFromResponse(response, path); !errors.Is(err, ErrMetadata) {
				t.Fatalf("parse physical metadata: %v, want ErrMetadata", err)
			}
		})
	}
}

func TestMetadataResponseConversionFailures(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	response := metadataResponse(path)
	response.TableMetadata[0].BucketMetadata[0].LeaderId = nil
	metadata, err := tableMetadataFromResponse(response, path)
	if err != nil || metadata.BucketDetails[0].Leader != nil {
		t.Fatalf("missing leader metadata = %#v, %v", metadata, err)
	}

	response = metadataResponse(path)
	response.TabletServers[0].Port = proto.Int32(0)
	if _, err := tableMetadataFromResponse(response, path); !errors.Is(err, ErrMetadata) {
		t.Fatalf("bad tablet error = %v", err)
	}
	if _, err := tableMetadataFromResponse(metadataResponse(path), TablePath{Database: "db", Table: "missing"}); !errors.Is(err, ErrUnknownTable) {
		t.Fatalf("unknown table error = %v", err)
	}
	if _, err := partitionMetadataFromResponse(metadataResponse(path), PhysicalTablePath{TablePath: path, Partition: "missing"}); !errors.Is(err, ErrUnknownPartition) {
		t.Fatalf("unknown partition error = %v", err)
	}
}

func TestRouterOwnsFetchedPartitionAndServers(t *testing.T) {
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}, Partition: "day=1"}
	coordinator := ServerNode{ID: 1, Address: "coordinator:9123", ServerType: Coordinator}
	node := ServerNode{ID: 2, Address: "tablet:9123", ServerType: TabletServer}
	leader := node
	partition := PartitionMetadata{
		Path: path, BucketCount: 1,
		Buckets:       map[int32]ServerNode{0: node},
		BucketDetails: map[int32]BucketMetadata{0: {Leader: &leader, Replicas: []int32{2, 3}, ISR: []int32{2}}},
		coordinator:   coordinator,
		tablets:       map[int32]ServerNode{2: node},
	}
	router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
		return TableMetadata{Path: path.TablePath}, nil
	}).WithPhysicalMetadataFetcher(func(context.Context, PhysicalTablePath) (PartitionMetadata, error) {
		return partition, nil
	})
	if err := router.RefreshPhysical(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	partition.tablets[2] = ServerNode{}
	delete(partition.Buckets, 0)
	detail := partition.BucketDetails[0]
	detail.Leader.Address = "changed:9123"
	detail.Replicas[0], detail.ISR[0] = 9, 9
	delete(partition.BucketDetails, 0)
	if got, count, err := router.lookupPhysical(path, 0); err != nil || got != node || count != 1 {
		t.Fatalf("cached route = %v, %d, %v", got, count, err)
	}
	router.mu.RLock()
	cached := router.metadata.Tables[path.TablePath].Partitions[physicalTableKey(path)]
	owned, ownedTablet := cached.BucketDetails[0], cached.tablets[2]
	router.mu.RUnlock()
	if owned.Leader == nil || *owned.Leader != node || owned.Replicas[0] != 2 || owned.ISR[0] != 2 || ownedTablet != node {
		t.Fatalf("cached partition aliased its fetch response: %#v, %#v", owned, ownedTablet)
	}
	partition.coordinator = ServerNode{}
	partition.tablets = nil
	if err := router.RefreshPhysical(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	router.mu.RLock()
	gotCoordinator, gotTablet := router.metadata.Coordinator, router.metadata.Tablets[2]
	router.mu.RUnlock()
	if gotCoordinator != coordinator || gotTablet != node {
		t.Fatalf("empty partition server metadata changed defaults = %v, %v", gotCoordinator, gotTablet)
	}
}

func TestClientFetchesMetadataThroughCoordinator(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	calls := 0
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		calls++
		message := request.(*fmsg.MessageRequest).Message().(*fmsg.MetadataRequest)
		if request.APIKey() != fmsg.APIKeyGetMetadata || message.GetTablePath()[0].GetTableName() != "events" {
			t.Fatalf("metadata request = %#v", message)
		}
		response, _ := fmsg.NewResponse(fmsg.APIKeyGetMetadata, request.Version())
		*response.Message().(*fmsg.MetadataResponse) = *metadataResponse(path)
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetMetadata] = 0
	table, err := client.fetchTableMetadata(context.Background(), path)
	if err != nil || table.ID != 9 || calls != 1 {
		t.Fatalf("fetchTableMetadata() = %#v, %v (calls %d)", table, err, calls)
	}
}

func TestClientFetchesPhysicalMetadataThroughCoordinator(t *testing.T) {
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}, Partition: "day=2026-07-30"}
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		message := request.(*fmsg.MessageRequest).Message().(*fmsg.MetadataRequest)
		if got := message.GetPartitionsPath()[0].GetPartitionName(); got != path.Partition {
			t.Fatalf("partition name = %q", got)
		}
		response, _ := fmsg.NewResponse(fmsg.APIKeyGetMetadata, request.Version())
		*response.Message().(*fmsg.MetadataResponse) = *metadataResponse(path.TablePath)
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetMetadata] = 0
	partition, err := client.fetchPartitionMetadata(context.Background(), path)
	if err != nil || partition.ID != 10 || partition.Buckets[1].Address != "tablet:9123" {
		t.Fatalf("fetchPartitionMetadata() = %#v, %v", partition, err)
	}
}

func TestGetTableLoadsServerSchema(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		switch message := response.Message().(type) {
		case *fmsg.GetTableInfoResponse:
			if request.(*fmsg.MessageRequest).Message().(*fmsg.GetTableInfoRequest).GetTablePath().GetTableName() != "events" {
				t.Fatal("missing table path")
			}
			message.TableId, message.SchemaId = proto.Int64(9), proto.Int32(3)
			message.TableJson = []byte(`{"bucket_key":["id"],"partition_key":[],"bucket_count":4,"properties":{"table.merge-engine":"aggregation"}}`)
			message.RemoteDataDir, message.BucketCountEpoch = proto.String("s3://bucket/table"), proto.Int64(8)
		case *fmsg.GetTableSchemaResponse:
			if request.(*fmsg.MessageRequest).Message().(*fmsg.GetTableSchemaRequest).GetSchemaId() != 3 {
				t.Fatal("missing schema id")
			}
			message.SchemaId = proto.Int32(3)
			message.SchemaJson = []byte(`{"version":1,"columns":[{"name":"id","data_type":{"type":"BIGINT"},"id":1}],"primary_key":["id"],"highest_field_id":1}`)
		default:
			t.Fatalf("unexpected response %T", message)
		}
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetTableInfo] = 0
	client.versions[fmsg.APIKeyGetTableSchema] = 0
	table, err := client.GetTable(context.Background(), path)
	if err != nil || table.ID != 9 || table.SchemaID != 3 || table.Kind != PrimaryKeyTable ||
		table.BucketCount != 4 || len(table.Schema.BucketKey) != 1 ||
		!table.BucketCountEpochKnown || table.BucketCountEpoch != 8 || table.RemoteDataDirectory != "s3://bucket/table" ||
		table.Properties["table.merge-engine"] != "aggregation" {
		t.Fatalf("GetTable() = %#v, %v", table, err)
	}
}

func TestGetTableRejectsReplacementDuringSchemaFetch(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	var tableID int64 = 9
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		switch message := response.Message().(type) {
		case *fmsg.GetTableInfoResponse:
			message.TableId, message.SchemaId = proto.Int64(tableID), proto.Int32(1)
			message.TableJson = []byte(`{"bucket_count":1}`)
		case *fmsg.GetTableSchemaResponse:
			tableID = 10
			message.SchemaId = proto.Int32(1)
			message.SchemaJson = []byte(`{"version":1,"columns":[{"name":"new_column","data_type":{"type":"BIGINT"},"id":1}],"highest_field_id":1}`)
		}
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetTableInfo], client.versions[fmsg.APIKeyGetTableSchema] = 0, 0
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if _, err := client.GetTable(ctx, path); !errors.Is(err, ErrMetadata) {
		t.Fatalf("GetTable during replacement = %v", err)
	}
	table, err := client.GetTable(ctx, path)
	if err != nil || table.ID != 10 || table.Schema.Columns[0].Name != "new_column" {
		t.Fatalf("GetTable after replacement = %+v, %v", table, err)
	}
}

func metadataResponse(path TablePath) *fmsg.MetadataResponse {
	return &fmsg.MetadataResponse{
		CoordinatorServer: serverNode(1, "coordinator", 9123),
		TabletServers:     []*fmsg.PbServerNode{serverNode(2, "tablet", 9123)},
		TableMetadata: []*fmsg.PbTableMetadata{{
			TablePath: pbTablePath(path), TableId: proto.Int64(9), SchemaId: proto.Int32(3),
			RemoteDataDir: proto.String("s3://bucket/table"), BucketCountEpoch: proto.Int64(8),
			BucketMetadata: []*fmsg.PbBucketMetadata{{
				BucketId: proto.Int32(0), LeaderId: proto.Int32(2), ReplicaId: []int32{2, 3},
				LeaderEpoch: proto.Int32(4), BucketEpoch: proto.Int32(5), Isr: []int32{2},
			}},
		}},
		PartitionMetadata: []*fmsg.PbPartitionMetadata{{
			TableId: proto.Int64(9), PartitionName: proto.String("day=2026-07-30"), PartitionId: proto.Int64(10),
			BucketCount:    proto.Int32(3),
			BucketMetadata: []*fmsg.PbBucketMetadata{{BucketId: proto.Int32(1), LeaderId: proto.Int32(2)}},
		}},
	}
}

func serverNode(id int32, host string, port int32) *fmsg.PbServerNode {
	return &fmsg.PbServerNode{NodeId: proto.Int32(id), Host: proto.String(host), Port: proto.Int32(port)}
}
