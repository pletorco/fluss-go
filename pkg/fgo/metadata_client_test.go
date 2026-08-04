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
	if table.ID != 9 || table.SchemaID != 3 || table.Buckets[0].Address != "tablet:9123" {
		t.Fatalf("table metadata = %#v", table)
	}
	partition, ok := table.Partitions[physicalTableKey(PhysicalTablePath{TablePath: path, Partition: "day=2026-07-30"})]
	if !ok || partition.ID != 10 || partition.Buckets[1].ID != 2 {
		t.Fatalf("partition metadata = %#v", table.Partitions)
	}

	got, err := partitionMetadataFromResponse(response, PhysicalTablePath{TablePath: path, Partition: "day=2026-07-30"})
	if err != nil || got.ID != 10 {
		t.Fatalf("partitionMetadataFromResponse() = %#v, %v", got, err)
	}
}

func TestMetadataResponseConversionFailures(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	response := metadataResponse(path)
	response.TableMetadata[0].BucketMetadata[0].LeaderId = nil
	if _, err := tableMetadataFromResponse(response, path); !errors.Is(err, ErrNoBucketLeader) {
		t.Fatalf("missing leader error = %v", err)
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

func TestApplyPartitionServersCopiesAvailableNodes(t *testing.T) {
	coordinator := ServerNode{ID: 1, Address: "coordinator:9123", ServerType: Coordinator}
	tablets := map[int32]ServerNode{2: {ID: 2, Address: "tablet:9123", ServerType: TabletServer}}
	metadata := &Metadata{}
	applyPartitionServers(metadata, PartitionMetadata{
		coordinator: coordinator,
		tablets:     tablets,
	})
	tablets[2] = ServerNode{}
	if metadata.Coordinator != coordinator || metadata.Tablets[2].Address != "tablet:9123" {
		t.Fatalf("partition servers = %#v", metadata)
	}
	unchanged := *metadata
	applyPartitionServers(metadata, PartitionMetadata{})
	if metadata.Coordinator != unchanged.Coordinator || metadata.Tablets[2] != unchanged.Tablets[2] {
		t.Fatalf("empty partition changed metadata = %#v", metadata)
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
		table.Properties["table.merge-engine"] != "aggregation" {
		t.Fatalf("GetTable() = %#v, %v", table, err)
	}
}

func metadataResponse(path TablePath) *fmsg.MetadataResponse {
	return &fmsg.MetadataResponse{
		CoordinatorServer: serverNode(1, "coordinator", 9123),
		TabletServers:     []*fmsg.PbServerNode{serverNode(2, "tablet", 9123)},
		TableMetadata: []*fmsg.PbTableMetadata{{
			TablePath: pbTablePath(path), TableId: proto.Int64(9), SchemaId: proto.Int32(3),
			BucketMetadata: []*fmsg.PbBucketMetadata{{BucketId: proto.Int32(0), LeaderId: proto.Int32(2)}},
		}},
		PartitionMetadata: []*fmsg.PbPartitionMetadata{{
			TableId: proto.Int64(9), PartitionName: proto.String("day=2026-07-30"), PartitionId: proto.Int64(10),
			BucketMetadata: []*fmsg.PbBucketMetadata{{BucketId: proto.Int32(1), LeaderId: proto.Int32(2)}},
		}},
	}
}

func serverNode(id int32, host string, port int32) *fmsg.PbServerNode {
	return &fmsg.PbServerNode{NodeId: proto.Int32(id), Host: proto.String(host), Port: proto.Int32(port)}
}
