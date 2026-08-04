package fadm

import (
	"context"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestLakeSnapshotMatchesJava091Contract(t *testing.T) {
	path := fgo.TablePath{Database: "db", Table: "lake_users"}
	requester := &fakeRequester{coordinator: func(
		_ context.Context,
		request fmsg.Request,
	) (fmsg.Response, error) {
		return lakeSnapshotContractResponse(t, path, request)
	}}

	snapshot, err := newClient(requester).GetReadableLakeSnapshot(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	assertLakeSnapshotContract(t, snapshot)
}

func lakeSnapshotContractResponse(
	t *testing.T,
	path fgo.TablePath,
	request fmsg.Request,
) (fmsg.Response, error) {
	t.Helper()
	if request.APIKey() != fmsg.APIKeyGetLakeSnapshot {
		t.Fatalf("API key = %d", request.APIKey())
	}
	messageRequest, ok := request.(*fmsg.MessageRequest)
	if !ok {
		t.Fatalf("lake snapshot request type = %T", request)
	}
	message := messageRequest.Message().(*fmsg.GetLakeSnapshotRequest)
	if message.GetTablePath().GetDatabaseName() != path.Database ||
		message.GetTablePath().GetTableName() != path.Table ||
		message.SnapshotId != nil || !message.GetReadable() {
		t.Fatalf("lake snapshot request = %#v", message)
	}
	response, err := fmsg.NewResponse(request.APIKey(), request.Version())
	if err != nil {
		return nil, err
	}
	result := response.Message().(*fmsg.GetLakeSnapshotResponse)
	result.TableId = proto.Int64(91)
	result.SnapshotId = proto.Int64(901)
	result.BucketSnapshots = []*fmsg.PbLakeSnapshotForBucket{
		{
			PartitionId: proto.Int64(7), PartitionName: proto.String("region=kr"),
			BucketId: proto.Int32(0), LogOffset: proto.Int64(101),
		},
		{BucketId: proto.Int32(1), LogOffset: proto.Int64(202)},
	}
	return response, nil
}

func assertLakeSnapshotContract(t *testing.T, snapshot LakeSnapshot) {
	t.Helper()
	if snapshot.TableID != 91 || snapshot.SnapshotID != 901 || len(snapshot.Buckets) != 2 {
		t.Fatalf("lake snapshot = %#v", snapshot)
	}
	partitioned, unpartitioned := snapshot.Buckets[0], snapshot.Buckets[1]
	if partitioned.PartitionID != 7 || partitioned.PartitionName != "region=kr" ||
		partitioned.Bucket != 0 || partitioned.LogOffset != 101 {
		t.Fatalf("partitioned bucket = %#v", partitioned)
	}
	if unpartitioned.PartitionID != -1 || unpartitioned.PartitionName != "" ||
		unpartitioned.Bucket != 1 || unpartitioned.LogOffset != 202 {
		t.Fatalf("unpartitioned bucket = %#v", unpartitioned)
	}
}
