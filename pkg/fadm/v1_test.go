package fadm

import (
	"context"
	"errors"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestFluss100AdministrationAPIs(t *testing.T) {
	comment, value := "updated", "team"
	seen := make(map[fmsg.APIKey]bool)
	client := newClient(&fakeRequester{coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		seen[request.APIKey()] = true
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		switch message := request.(*fmsg.MessageRequest).Message().(type) {
		case *fmsg.AlterDatabaseRequest:
			if message.GetDatabaseName() != "db" || !message.GetIgnoreIfNotExists() ||
				message.GetComment() != comment || len(message.GetConfigChanges()) != 1 {
				t.Fatalf("AlterDatabase request = %#v", message)
			}
		case *fmsg.GetClusterHealthRequest:
			health := response.Message().(*fmsg.GetClusterHealthResponse)
			health.NumReplicas, health.InSyncReplicas = proto.Int32(6), proto.Int32(5)
			health.NumLeaderReplicas, health.ActiveLeaderReplicas = proto.Int32(3), proto.Int32(3)
			health.Status = proto.Int32(int32(ClusterHealthYellow))
		case *fmsg.ListRemoteLogManifestsRequest:
			if message.GetTableId() != 9 || message.GetPartitionId() != 10 {
				t.Fatalf("ListRemoteLogManifests request = %#v", message)
			}
			response.Message().(*fmsg.ListRemoteLogManifestsResponse).Manifests = []*fmsg.PbRemoteLogManifestEntry{{
				TableBucket:           &fmsg.PbTableBucket{TableId: proto.Int64(9), PartitionId: proto.Int64(10), BucketId: proto.Int32(2)},
				RemoteLogManifestPath: proto.String("s3://bucket/manifest"), RemoteLogEndOffset: proto.Int64(42),
			}}
		case *fmsg.ListKvSnapshotsRequest:
			if message.GetTableId() != 9 || message.PartitionId != nil {
				t.Fatalf("ListKvSnapshots request = %#v", message)
			}
			listed := response.Message().(*fmsg.ListKvSnapshotsResponse)
			listed.TableId = proto.Int64(9)
			listed.ActiveSnapshots = []*fmsg.PbKvSnapshot{{
				BucketId: proto.Int32(2), SnapshotId: proto.Int64(7), LogOffset: proto.Int64(41),
			}}
		default:
			t.Fatalf("unexpected request %T", message)
		}
		return response, nil
	}})

	if err := client.AlterDatabase(context.Background(), "db", AlterDatabase{
		Config: []AlterConfig{{Key: "owner", Value: &value, Op: ConfigSet}}, Comment: &comment,
	}, true); err != nil {
		t.Fatal(err)
	}
	health, err := client.GetClusterHealth(context.Background())
	if err != nil || health.Replicas != 6 || health.InSyncReplicas != 5 || health.Status != ClusterHealthYellow {
		t.Fatalf("GetClusterHealth() = %#v, %v", health, err)
	}
	manifests, err := client.ListRemoteLogManifests(context.Background(), 9, 10)
	if err != nil || len(manifests) != 1 || manifests[0].Bucket != 2 || manifests[0].LogEndOffset != 42 {
		t.Fatalf("ListRemoteLogManifests() = %#v, %v", manifests, err)
	}
	snapshots, err := client.ListKVSnapshots(context.Background(), 9, -1)
	if err != nil || len(snapshots.Snapshots) != 1 || snapshots.Snapshots[0].SnapshotID != 7 {
		t.Fatalf("ListKVSnapshots() = %#v, %v", snapshots, err)
	}
	if len(seen) != 4 {
		t.Fatalf("seen APIs = %#v", seen)
	}
}

func TestFluss100AdministrationValidation(t *testing.T) {
	client := newClient(&fakeRequester{coordinator: func(context.Context, fmsg.Request) (fmsg.Response, error) {
		return nil, errors.New("unexpected request")
	}})
	checks := []error{
		client.AlterDatabase(context.Background(), "", AlterDatabase{}, false),
		client.AlterDatabase(context.Background(), "db", AlterDatabase{}, false),
		client.AlterDatabase(context.Background(), "db", AlterDatabase{Config: []AlterConfig{{Key: "x", Op: ConfigSet}}}, false),
	}
	_, manifestErr := client.ListRemoteLogManifests(context.Background(), -1, -1)
	checks = append(checks, manifestErr)
	_, snapshotErr := client.ListKVSnapshots(context.Background(), 1, -2)
	checks = append(checks, snapshotErr)
	for index, err := range checks {
		if !errors.Is(err, fgo.ErrInvalidConfig) {
			t.Fatalf("validation %d error = %v", index, err)
		}
	}
}

func TestAlterTableBucketCount(t *testing.T) {
	count := int32(8)
	client := newClient(&fakeRequester{coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		message := request.(*fmsg.MessageRequest).Message().(*fmsg.AlterTableRequest)
		if message.GetModifyBucketCount().GetNewBucketCount() != count {
			t.Fatalf("AlterTable request = %#v", message)
		}
		return fmsg.NewResponse(request.APIKey(), request.Version())
	}})
	if err := client.AlterTable(
		context.Background(), fgo.TablePath{Database: "db", Table: "events"},
		AlterTable{BucketCount: &count}, false,
	); err != nil {
		t.Fatal(err)
	}
	invalid := int32(0)
	if err := client.AlterTable(
		context.Background(), fgo.TablePath{Database: "db", Table: "events"},
		AlterTable{BucketCount: &invalid}, false,
	); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("invalid bucket count error = %v", err)
	}
}
