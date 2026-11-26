package fgo

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestRequestBucketReroutesStaleListOffsetsReply(t *testing.T) {
	for _, test := range []struct {
		name            string
		requestBuckets  []int32
		firstResults    []*fmsg.PbListOffsetsRespForBucket
		secondError     fmsg.ErrorCode
		wantReroute     bool
		wantInvalidated bool
	}{
		{name: "new leader", requestBuckets: []int32{0}, firstResults: offsetReplies(0, fmsg.ErrorCodeNotLeaderOrFollower), wantReroute: true},
		{name: "second stale reply", requestBuckets: []int32{0}, firstResults: offsetReplies(0, fmsg.ErrorCodeNotLeaderOrFollower),
			secondError: fmsg.ErrorCodeNotLeaderOrFollower, wantReroute: true, wantInvalidated: true},
		{name: "different bucket", requestBuckets: []int32{0}, firstResults: offsetReplies(1, fmsg.ErrorCodeNotLeaderOrFollower)},
		{name: "multiple buckets", requestBuckets: []int32{0, 1}, firstResults: offsetReplies(0, fmsg.ErrorCodeNotLeaderOrFollower)},
		{name: "storage error", requestBuckets: []int32{0}, firstResults: offsetReplies(0, fmsg.ErrorCodeStorageException)},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			path := PhysicalTablePath{TablePath: appendWriterTable().Path, Partition: "kr"}
			old := ServerNode{ID: 2, Address: "old:9123", ServerType: TabletServer}
			current := ServerNode{ID: 3, Address: "new:9123", ServerType: TabletServer}
			var fetches atomic.Int32
			router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
				return TableMetadata{Path: path.TablePath}, nil
			}).WithPhysicalMetadataFetcher(func(context.Context, PhysicalTablePath) (PartitionMetadata, error) {
				leader := current
				if fetches.Add(1) == 1 {
					leader = old
				}
				return PartitionMetadata{Path: path, BucketCount: 1, Buckets: map[int32]ServerNode{0: leader}}, nil
			})
			client := listOffsetsRoutingClient(t, router, map[ServerNode][]*fmsg.PbListOffsetsRespForBucket{
				old: test.firstResults, current: offsetReplies(0, test.secondError),
			})
			request, err := fmsg.NewRequest(fmsg.APIKeyListOffsets, 0)
			if err != nil {
				t.Fatal(err)
			}
			request.Message().(*fmsg.ListOffsetsRequest).BucketId = test.requestBuckets
			response, err := client.RequestBucket(ctx, path, 0, request)
			if err != nil {
				t.Fatal(err)
			}
			wantResponses, wantLeader, wantFetches := test.firstResults, old, int32(1)
			if test.wantReroute {
				wantResponses, wantLeader, wantFetches = offsetReplies(0, test.secondError), current, 2
			}
			if test.wantInvalidated {
				wantLeader = ServerNode{}
			}
			want := &fmsg.ListOffsetsResponse{BucketsResp: wantResponses}
			if fetches.Load() != wantFetches || !proto.Equal(response.Message(), want) {
				t.Fatalf("ListOffsets returned %v with %d fetches, want %v with %d fetches", response.Message(), fetches.Load(), want, wantFetches)
			}
			if leader, _, err := router.lookupPhysical(path, 0); err != nil || leader != wantLeader {
				t.Fatalf("cached leader = %v, %v; want %v", leader, err, wantLeader)
			}
		})
	}
}

func offsetReplies(bucket int32, code fmsg.ErrorCode) []*fmsg.PbListOffsetsRespForBucket {
	return []*fmsg.PbListOffsetsRespForBucket{{BucketId: proto.Int32(bucket), ErrorCode: proto.Int32(int32(code))}}
}

func listOffsetsRoutingClient(t *testing.T, router *Router, replies map[ServerNode][]*fmsg.PbListOffsetsRespForBucket) *Client {
	t.Helper()
	client := newClient(nil, nil)
	client.router = router
	client.manager = newConnectionManager(config{})
	for node, results := range replies {
		tablet := newClient(requesterFunc(func(_ context.Context, req fmsg.Request) (fmsg.Response, error) {
			response, err := fmsg.NewResponse(req.APIKey(), req.Version())
			if err != nil {
				return nil, err
			}
			response.Message().(*fmsg.ListOffsetsResponse).BucketsResp = results
			return response, nil
		}), nil)
		tablet.versions[fmsg.APIKeyListOffsets] = 0
		client.manager.clients[connectionKey{id: node.ID, address: node.Address, serverType: node.ServerType}] = tablet
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
