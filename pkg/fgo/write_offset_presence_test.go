package fgo

import (
	"context"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestPublicWriterPreservesResponseOffsetPresence(t *testing.T) {
	for _, table := range []Table{appendWriterTable(), upsertWriterTable()} {
		for _, known := range []bool{false, true} {
			label := table.Path.Table + "/absent"
			if known {
				label = table.Path.Table + "/present"
			}
			t.Run(label, func(t *testing.T) {
				client := writerWithOptionalOffset(t, table, known)
				ctx := t.Context()
				var first, second *WriteFuture
				var closeWriter func(context.Context) error
				if table.Kind == LogTable {
					w, err := client.NewAppendWriter(ctx, table, WithAppendBatchTimeout(time.Hour))
					if err != nil {
						t.Fatal(err)
					}
					first, second = w.Append(ctx, Row{int32(1), "one"}), w.Append(ctx, Row{int32(2), "two"})
					closeWriter = w.Close
				} else {
					w, err := client.NewUpsertWriter(ctx, table, WithUpsertBatchTimeout(time.Hour))
					if err != nil {
						t.Fatal(err)
					}
					first, second = w.Upsert(ctx, Row{int32(1), "one", nil}), w.Upsert(ctx, Row{int32(2), "two", nil})
					closeWriter = w.Close
				}
				if err := closeWriter(ctx); err != nil {
					t.Fatal(err)
				}
				for index, future := range []*WriteFuture{first, second} {
					result := future.Await(ctx)
					want := int64(0)
					if known {
						want = int64(index)
					}
					if result.Err != nil || result.OffsetKnown != known || result.BaseOffset != want || result.Records != 1 {
						t.Errorf("record %d = %+v; want known=%v offset=%d", index, result, known, want)
					}
				}
			})
		}
	}
}

func writerWithOptionalOffset(t *testing.T, table Table, known bool) *Client {
	t.Helper()
	client := routedWriterClient(t, func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		metadata := metadataResponse(table.Path)
		metadata.TableMetadata[0].TableId = proto.Int64(table.ID)
		proto.Merge(response.Message().(*fmsg.MetadataResponse), metadata)
		return response, nil
	}, func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		switch message := response.Message().(type) {
		case *fmsg.InitWriterResponse:
			message.WriterId = proto.Int64(1)
		case *fmsg.ProduceLogResponse:
			item := &fmsg.PbProduceLogRespForBucket{BucketId: proto.Int32(0)}
			if known {
				item.BaseOffset = proto.Int64(0)
			}
			message.BucketsResp = []*fmsg.PbProduceLogRespForBucket{item}
		case *fmsg.PutKvResponse:
			item := &fmsg.PbPutKvRespForBucket{BucketId: proto.Int32(0)}
			if known {
				item.LogEndOffset = proto.Int64(2)
			}
			message.BucketsResp = []*fmsg.PbPutKvRespForBucket{item}
		}
		return response, nil
	})
	client.manager.clients[connectionKey{id: 2, address: "tablet:9123", serverType: TabletServer}].versions[fmsg.APIKeyPutKv] = 0
	return client
}
