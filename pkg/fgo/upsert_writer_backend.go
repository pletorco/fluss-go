package fgo

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type upsertWriterBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]ServerNode, error)
	initWriter(context.Context, PhysicalTablePath, int32) (int64, error)
	put(context.Context, kvPutRequest) (kvPutResult, error)
}

type kvPutResult struct {
	logEnd        int64
	pressure      float32
	pressureKnown bool
}

type clientUpsertWriterBackend struct{ client *Client }

func (b clientUpsertWriterBackend) ensurePartition(
	ctx context.Context,
	path PhysicalTablePath,
	partitionKeys []string,
) error {
	return b.client.ensureDynamicPartition(ctx, path, partitionKeys)
}

type kvPutRequest struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	targets     []int32
	records     []byte
	timeout     time.Duration
	acks        int32
	mergeMode   MergeMode
}

func (b clientUpsertWriterBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]ServerNode, error) {
	return (clientAppendWriterBackend{client: b.client}).metadata(ctx, path)
}

func (b clientUpsertWriterBackend) initWriter(ctx context.Context, path PhysicalTablePath, bucket int32) (int64, error) {
	return (clientAppendWriterBackend{client: b.client}).initWriter(ctx, path, bucket)
}

func (b clientUpsertWriterBackend) put(
	ctx context.Context,
	input kvPutRequest,
) (kvPutResult, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyPutKv, 0)
	if err != nil {
		return kvPutResult{}, err
	}
	message := request.Message().(*fmsg.PutKvRequest)
	message.Acks = proto.Int32(input.acks)
	message.TableId = proto.Int64(input.tableID)
	message.TimeoutMs = proto.Int32(int32(input.timeout / time.Millisecond))
	message.TargetColumns = append([]int32(nil), input.targets...)
	message.AggMode = proto.Int32(int32(input.mergeMode))
	bucketRequest := &fmsg.PbPutKvReqForBucket{BucketId: proto.Int32(input.bucket), Records: input.records}
	if input.partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(input.partitionID)
	}
	message.BucketsReq = []*fmsg.PbPutKvReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, input.path, input.bucket, request)
	if err != nil {
		return kvPutResult{}, err
	}
	put, ok := response.Message().(*fmsg.PutKvResponse)
	if !ok {
		return kvPutResult{}, fmt.Errorf("fgo: put KV: unexpected response %T", response.Message())
	}
	if len(put.GetBucketsResp()) != 1 || put.GetBucketsResp()[0].GetBucketId() != input.bucket {
		return kvPutResult{}, fmt.Errorf("%w: put KV response omitted bucket %d", ErrValidation, input.bucket)
	}
	result := put.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyPutKv); err != nil {
		b.client.invalidatePhysicalOnMetadataError(input.path, err)
		return kvPutResult{}, err
	}
	pressure := result.GetPressure()
	if result.Pressure != nil && (math.IsNaN(float64(pressure)) || pressure < 0 || pressure >= 1) {
		return kvPutResult{}, fmt.Errorf("%w: invalid KV pressure %v", ErrValidation, pressure)
	}
	return kvPutResult{
		logEnd: result.GetLogEndOffset(), pressure: pressure, pressureKnown: result.Pressure != nil,
	}, nil
}
