package fgo

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type appendWriterBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]ServerNode, error)
	initWriter(context.Context, PhysicalTablePath, int32) (int64, error)
	produce(context.Context, logProduceRequest) (int64, error)
}

type clientAppendWriterBackend struct{ client *Client }

func (b clientAppendWriterBackend) ensurePartition(
	ctx context.Context,
	path PhysicalTablePath,
	partitionKeys []string,
) error {
	return b.client.ensureDynamicPartition(ctx, path, partitionKeys)
}

type logProduceRequest struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	records     []byte
	timeout     time.Duration
	acks        int32
}

func (b clientAppendWriterBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]ServerNode, error) {
	if path.Partition == "" {
		table, err := b.client.fetchTableMetadata(ctx, path.TablePath)
		return table.ID, table.Buckets, err
	}
	partition, err := b.client.fetchPartitionMetadata(ctx, path)
	return partition.ID, partition.Buckets, err
}

func (b clientAppendWriterBackend) initWriter(ctx context.Context, path PhysicalTablePath, bucket int32) (int64, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyInitWriter, 0)
	if err != nil {
		return 0, err
	}
	request.Message().(*fmsg.InitWriterRequest).TablePath = []*fmsg.PbTablePath{pbTablePath(path.TablePath)}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return 0, err
	}
	message, ok := response.Message().(*fmsg.InitWriterResponse)
	if !ok {
		return 0, fmt.Errorf("fgo: init writer: unexpected response %T", response.Message())
	}
	return message.GetWriterId(), nil
}

func (b clientAppendWriterBackend) produce(
	ctx context.Context,
	input logProduceRequest,
) (int64, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyProduceLog, 0)
	if err != nil {
		return 0, err
	}
	if input.timeout/time.Millisecond > math.MaxInt32 {
		return 0, fmt.Errorf("%w: log timeout exceeds protocol range", ErrInvalidConfig)
	}
	message := request.Message().(*fmsg.ProduceLogRequest)
	message.Acks = proto.Int32(input.acks)
	message.TableId = proto.Int64(input.tableID)
	message.TimeoutMs = proto.Int32(int32(input.timeout / time.Millisecond))
	bucketRequest := &fmsg.PbProduceLogReqForBucket{BucketId: proto.Int32(input.bucket), Records: input.records}
	if input.path.Partition != "" {
		bucketRequest.PartitionId = proto.Int64(input.partitionID)
	}
	message.BucketsReq = []*fmsg.PbProduceLogReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, input.path, input.bucket, request)
	if err != nil {
		return 0, err
	}
	produced, ok := response.Message().(*fmsg.ProduceLogResponse)
	if !ok {
		return 0, fmt.Errorf("fgo: produce log: unexpected response %T", response.Message())
	}
	if len(produced.GetBucketsResp()) != 1 || produced.GetBucketsResp()[0].GetBucketId() != input.bucket {
		return 0, fmt.Errorf("%w: produce response omitted bucket %d", ErrValidation, input.bucket)
	}
	result := produced.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyProduceLog); err != nil {
		b.client.invalidatePhysicalOnMetadataError(input.path, err)
		return 0, err
	}
	return result.GetBaseOffset(), nil
}
