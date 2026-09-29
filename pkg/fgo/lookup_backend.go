package fgo

import (
	"context"
	"fmt"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type lookupBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]ServerNode, error)
	lookup(context.Context, lookupRequest) ([][]byte, error)
	prefixLookup(context.Context, PhysicalTablePath, int32, int64, int64, [][]byte) ([][][]byte, error)
}

type clientLookupBackend struct{ client *Client }

func (b clientLookupBackend) schemaResolver() schemaResolver {
	if b.client == nil || b.client.schemas == nil {
		return nil
	}
	return b.client
}

type lookupRequest struct {
	path             PhysicalTablePath
	bucket           int32
	tableID          int64
	partitionID      int64
	keys             [][]byte
	insertIfNotExist bool
	timeout          time.Duration
	acks             int32
}

func (b clientLookupBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]ServerNode, error) {
	return (clientAppendWriterBackend{client: b.client}).metadata(ctx, path)
}

func (b clientLookupBackend) lookup(
	ctx context.Context,
	input lookupRequest,
) ([][]byte, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyLookup, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.LookupRequest)
	message.TableId = proto.Int64(input.tableID)
	if input.insertIfNotExist {
		message.InsertIfNotExists = proto.Bool(true)
		message.TimeoutMs = proto.Int32(int32(input.timeout / time.Millisecond))
		message.Acks = proto.Int32(input.acks)
	}
	bucketRequest := &fmsg.PbLookupReqForBucket{
		BucketId: proto.Int32(input.bucket),
		Keys:     cloneBytesList(input.keys),
	}
	if input.partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(input.partitionID)
	}
	message.BucketsReq = []*fmsg.PbLookupReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, input.path, input.bucket, request)
	if err != nil {
		return nil, err
	}
	lookup, ok := response.Message().(*fmsg.LookupResponse)
	if !ok {
		return nil, fmt.Errorf("fgo: lookup: unexpected response %T", response.Message())
	}
	if len(lookup.GetBucketsResp()) != 1 || lookup.GetBucketsResp()[0].GetBucketId() != input.bucket {
		return nil, fmt.Errorf("%w: lookup response omitted bucket %d", ErrValidation, input.bucket)
	}
	result := lookup.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyLookup); err != nil {
		b.client.invalidatePhysicalOnMetadataError(input.path, err)
		return nil, err
	}
	if len(result.GetValues()) != len(input.keys) {
		return nil, fmt.Errorf(
			"%w: lookup returned %d values for %d keys",
			ErrValidation, len(result.GetValues()), len(input.keys),
		)
	}
	values := make([][]byte, len(input.keys))
	for index, value := range result.GetValues() {
		if value != nil && value.Values != nil {
			values[index] = append([]byte(nil), value.GetValues()...)
		}
	}
	return values, nil
}

func (b clientLookupBackend) prefixLookup(
	ctx context.Context,
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	keys [][]byte,
) ([][][]byte, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyPrefixLookup, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.PrefixLookupRequest)
	message.TableId = proto.Int64(tableID)
	bucketRequest := &fmsg.PbPrefixLookupReqForBucket{BucketId: proto.Int32(bucket), Keys: cloneBytesList(keys)}
	if partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(partitionID)
	}
	message.BucketsReq = []*fmsg.PbPrefixLookupReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return nil, err
	}
	lookup, ok := response.Message().(*fmsg.PrefixLookupResponse)
	if !ok {
		return nil, fmt.Errorf("fgo: prefix lookup: unexpected response %T", response.Message())
	}
	if len(lookup.GetBucketsResp()) != 1 || lookup.GetBucketsResp()[0].GetBucketId() != bucket {
		return nil, fmt.Errorf("%w: prefix lookup response omitted bucket %d", ErrValidation, bucket)
	}
	result := lookup.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyPrefixLookup); err != nil {
		b.client.invalidatePhysicalOnMetadataError(path, err)
		return nil, err
	}
	if len(result.GetValueLists()) != len(keys) {
		return nil, fmt.Errorf("%w: prefix lookup returned %d lists for %d keys", ErrValidation, len(result.GetValueLists()), len(keys))
	}
	values := make([][][]byte, len(keys))
	for index, list := range result.GetValueLists() {
		values[index] = cloneBytesList(list.GetValues())
	}
	return values, nil
}

func cloneBytesList(values [][]byte) [][]byte {
	cloned := make([][]byte, len(values))
	for index, value := range values {
		cloned[index] = append([]byte(nil), value...)
	}
	return cloned
}
