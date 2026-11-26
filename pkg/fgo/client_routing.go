package fgo

import (
	"errors"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func (c *Client) invalidatePhysicalOnMetadataError(path PhysicalTablePath, err error) {
	if c != nil && c.router != nil && errors.Is(err, ErrMetadata) {
		c.router.InvalidatePhysical(path)
	}
}

// ListOffsets can report a moved leader in its response body rather than an
// RPC error. Reuse the bounded reroute only for a matching single-bucket read.
func staleListOffsetsReply(request fmsg.Request, response fmsg.Response, bucket int32) bool {
	if response == nil || request.APIKey() != fmsg.APIKeyListOffsets {
		return false
	}
	typed, ok := request.(*fmsg.MessageRequest)
	if !ok {
		return false
	}
	input, ok := typed.Message().(*fmsg.ListOffsetsRequest)
	if !ok || len(input.GetBucketId()) != 1 || input.GetBucketId()[0] != bucket {
		return false
	}
	offsets, ok := response.Message().(*fmsg.ListOffsetsResponse)
	if !ok || len(offsets.GetBucketsResp()) != 1 {
		return false
	}
	result := offsets.GetBucketsResp()[0]
	return result != nil && result.BucketId != nil && result.GetBucketId() == bucket &&
		errorCategory(fmsg.ErrorCode(result.GetErrorCode())) == ErrMetadata
}

func setRoutingBucketCount(request fmsg.Request, count int32) {
	if request == nil || count <= 0 {
		return
	}
	typed, ok := request.(interface{ Message() proto.Message })
	if !ok {
		return
	}
	value := proto.Int32(count)
	switch message := typed.Message().(type) {
	case *fmsg.ProduceLogRequest:
		setProduceRoutingBucketCount(message, value)
	case *fmsg.FetchLogRequest:
		setFetchRoutingBucketCount(message, value)
	case *fmsg.PutKvRequest:
		setPutRoutingBucketCount(message, value)
	case *fmsg.LookupRequest:
		setLookupRoutingBucketCount(message, value)
	case *fmsg.PrefixLookupRequest:
		setPrefixLookupRoutingBucketCount(message, value)
	case *fmsg.LimitScanRequest:
		message.RoutingBucketCount = value
	case *fmsg.ScanKvRequest:
		if message.BucketScanReq != nil {
			message.BucketScanReq.RoutingBucketCount = value
		}
	case *fmsg.ListOffsetsRequest:
		message.RoutingBucketCount = value
	case *fmsg.GetTableStatsRequest:
		setTableStatsRoutingBucketCount(message, value)
	}
}

func setProduceRoutingBucketCount(request *fmsg.ProduceLogRequest, count *int32) {
	for _, bucket := range request.GetBucketsReq() {
		bucket.RoutingBucketCount = count
	}
}

func setFetchRoutingBucketCount(request *fmsg.FetchLogRequest, count *int32) {
	for _, table := range request.GetTablesReq() {
		for _, bucket := range table.GetBucketsReq() {
			bucket.RoutingBucketCount = count
		}
	}
}

func setPutRoutingBucketCount(request *fmsg.PutKvRequest, count *int32) {
	for _, bucket := range request.GetBucketsReq() {
		bucket.RoutingBucketCount = count
	}
}

func setLookupRoutingBucketCount(request *fmsg.LookupRequest, count *int32) {
	for _, bucket := range request.GetBucketsReq() {
		bucket.RoutingBucketCount = count
	}
}

func setPrefixLookupRoutingBucketCount(request *fmsg.PrefixLookupRequest, count *int32) {
	for _, bucket := range request.GetBucketsReq() {
		bucket.RoutingBucketCount = count
	}
}

func setTableStatsRoutingBucketCount(request *fmsg.GetTableStatsRequest, count *int32) {
	for _, bucket := range request.GetBucketsReq() {
		bucket.RoutingBucketCount = count
	}
}
