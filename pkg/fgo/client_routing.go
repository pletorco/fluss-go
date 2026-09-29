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
