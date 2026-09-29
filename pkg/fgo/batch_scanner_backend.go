package fgo

import (
	"context"
	"fmt"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type batchScanBackend interface {
	limitScan(context.Context, TableBucket, int32) (bool, []byte, error)
}

type kvSessionScanBackend interface {
	scanKV(context.Context, TableBucket, int64, int32, []byte, int32, bool) (kvSessionBatch, error)
}

type kvSessionBatch struct {
	scannerID []byte
	hasMore   bool
	records   []byte
}

type clientBatchScanBackend struct{ client *Client }

func (b clientBatchScanBackend) schemaResolver() schemaResolver {
	if b.client == nil || b.client.schemas == nil {
		return nil
	}
	return b.client
}

// ResolveTableBuckets returns a stable bucket-ID ordered snapshot of current tablet leaders.
func (c *Client) ResolveTableBuckets(
	ctx context.Context,
	path PhysicalTablePath,
) ([]TableBucket, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if err := path.Validate(); err != nil {
		return nil, err
	}
	if path.Partition == "" {
		table, err := c.fetchTableMetadata(ctx, path.TablePath)
		if err != nil {
			return nil, err
		}
		return resolvedTableBuckets(table.ID, -1, table.BucketCount, table.Buckets)
	}
	partition, err := c.fetchPartitionMetadata(ctx, path)
	if err != nil {
		return nil, err
	}
	table, err := c.fetchTableMetadata(ctx, path.TablePath)
	if err != nil {
		return nil, err
	}
	return resolvedTableBuckets(table.ID, partition.ID, partition.BucketCount, partition.Buckets)
}

func resolvedTableBuckets(
	tableID, partitionID int64,
	bucketCount int32,
	locations map[int32]ServerNode,
) ([]TableBucket, error) {
	buckets, err := sortedBuckets(locations)
	if err != nil {
		return nil, err
	}
	result := make([]TableBucket, len(buckets))
	for index, bucket := range buckets {
		result[index] = TableBucket{
			TableID: tableID, PartitionID: partitionID, BucketID: bucket,
			BucketCount: bucketCount, Leader: locations[bucket],
		}
	}
	return result, nil
}

func (b clientBatchScanBackend) limitScan(
	ctx context.Context,
	bucket TableBucket,
	limit int32,
) (bool, []byte, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyLimitScan, 0)
	if err != nil {
		return false, nil, err
	}
	message := request.Message().(*fmsg.LimitScanRequest)
	message.TableId = proto.Int64(bucket.TableID)
	message.BucketId = proto.Int32(bucket.BucketID)
	message.Limit = proto.Int32(limit)
	if bucket.PartitionID >= 0 {
		message.PartitionId = proto.Int64(bucket.PartitionID)
	}
	response, err := b.client.RequestTo(ctx, bucket.Leader, request)
	if err != nil {
		return false, nil, err
	}
	scanned, ok := response.Message().(*fmsg.LimitScanResponse)
	if !ok {
		return false, nil, fmt.Errorf("fgo: limit scan: unexpected response %T", response.Message())
	}
	if err := responseServerError(
		scanned.GetErrorCode(), scanned.GetErrorMessage(), fmsg.APIKeyLimitScan,
	); err != nil {
		return false, nil, err
	}
	return scanned.GetIsLogTable(), append([]byte(nil), scanned.GetRecords()...), nil
}

func (b clientBatchScanBackend) scanKV(
	ctx context.Context,
	bucket TableBucket,
	limit int64,
	batchSize int32,
	scannerID []byte,
	sequence int32,
	closeScanner bool,
) (kvSessionBatch, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyScanKv, 0)
	if err != nil {
		return kvSessionBatch{}, err
	}
	message := request.Message().(*fmsg.ScanKvRequest)
	message.CallSeqId, message.BatchSizeBytes, message.CloseScanner = proto.Int32(sequence), proto.Int32(batchSize), proto.Bool(closeScanner)
	if len(scannerID) != 0 {
		message.ScannerId = append([]byte(nil), scannerID...)
	} else {
		count := bucket.BucketCount
		if count <= 0 {
			return kvSessionBatch{}, fmt.Errorf("%w: KV scan bucket count must be positive", ErrMetadata)
		}
		message.BucketScanReq = &fmsg.PbScanReqForBucket{
			TableId: proto.Int64(bucket.TableID), BucketId: proto.Int32(bucket.BucketID),
			Limit: proto.Int64(limit), RoutingBucketCount: proto.Int32(count),
		}
		if bucket.PartitionID >= 0 {
			message.BucketScanReq.PartitionId = proto.Int64(bucket.PartitionID)
		}
	}
	response, err := b.client.RequestTo(ctx, bucket.Leader, request)
	if err != nil {
		return kvSessionBatch{}, err
	}
	scanned, ok := response.Message().(*fmsg.ScanKvResponse)
	if !ok {
		return kvSessionBatch{}, fmt.Errorf("fgo: scan KV: unexpected response %T", response.Message())
	}
	if err := responseServerError(scanned.GetErrorCode(), scanned.GetErrorMessage(), fmsg.APIKeyScanKv); err != nil {
		return kvSessionBatch{}, err
	}
	return kvSessionBatch{
		scannerID: append([]byte(nil), scanned.GetScannerId()...),
		hasMore:   scanned.GetHasMoreResults(), records: append([]byte(nil), scanned.GetRecords()...),
	}, nil
}
