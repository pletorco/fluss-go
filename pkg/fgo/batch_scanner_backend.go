package fgo

import (
	"context"
	"errors"
	"fmt"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type batchScanBackend interface {
	limitScan(context.Context, TableBucket, int32) (bool, []byte, error)
}

type logBatchScanBackend interface {
	limitLogScan(context.Context, Table, TableBucket, int32) (bool, []byte, error)
}

type kvSessionScanBackend interface {
	scanKV(context.Context, TableBucket, int32, []byte, int32, bool) (kvSessionBatch, error)
}

type kvSessionBatch struct {
	scannerID []byte
	hasMore   bool
	records   []byte
	offset    kvScanOffset
}

type kvScanOffset struct {
	value int64
	known bool
}

type clientBatchScanBackend struct{ client *Client }

func (b clientBatchScanBackend) schemaResolver() schemaResolver {
	if b.client == nil || b.client.schemas == nil {
		return nil
	}
	return b.client
}

// ResolveTableBuckets returns every bucket in bucket-ID order with its current
// tablet leader. It returns [ErrNoBucketLeader] instead of an incomplete list.
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
	layout, err := c.fetchBucketLayout(ctx, path)
	if err != nil {
		return nil, err
	}
	return resolvedTableBuckets(layout.tableID, layout.partitionID, layout.count, layout.leaders)
}

func resolvedTableBuckets(
	tableID, partitionID int64,
	bucketCount int32,
	locations map[int32]ServerNode,
) ([]TableBucket, error) {
	buckets, err := completeBucketIDs(bucketCount, locations)
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

func (b clientBatchScanBackend) limitLogScan(
	ctx context.Context, table Table, bucket TableBucket, limit int32,
) (bool, []byte, error) {
	isLog, records, err := b.limitScan(ctx, bucket, limit)
	if ctx.Err() != nil || (!errors.Is(err, ErrMetadata) && !shouldReplaceConnection(err)) {
		return isLog, records, err
	}
	bucket, refreshErr := b.refreshLogScanBucket(ctx, table, bucket)
	if refreshErr != nil {
		return false, nil, errors.Join(err, refreshErr)
	}
	// LIMIT_SCAN is stateless. Retrying once cannot advance a server cursor.
	return b.limitScan(ctx, bucket, limit)
}

func (b clientBatchScanBackend) refreshLogScanBucket(
	ctx context.Context, table Table, bucket TableBucket,
) (TableBucket, error) {
	path := PhysicalTablePath{TablePath: table.Path}
	if bucket.PartitionID >= 0 {
		name, err := b.logScanPartitionName(ctx, table, bucket.PartitionID)
		if err != nil {
			return TableBucket{}, err
		}
		path.Partition = name
	}
	layout, err := b.client.fetchBucketLayout(ctx, path)
	if err != nil {
		return TableBucket{}, err
	}
	if layout.tableID != bucket.TableID || layout.partitionID != bucket.PartitionID {
		return TableBucket{}, fmt.Errorf("%w: %s now has a different physical identity", ErrMetadata, path)
	}
	if layout.count <= bucket.BucketID || (bucket.BucketCount > 0 && layout.count != bucket.BucketCount) {
		return TableBucket{}, fmt.Errorf("%w: %s now has a different bucket layout", ErrMetadata, path)
	}
	leader, ok := layout.leaders[bucket.BucketID]
	if !ok {
		return TableBucket{}, fmt.Errorf("%w: %s bucket %d", ErrNoBucketLeader, path, bucket.BucketID)
	}
	bucket.Leader, bucket.BucketCount = leader, layout.count
	return bucket, nil
}

func (b clientBatchScanBackend) logScanPartitionName(
	ctx context.Context, table Table, partitionID int64,
) (string, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyListPartitionInfos, 0)
	if err != nil {
		return "", err
	}
	message := request.Message().(*fmsg.ListPartitionInfosRequest)
	message.TablePath = pbTablePath(table.Path)
	message.IncludeSystemPartitions = proto.Bool(true)
	response, err := b.client.RequestCoordinator(ctx, request)
	if err != nil {
		return "", err
	}
	partitions, ok := response.Message().(*fmsg.ListPartitionInfosResponse)
	if !ok {
		return "", fmt.Errorf("fgo: list partitions: unexpected response %T", response.Message())
	}
	for _, partition := range partitions.GetPartitionsInfo() {
		if partition != nil && partition.PartitionId != nil && partition.GetPartitionId() == partitionID {
			spec := make(PartitionSpec)
			for _, entry := range partition.GetPartitionSpec().GetPartitionKeyValues() {
				spec[entry.GetKey()] = entry.GetValue()
			}
			return table.Schema.PartitionName(spec)
		}
	}
	return "", fmt.Errorf("%w: %s partition ID %d", ErrUnknownPartition, table.Path, partitionID)
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
	if bucket.BucketCount > 0 {
		message.RoutingBucketCount = proto.Int32(bucket.BucketCount)
	}
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
		// The optional wire limit caps the entire session, not its pages.
		// Leave it unset so the cursor reads the entire bucket.
		message.BucketScanReq = &fmsg.PbScanReqForBucket{
			TableId: proto.Int64(bucket.TableID), BucketId: proto.Int32(bucket.BucketID),
			RoutingBucketCount: proto.Int32(count),
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
		offset: kvScanOffset{value: scanned.GetLogOffset(), known: scanned.LogOffset != nil && scanned.GetLogOffset() >= 0},
	}, nil
}
