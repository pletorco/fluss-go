package fadm

import (
	"context"
	"fmt"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// LakeBucketSnapshot describes one bucket included in a lake snapshot.
type LakeBucketSnapshot struct {
	// PartitionID is -1 for an unpartitioned table.
	PartitionID int64
	// PartitionName is empty for an unpartitioned table.
	PartitionName string
	// Bucket identifies the logical table bucket.
	Bucket int32
	// LogOffset is the included state offset.
	LogOffset int64
}

// LakeSnapshot contains the server-selected lake snapshot state.
type LakeSnapshot struct {
	// TableID is the server-assigned table identifier.
	TableID int64
	// SnapshotID identifies the lake snapshot.
	SnapshotID int64
	// Buckets contains included bucket state.
	Buckets []LakeBucketSnapshot
}

// GetLatestLakeSnapshot returns the latest tiered lake snapshot.
func (c *Client) GetLatestLakeSnapshot(ctx context.Context, path fgo.TablePath) (LakeSnapshot, error) {
	return c.getLakeSnapshot(ctx, path, nil, false)
}

// GetLakeSnapshot returns the historical lake snapshot identified by snapshotID.
func (c *Client) GetLakeSnapshot(ctx context.Context, path fgo.TablePath, snapshotID int64) (LakeSnapshot, error) {
	return c.getLakeSnapshot(ctx, path, &snapshotID, false)
}

// GetReadableLakeSnapshot returns the latest snapshot safe for union reads.
func (c *Client) GetReadableLakeSnapshot(ctx context.Context, path fgo.TablePath) (LakeSnapshot, error) {
	return c.getLakeSnapshot(ctx, path, nil, true)
}

func (c *Client) getLakeSnapshot(
	ctx context.Context,
	path fgo.TablePath,
	snapshotID *int64,
	readable bool,
) (LakeSnapshot, error) {
	if err := path.Validate(); err != nil {
		return LakeSnapshot{}, err
	}
	if snapshotID != nil && *snapshotID < 0 {
		return LakeSnapshot{}, fmt.Errorf("%w: negative lake snapshot ID", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyGetLakeSnapshot, 0)
	if err != nil {
		return LakeSnapshot{}, err
	}
	message := request.Message().(*fmsg.GetLakeSnapshotRequest)
	message.TablePath, message.Readable = pbTablePath(path), proto.Bool(readable)
	if snapshotID != nil {
		message.SnapshotId = proto.Int64(*snapshotID)
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return LakeSnapshot{}, err
	}
	snapshot, ok := response.Message().(*fmsg.GetLakeSnapshotResponse)
	if !ok {
		return LakeSnapshot{}, unexpected("lake snapshot", response)
	}
	result := LakeSnapshot{TableID: snapshot.GetTableId(), SnapshotID: snapshot.GetSnapshotId()}
	for _, bucket := range snapshot.GetBucketSnapshots() {
		partitionID := int64(-1)
		if bucket.PartitionId != nil {
			partitionID = bucket.GetPartitionId()
		}
		result.Buckets = append(result.Buckets, LakeBucketSnapshot{
			PartitionID: partitionID, PartitionName: bucket.GetPartitionName(),
			Bucket: bucket.GetBucketId(), LogOffset: bucket.GetLogOffset(),
		})
	}
	return result, nil
}

// TableStats is the table and log size result for one bucket.
type TableStats struct {
	// Bucket is the requested bucket ID.
	Bucket int32
	// RowCount is valid when Err is nil.
	RowCount int64
	// PartitionID is -1 for an unpartitioned table.
	PartitionID int64
	// Err is the bucket-local statistics failure.
	Err error
}

// TableStats returns one independent result per requested bucket in input
// order. Callers must inspect every result's Err and preserve partial success.
func (c *Client) GetTableStats(
	ctx context.Context,
	table fgo.Table,
	path fgo.PhysicalTablePath,
	partitionID int64,
	buckets []int32,
) []TableStats {
	results := make([]TableStats, len(buckets))
	for index, bucket := range buckets {
		results[index] = TableStats{Bucket: bucket, PartitionID: partitionID}
		if table.ID < 0 || partitionID < -1 || bucket < 0 {
			results[index].Err = fmt.Errorf("%w: invalid table stats identity", fgo.ErrInvalidConfig)
			continue
		}
		request, err := fmsg.NewRequest(fmsg.APIKeyGetTableStats, 0)
		if err != nil {
			results[index].Err = err
			continue
		}
		bucketRequest := &fmsg.PbTableStatsReqForBucket{BucketId: proto.Int32(bucket)}
		if partitionID >= 0 {
			bucketRequest.PartitionId = proto.Int64(partitionID)
		}
		message := request.Message().(*fmsg.GetTableStatsRequest)
		message.TableId, message.BucketsReq = proto.Int64(table.ID), []*fmsg.PbTableStatsReqForBucket{bucketRequest}
		response, err := c.requester.RequestBucket(ctx, path, bucket, request)
		if err != nil {
			results[index].Err = err
			continue
		}
		stats, ok := response.Message().(*fmsg.GetTableStatsResponse)
		if !ok || len(stats.GetBucketsResp()) != 1 || stats.GetBucketsResp()[0].GetBucketId() != bucket {
			results[index].Err = fmt.Errorf("%w: table stats omitted bucket %d", fgo.ErrValidation, bucket)
			continue
		}
		item := stats.GetBucketsResp()[0]
		results[index].RowCount = item.GetRowCount()
		results[index].Err = fgo.ResponseError(item.GetErrorCode(), item.GetErrorMessage(), fmsg.APIKeyGetTableStats)
	}
	return results
}
