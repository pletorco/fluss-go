package fgo

import (
	"context"
	"fmt"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type scannerFetch struct {
	records                []byte
	highWatermark          int64
	filteredEndOffset      int64
	filteredEndOffsetKnown bool
	remote                 *RemoteLogFetchInfo
}

type logScannerBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]ServerNode, error)
	listOffset(context.Context, PhysicalTablePath, int32, int64, int64, ScanOffset) (int64, error)
	fetch(context.Context, logFetchRequest) (scannerFetch, error)
}

type clientLogScannerBackend struct{ client *Client }

func (b clientLogScannerBackend) schemaResolver() schemaResolver {
	if b.client == nil || b.client.schemas == nil {
		return nil
	}
	return b.client
}

type logFetchRequest struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	offset      int64
	projection  []int32
	filter      *scanFilter
	config      LogScannerConfig
}

func (b clientLogScannerBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]ServerNode, error) {
	if path.Partition == "" {
		table, err := b.client.fetchTableMetadata(ctx, path.TablePath)
		return table.ID, table.Buckets, err
	}
	partition, err := b.client.fetchPartitionMetadata(ctx, path)
	return partition.ID, partition.Buckets, err
}

func (b clientLogScannerBackend) listOffset(
	ctx context.Context,
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	start ScanOffset,
) (int64, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyListOffsets, 0)
	if err != nil {
		return 0, err
	}
	message := request.Message().(*fmsg.ListOffsetsRequest)
	message.FollowerServerId = proto.Int32(-1)
	message.TableId = proto.Int64(tableID)
	message.BucketId = []int32{bucket}
	switch start.Kind {
	case ScanFromEarliest:
		message.OffsetType = proto.Int32(0)
	case ScanFromLatest:
		message.OffsetType = proto.Int32(1)
	case ScanFromTimestamp:
		message.OffsetType = proto.Int32(2)
		message.StartTimestamp = proto.Int64(start.Timestamp.UnixMilli())
	default:
		return 0, fmt.Errorf("%w: explicit offset does not require ListOffsets", ErrInvalidConfig)
	}
	if partitionID >= 0 {
		message.PartitionId = proto.Int64(partitionID)
	}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return 0, err
	}
	offsets, ok := response.Message().(*fmsg.ListOffsetsResponse)
	if !ok {
		return 0, fmt.Errorf("fgo: list offsets: unexpected response %T", response.Message())
	}
	if len(offsets.GetBucketsResp()) != 1 || offsets.GetBucketsResp()[0].GetBucketId() != bucket {
		return 0, fmt.Errorf("%w: list offsets omitted bucket %d", ErrValidation, bucket)
	}
	result := offsets.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyListOffsets); err != nil {
		b.client.invalidatePhysicalOnMetadataError(path, err)
		return 0, err
	}
	return result.GetOffset(), nil
}

func (b clientLogScannerBackend) fetch(
	ctx context.Context,
	input logFetchRequest,
) (scannerFetch, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyFetchLog, 0)
	if err != nil {
		return scannerFetch{}, err
	}
	message := request.Message().(*fmsg.FetchLogRequest)
	message.FollowerServerId = proto.Int32(-1)
	message.MaxBytes = proto.Int32(input.config.FetchMaxBytes)
	message.MaxWaitMs = proto.Int32(int32(input.config.FetchWaitMaxTime / time.Millisecond))
	message.MinBytes = proto.Int32(input.config.FetchMinBytes)
	bucketRequest := &fmsg.PbFetchLogReqForBucket{
		BucketId: proto.Int32(input.bucket), FetchOffset: proto.Int64(input.offset),
		MaxFetchBytes: proto.Int32(input.config.FetchMaxBytesForBucket),
	}
	if input.partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(input.partitionID)
	}
	message.TablesReq = []*fmsg.PbFetchLogReqForTable{{
		TableId: proto.Int64(input.tableID), ProjectionPushdownEnabled: proto.Bool(len(input.projection) != 0),
		ProjectedFields: input.projection, BucketsReq: []*fmsg.PbFetchLogReqForBucket{bucketRequest},
	}}
	if input.filter != nil {
		// Fluss requires the predicate and its schema ID to be set together.
		message.TablesReq[0].FilterPredicate = input.filter.predicate
		message.TablesReq[0].FilterSchemaId = proto.Int32(input.filter.schemaID)
	}
	response, err := b.client.RequestBucket(ctx, input.path, input.bucket, request)
	if err != nil {
		return scannerFetch{}, err
	}
	fetched, ok := response.Message().(*fmsg.FetchLogResponse)
	if !ok {
		return scannerFetch{}, fmt.Errorf("fgo: fetch log: unexpected response %T", response.Message())
	}
	if len(fetched.GetTablesResp()) != 1 || fetched.GetTablesResp()[0].GetTableId() != input.tableID ||
		len(fetched.GetTablesResp()[0].GetBucketsResp()) != 1 ||
		fetched.GetTablesResp()[0].GetBucketsResp()[0].GetBucketId() != input.bucket {
		return scannerFetch{}, fmt.Errorf("%w: fetch response omitted table or bucket", ErrValidation)
	}
	result := fetched.GetTablesResp()[0].GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyFetchLog); err != nil {
		b.client.invalidatePhysicalOnMetadataError(input.path, err)
		return scannerFetch{}, err
	}
	records := append([]byte(nil), result.GetRecords()...)
	remote := remoteLogFetchInfo(result.GetRemoteLogFetchInfo())
	if remote != nil {
		remoteRecords, err := b.client.readRemoteLog(ctx, remote)
		if err != nil {
			return scannerFetch{}, err
		}
		records = append(remoteRecords, records...)
	}
	return scannerFetch{
		records: records, highWatermark: result.GetHighWatermark(),
		filteredEndOffset: result.GetFilteredEndOffset(), filteredEndOffsetKnown: result.FilteredEndOffset != nil,
		remote: remote,
	}, nil
}

func remoteLogFetchInfo(info *fmsg.PbRemoteLogFetchInfo) *RemoteLogFetchInfo {
	if info == nil {
		return nil
	}
	result := &RemoteLogFetchInfo{
		TabletDirectory:    info.GetRemoteLogTabletDir(),
		PartitionName:      info.GetPartitionName(),
		FirstStartPosition: int(info.GetFirstStartPos()),
		Segments:           make([]RemoteLogSegment, len(info.GetRemoteLogSegments())),
	}
	for index, segment := range info.GetRemoteLogSegments() {
		result.Segments[index] = RemoteLogSegment{
			ID:          segment.GetRemoteLogSegmentId(),
			StartOffset: segment.GetRemoteLogStartOffset(),
			EndOffset:   segment.GetRemoteLogEndOffset(),
			SizeBytes:   int64(segment.GetSegmentSizeInBytes()),
			MaxTime:     time.UnixMilli(segment.GetMaxTimestamp()),
		}
	}
	return result
}
