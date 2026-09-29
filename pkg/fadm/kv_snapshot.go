package fadm

import (
	"context"
	"fmt"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// KVSnapshot identifies the latest available state for one bucket.
type KVSnapshot struct {
	// Bucket identifies the primary-key table bucket.
	Bucket int32
	// SnapshotID is valid only when Available is true.
	SnapshotID int64
	// LogOffset is the snapshot's inclusive state offset.
	LogOffset int64
	// Available distinguishes no snapshot from snapshot ID zero.
	Available bool
}

// KVSnapshots groups latest snapshot metadata by table partition.
type KVSnapshots struct {
	// TableID is the server-assigned table identifier.
	TableID int64
	// PartitionID is -1 for an unpartitioned table.
	PartitionID int64
	// Snapshots contains one latest state per bucket.
	Snapshots []KVSnapshot
}

// GetLatestKVSnapshots returns current primary-key snapshot IDs and offsets.
func (c *Client) GetLatestKVSnapshots(
	ctx context.Context,
	path fgo.TablePath,
	partition string,
) (KVSnapshots, error) {
	if err := path.Validate(); err != nil {
		return KVSnapshots{}, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyGetLatestKvSnapshots, 0)
	if err != nil {
		return KVSnapshots{}, err
	}
	message := request.Message().(*fmsg.GetLatestKvSnapshotsRequest)
	message.TablePath = pbTablePath(path)
	if partition != "" {
		message.PartitionName = proto.String(partition)
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return KVSnapshots{}, err
	}
	latest, ok := response.Message().(*fmsg.GetLatestKvSnapshotsResponse)
	if !ok {
		return KVSnapshots{}, unexpected("latest KV snapshots", response)
	}
	result := KVSnapshots{TableID: latest.GetTableId(), PartitionID: -1}
	if latest.PartitionId != nil {
		result.PartitionID = latest.GetPartitionId()
	}
	for _, snapshot := range latest.GetLatestSnapshots() {
		result.Snapshots = append(result.Snapshots, KVSnapshot{
			Bucket: snapshot.GetBucketId(), SnapshotID: snapshot.GetSnapshotId(),
			LogOffset: snapshot.GetLogOffset(), Available: snapshot.SnapshotId != nil,
		})
	}
	return result, nil
}

// SnapshotFile maps one remote object to its local snapshot name.
type SnapshotFile struct {
	// RemotePath is the server-managed object location.
	RemotePath string
	// LocalName is the relative name expected by the snapshot reader.
	LocalName string
}

// KVSnapshotMetadata contains immutable files for one primary-key snapshot.
type KVSnapshotMetadata struct {
	// LogOffset is the snapshot's inclusive state offset.
	LogOffset int64
	// Files contains immutable snapshot objects.
	Files []SnapshotFile
}

// KVSnapshotMetadata returns immutable file metadata for one snapshot.
func (c *Client) GetKVSnapshotMetadata(
	ctx context.Context,
	tableID, partitionID int64,
	bucket int32,
	snapshotID int64,
) (KVSnapshotMetadata, error) {
	if tableID < 0 || partitionID < -1 || bucket < 0 || snapshotID < 0 {
		return KVSnapshotMetadata{}, fmt.Errorf("%w: invalid snapshot identity", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyGetKvSnapshotMetadata, 0)
	if err != nil {
		return KVSnapshotMetadata{}, err
	}
	message := request.Message().(*fmsg.GetKvSnapshotMetadataRequest)
	message.TableId, message.BucketId, message.SnapshotId = proto.Int64(tableID), proto.Int32(bucket), proto.Int64(snapshotID)
	if partitionID >= 0 {
		message.PartitionId = proto.Int64(partitionID)
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return KVSnapshotMetadata{}, err
	}
	metadata, ok := response.Message().(*fmsg.GetKvSnapshotMetadataResponse)
	if !ok {
		return KVSnapshotMetadata{}, unexpected("KV snapshot metadata", response)
	}
	result := KVSnapshotMetadata{LogOffset: metadata.GetLogOffset()}
	for _, file := range metadata.GetSnapshotFiles() {
		result.Files = append(result.Files, SnapshotFile{
			RemotePath: file.GetRemotePath(), LocalName: file.GetLocalFileName(),
		})
	}
	return result, nil
}

// KVSnapshotLease identifies one bucket snapshot protected by a lease.
type KVSnapshotLease struct {
	// TableID is the server-assigned table identifier.
	TableID int64
	// PartitionID is -1 for an unpartitioned table.
	PartitionID int64
	// Bucket identifies the primary-key table bucket.
	Bucket int32
	// SnapshotID identifies the protected immutable snapshot.
	SnapshotID int64
}

// AcquireKVSnapshotLease acquires a server-managed lease for duration and
// returns snapshots that were unavailable and therefore not leased. Fluss
// expires acquired snapshots after that duration; callers should release
// individual buckets early or drop the complete lease when no longer needed.
func (c *Client) AcquireKVSnapshotLease(
	ctx context.Context,
	leaseID string,
	duration time.Duration,
	snapshots []KVSnapshotLease,
) ([]KVSnapshotLease, error) {
	if leaseID == "" || duration <= 0 || len(snapshots) == 0 {
		return nil, fmt.Errorf("%w: lease ID, duration, and snapshots are required", fgo.ErrInvalidConfig)
	}
	grouped, err := leaseMessages(snapshots)
	if err != nil {
		return nil, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyAcquireKvSnapshotLease, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.AcquireKvSnapshotLeaseRequest)
	message.LeaseId, message.LeaseDurationMs = proto.String(leaseID), proto.Int64(duration.Milliseconds())
	message.SnapshotsToLease = grouped
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	acquired, ok := response.Message().(*fmsg.AcquireKvSnapshotLeaseResponse)
	if !ok {
		return nil, unexpected("acquire KV snapshot lease", response)
	}
	return leasesFromMessage(acquired.GetUnavailableSnapshots()), nil
}

// RenewKVSnapshotLease extends an existing lease for duration. Fluss identifies
// the lease by leaseID; no snapshot identities are required for renewal.
func (c *Client) RenewKVSnapshotLease(
	ctx context.Context,
	leaseID string,
	duration time.Duration,
) error {
	if leaseID == "" || duration <= 0 {
		return fmt.Errorf("%w: lease ID and positive duration are required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyAcquireKvSnapshotLease, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.AcquireKvSnapshotLeaseRequest)
	message.LeaseId = proto.String(leaseID)
	message.LeaseDurationMs = proto.Int64(duration.Milliseconds())
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

// ReleaseKVSnapshotLease releases selected buckets from leaseID.
func (c *Client) ReleaseKVSnapshotLease(ctx context.Context, leaseID string, buckets []fgo.TableBucket) error {
	if leaseID == "" || len(buckets) == 0 {
		return fmt.Errorf("%w: lease ID and buckets are required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyReleaseKvSnapshotLease, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.ReleaseKvSnapshotLeaseRequest)
	message.LeaseId = proto.String(leaseID)
	for _, bucket := range buckets {
		if bucket.TableID < 0 || bucket.PartitionID < -1 || bucket.BucketID < 0 {
			return fmt.Errorf("%w: invalid lease bucket", fgo.ErrInvalidConfig)
		}
		item := &fmsg.PbTableBucket{TableId: proto.Int64(bucket.TableID), BucketId: proto.Int32(bucket.BucketID)}
		if bucket.PartitionID >= 0 {
			item.PartitionId = proto.Int64(bucket.PartitionID)
		}
		message.BucketsToRelease = append(message.BucketsToRelease, item)
	}
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

// DropKVSnapshotLease releases every bucket held by leaseID.
func (c *Client) DropKVSnapshotLease(ctx context.Context, leaseID string) error {
	if leaseID == "" {
		return fmt.Errorf("%w: lease ID is required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyDropKvSnapshotLease, 0)
	if err != nil {
		return err
	}
	request.Message().(*fmsg.DropKvSnapshotLeaseRequest).LeaseId = proto.String(leaseID)
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func leaseMessages(snapshots []KVSnapshotLease) ([]*fmsg.PbKvSnapshotLeaseForTable, error) {
	grouped := make(map[int64]*fmsg.PbKvSnapshotLeaseForTable)
	order := make([]int64, 0)
	for _, snapshot := range snapshots {
		if snapshot.TableID < 0 || snapshot.PartitionID < -1 || snapshot.Bucket < 0 || snapshot.SnapshotID < 0 {
			return nil, fmt.Errorf("%w: invalid snapshot lease", fgo.ErrInvalidConfig)
		}
		table := grouped[snapshot.TableID]
		if table == nil {
			table = &fmsg.PbKvSnapshotLeaseForTable{TableId: proto.Int64(snapshot.TableID)}
			grouped[snapshot.TableID] = table
			order = append(order, snapshot.TableID)
		}
		bucket := &fmsg.PbKvSnapshotLeaseForBucket{
			BucketId: proto.Int32(snapshot.Bucket), SnapshotId: proto.Int64(snapshot.SnapshotID),
		}
		if snapshot.PartitionID >= 0 {
			bucket.PartitionId = proto.Int64(snapshot.PartitionID)
		}
		table.BucketSnapshots = append(table.BucketSnapshots, bucket)
	}
	result := make([]*fmsg.PbKvSnapshotLeaseForTable, len(order))
	for index, tableID := range order {
		result[index] = grouped[tableID]
	}
	return result, nil
}

func leasesFromMessage(tables []*fmsg.PbKvSnapshotLeaseForTable) []KVSnapshotLease {
	var result []KVSnapshotLease
	for _, table := range tables {
		for _, bucket := range table.GetBucketSnapshots() {
			partitionID := int64(-1)
			if bucket.PartitionId != nil {
				partitionID = bucket.GetPartitionId()
			}
			result = append(result, KVSnapshotLease{
				TableID: table.GetTableId(), PartitionID: partitionID,
				Bucket: bucket.GetBucketId(), SnapshotID: bucket.GetSnapshotId(),
			})
		}
	}
	return result
}
