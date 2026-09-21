package fadm

import (
	"context"
	"fmt"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// AlterDatabase describes Fluss 1.0 database metadata changes.
type AlterDatabase struct {
	// Config contains configuration operations.
	Config []AlterConfig
	// Comment replaces the database comment when non-nil. An empty string clears it.
	Comment *string
}

// AlterDatabase applies configuration and comment changes to name.
func (c *Client) AlterDatabase(
	ctx context.Context,
	name string,
	changes AlterDatabase,
	ignoreIfNotExists bool,
) error {
	if err := validateName("database", name); err != nil {
		return err
	}
	if len(changes.Config) == 0 && changes.Comment == nil {
		return fmt.Errorf("%w: alter database has no changes", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyAlterDatabase, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.AlterDatabaseRequest)
	message.DatabaseName, message.IgnoreIfNotExists = proto.String(name), proto.Bool(ignoreIfNotExists)
	message.Comment = changes.Comment
	for _, change := range changes.Config {
		item, err := alterConfigMessage(change)
		if err != nil {
			return err
		}
		message.ConfigChanges = append(message.ConfigChanges, item)
	}
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func alterConfigMessage(change AlterConfig) (*fmsg.PbAlterConfig, error) {
	if change.Key == "" || change.Op < ConfigSet || change.Op > ConfigSubtract {
		return nil, fmt.Errorf("%w: invalid config change", fgo.ErrInvalidConfig)
	}
	if change.Op != ConfigDelete && change.Value == nil {
		return nil, fmt.Errorf("%w: config value is required", fgo.ErrInvalidConfig)
	}
	return &fmsg.PbAlterConfig{
		ConfigKey: proto.String(change.Key), ConfigValue: change.Value,
		OpType: proto.Int32(int32(change.Op)),
	}, nil
}

// ClusterHealthStatus is the aggregate Fluss 1.0 cluster state.
type ClusterHealthStatus int32

const (
	// ClusterHealthGreen reports that every expected leader and replica is active and in sync.
	ClusterHealthGreen ClusterHealthStatus = 0
	// ClusterHealthYellow reports a degraded cluster that can still serve leaders.
	ClusterHealthYellow ClusterHealthStatus = 1
	// ClusterHealthRed reports missing active leaders.
	ClusterHealthRed ClusterHealthStatus = 2
	// ClusterHealthUnknown reports a status value the client cannot classify.
	ClusterHealthUnknown ClusterHealthStatus = 3
)

// ClusterHealth contains aggregate replica and leader counts.
type ClusterHealth struct {
	Replicas             int32
	InSyncReplicas       int32
	LeaderReplicas       int32
	ActiveLeaderReplicas int32
	Status               ClusterHealthStatus
}

// GetClusterHealth returns the coordinator's aggregate cluster health snapshot.
func (c *Client) GetClusterHealth(ctx context.Context) (ClusterHealth, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyGetClusterHealth, 0)
	if err != nil {
		return ClusterHealth{}, err
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return ClusterHealth{}, err
	}
	message, ok := response.Message().(*fmsg.GetClusterHealthResponse)
	if !ok {
		return ClusterHealth{}, unexpected("cluster health", response)
	}
	return ClusterHealth{
		Replicas: message.GetNumReplicas(), InSyncReplicas: message.GetInSyncReplicas(),
		LeaderReplicas: message.GetNumLeaderReplicas(), ActiveLeaderReplicas: message.GetActiveLeaderReplicas(),
		Status: ClusterHealthStatus(message.GetStatus()),
	}, nil
}

// RemoteLogManifest identifies the committed remote-log manifest for one bucket.
type RemoteLogManifest struct {
	TableID      int64
	PartitionID  int64
	Bucket       int32
	Path         string
	LogEndOffset int64
}

// ListRemoteLogManifests lists committed manifests for a table or partition.
// partitionID is -1 for an unpartitioned table.
func (c *Client) ListRemoteLogManifests(
	ctx context.Context,
	tableID, partitionID int64,
) ([]RemoteLogManifest, error) {
	if tableID < 0 || partitionID < -1 {
		return nil, fmt.Errorf("%w: invalid remote-log manifest identity", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyListRemoteLogManifests, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.ListRemoteLogManifestsRequest)
	message.TableId = proto.Int64(tableID)
	if partitionID >= 0 {
		message.PartitionId = proto.Int64(partitionID)
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	listed, ok := response.Message().(*fmsg.ListRemoteLogManifestsResponse)
	if !ok {
		return nil, unexpected("list remote-log manifests", response)
	}
	result := make([]RemoteLogManifest, 0, len(listed.GetManifests()))
	for _, item := range listed.GetManifests() {
		bucket := item.GetTableBucket()
		actualPartitionID := int64(-1)
		if bucket.PartitionId != nil {
			actualPartitionID = bucket.GetPartitionId()
		}
		result = append(result, RemoteLogManifest{
			TableID: bucket.GetTableId(), PartitionID: actualPartitionID, Bucket: bucket.GetBucketId(),
			Path: item.GetRemoteLogManifestPath(), LogEndOffset: item.GetRemoteLogEndOffset(),
		})
	}
	return result, nil
}

// ListKVSnapshots lists every active retained or in-use KV snapshot.
// Multiple entries for one bucket are preserved. partitionID is -1 for an unpartitioned table.
func (c *Client) ListKVSnapshots(
	ctx context.Context,
	tableID, partitionID int64,
) (KVSnapshots, error) {
	if tableID < 0 || partitionID < -1 {
		return KVSnapshots{}, fmt.Errorf("%w: invalid KV snapshot identity", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyListKvSnapshots, 0)
	if err != nil {
		return KVSnapshots{}, err
	}
	message := request.Message().(*fmsg.ListKvSnapshotsRequest)
	message.TableId = proto.Int64(tableID)
	if partitionID >= 0 {
		message.PartitionId = proto.Int64(partitionID)
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return KVSnapshots{}, err
	}
	listed, ok := response.Message().(*fmsg.ListKvSnapshotsResponse)
	if !ok {
		return KVSnapshots{}, unexpected("list KV snapshots", response)
	}
	result := KVSnapshots{TableID: listed.GetTableId(), PartitionID: -1}
	if listed.PartitionId != nil {
		result.PartitionID = listed.GetPartitionId()
	}
	for _, snapshot := range listed.GetActiveSnapshots() {
		result.Snapshots = append(result.Snapshots, KVSnapshot{
			Bucket: snapshot.GetBucketId(), SnapshotID: snapshot.GetSnapshotId(),
			LogOffset: snapshot.GetLogOffset(), Available: snapshot.SnapshotId != nil,
		})
	}
	return result, nil
}
