package fadm

import (
	"context"
	"fmt"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// ConfigEntry is one effective cluster configuration value and its source.
type ConfigEntry struct {
	// Key is the cluster configuration name.
	Key string
	// Value is the effective configuration value.
	Value string
	// Source identifies the default or configured value origin.
	Source string
}

// DescribeClusterConfigs returns effective cluster configuration values.
func (c *Client) DescribeClusterConfigs(ctx context.Context) ([]ConfigEntry, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyDescribeClusterConfigs, 0)
	if err != nil {
		return nil, err
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	message, ok := response.Message().(*fmsg.DescribeClusterConfigsResponse)
	if !ok {
		return nil, unexpected("describe cluster configs", response)
	}
	configs := make([]ConfigEntry, len(message.GetConfigs()))
	for index, config := range message.GetConfigs() {
		configs[index] = ConfigEntry{
			Key: config.GetConfigKey(), Value: config.GetConfigValue(), Source: config.GetConfigSource(),
		}
	}
	return configs, nil
}

// AlterClusterConfigs applies configuration changes as one request.
func (c *Client) AlterClusterConfigs(ctx context.Context, changes ...AlterConfig) error {
	if len(changes) == 0 {
		return fmt.Errorf("%w: no cluster config changes", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyAlterClusterConfigs, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.AlterClusterConfigsRequest)
	for _, change := range changes {
		if change.Key == "" || change.Op < ConfigSet || change.Op > ConfigSubtract {
			return fmt.Errorf("%w: invalid cluster config change", fgo.ErrInvalidConfig)
		}
		item := &fmsg.PbAlterConfig{ConfigKey: proto.String(change.Key), OpType: proto.Int32(int32(change.Op))}
		if change.Value != nil {
			item.ConfigValue = proto.String(*change.Value)
		}
		message.AlterConfigs = append(message.AlterConfigs, item)
	}
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

// AddServerTag adds tag to each server ID.
func (c *Client) AddServerTag(ctx context.Context, serverIDs []int32, tag int32) error {
	return c.changeServerTag(ctx, fmsg.APIKeyAddServerTag, serverIDs, tag)
}

// Server tags supported by Apache Fluss 1.0.
const (
	ServerTagPermanentOffline int32 = 0
	ServerTagTemporaryOffline int32 = 1
)

// RemoveServerTag removes tag from each server ID.
func (c *Client) RemoveServerTag(ctx context.Context, serverIDs []int32, tag int32) error {
	return c.changeServerTag(ctx, fmsg.APIKeyRemoveServerTag, serverIDs, tag)
}

func (c *Client) changeServerTag(ctx context.Context, key fmsg.APIKey, serverIDs []int32, tag int32) error {
	if len(serverIDs) == 0 ||
		(tag != ServerTagPermanentOffline && tag != ServerTagTemporaryOffline) {
		return fmt.Errorf("%w: server IDs and a supported tag are required", fgo.ErrInvalidConfig)
	}
	for _, serverID := range serverIDs {
		if serverID < 0 {
			return fmt.Errorf("%w: negative server ID", fgo.ErrInvalidConfig)
		}
	}
	request, err := fmsg.NewRequest(key, 0)
	if err != nil {
		return err
	}
	switch message := request.Message().(type) {
	case *fmsg.AddServerTagRequest:
		message.ServerIds, message.ServerTag = append([]int32(nil), serverIDs...), proto.Int32(tag)
	case *fmsg.RemoveServerTagRequest:
		message.ServerIds, message.ServerTag = append([]int32(nil), serverIDs...), proto.Int32(tag)
	default:
		return fmt.Errorf("%w: unsupported server tag API", fgo.ErrInvalidConfig)
	}
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

// RebalanceProgress is the current state of one rebalance operation.
type RebalanceProgress struct {
	// ID identifies the asynchronous rebalance.
	ID string
	// Status is the Fluss 1.0 protocol status code.
	Status int32
	// Tables contains physical-table progress.
	Tables []RebalanceTableProgress
}

// RebalanceTableProgress groups bucket progress for one physical table.
type RebalanceTableProgress struct {
	// TableID is the server-assigned physical table identifier.
	TableID int64
	// Buckets contains independent bucket moves.
	Buckets []RebalanceBucketProgress
}

// RebalanceBucketProgress describes one bucket move.
type RebalanceBucketProgress struct {
	// PartitionID is -1 for an unpartitioned table.
	PartitionID int64
	// Bucket identifies the logical table bucket.
	Bucket int32
	// Status is the Fluss 1.0 bucket progress code.
	Status int32
	// OriginalLeader is the leader before the rebalance.
	OriginalLeader int32
	// NewLeader is the selected leader after the rebalance.
	NewLeader int32
	// OriginalReplicas contains node IDs before the rebalance.
	OriginalReplicas []int32
	// NewReplicas contains selected node IDs after the rebalance.
	NewReplicas []int32
}

// Rebalance starts an asynchronous rebalance and returns its ID.
func (c *Client) Rebalance(ctx context.Context, goals ...int32) (string, error) {
	if len(goals) == 0 {
		return "", fmt.Errorf("%w: no rebalance goals", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyRebalance, 0)
	if err != nil {
		return "", err
	}
	request.Message().(*fmsg.RebalanceRequest).Goals = append([]int32(nil), goals...)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return "", err
	}
	message, ok := response.Message().(*fmsg.RebalanceResponse)
	if !ok {
		return "", unexpected("start rebalance", response)
	}
	if message.GetRebalanceId() == "" {
		return "", fmt.Errorf("%w: empty rebalance ID", fgo.ErrValidation)
	}
	return message.GetRebalanceId(), nil
}

// ListRebalanceProgress returns the latest state for id.
func (c *Client) ListRebalanceProgress(ctx context.Context, id string) (RebalanceProgress, error) {
	if id == "" {
		return RebalanceProgress{}, fmt.Errorf("%w: rebalance ID is required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyListRebalanceProgress, 0)
	if err != nil {
		return RebalanceProgress{}, err
	}
	request.Message().(*fmsg.ListRebalanceProgressRequest).RebalanceId = proto.String(id)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return RebalanceProgress{}, err
	}
	message, ok := response.Message().(*fmsg.ListRebalanceProgressResponse)
	if !ok {
		return RebalanceProgress{}, unexpected("rebalance progress", response)
	}
	progress := RebalanceProgress{ID: message.GetRebalanceId(), Status: message.GetRebalanceStatus()}
	for _, table := range message.GetTableProgress() {
		tableProgress := RebalanceTableProgress{TableID: table.GetTableId()}
		for _, bucket := range table.GetBucketsProgress() {
			plan := bucket.GetRebalancePlan()
			tableProgress.Buckets = append(tableProgress.Buckets, RebalanceBucketProgress{
				PartitionID: plan.GetPartitionId(), Bucket: plan.GetBucketId(),
				Status: bucket.GetRebalanceStatus(), OriginalLeader: plan.GetOriginalLeader(),
				NewLeader:        plan.GetNewLeader(),
				OriginalReplicas: append([]int32(nil), plan.GetOriginalReplicas()...),
				NewReplicas:      append([]int32(nil), plan.GetNewReplicas()...),
			})
		}
		progress.Tables = append(progress.Tables, tableProgress)
	}
	return progress, nil
}

// WaitRebalance polls until status is no longer zero (running), or the context is canceled.
func (c *Client) WaitRebalance(ctx context.Context, id string, interval time.Duration) (RebalanceProgress, error) {
	if interval <= 0 {
		return RebalanceProgress{}, fmt.Errorf("%w: rebalance poll interval must be positive", fgo.ErrInvalidConfig)
	}
	for {
		progress, err := c.ListRebalanceProgress(ctx, id)
		if err != nil || progress.Status != 0 {
			return progress, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return RebalanceProgress{}, ctx.Err()
		}
	}
}

// CancelRebalance requests cancellation of id.
func (c *Client) CancelRebalance(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("%w: rebalance ID is required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyCancelRebalance, 0)
	if err != nil {
		return err
	}
	request.Message().(*fmsg.CancelRebalanceRequest).RebalanceId = proto.String(id)
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

// FileSystemSecurityToken is the shared redacting token representation.
type FileSystemSecurityToken = fgo.FileSystemSecurityToken

// FileSystemSecurityToken requests temporary filesystem credentials.
func (c *Client) GetFileSystemSecurityToken(ctx context.Context) (FileSystemSecurityToken, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyGetFilesystemSecurityToken, 0)
	if err != nil {
		return FileSystemSecurityToken{}, err
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return FileSystemSecurityToken{}, err
	}
	message, ok := response.Message().(*fmsg.GetFileSystemSecurityTokenResponse)
	if !ok {
		return FileSystemSecurityToken{}, unexpected("filesystem security token", response)
	}
	result := FileSystemSecurityToken{
		Schema: message.GetSchema(), Token: append([]byte(nil), message.GetToken()...),
		ExpiresAt: millis(message.GetExpirationTime()), AdditionalInfo: make(map[string]string),
	}
	for _, item := range message.GetAdditionInfo() {
		result.AdditionalInfo[item.GetKey()] = item.GetValue()
	}
	return result, nil
}
