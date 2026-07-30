package fadm

import (
	"context"
	"fmt"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type ACL struct {
	ResourceName  string
	ResourceType  int32
	PrincipalName string
	PrincipalType string
	Host          string
	Operation     int32
	Permission    int32
}

func (a ACL) validate() error {
	if a.ResourceName == "" || a.PrincipalName == "" || a.PrincipalType == "" || a.Host == "" ||
		a.ResourceType < 0 || a.Operation < 0 || a.Permission < 0 {
		return fmt.Errorf("%w: invalid ACL", fgo.ErrInvalidConfig)
	}
	return nil
}

func (a ACL) message() *fmsg.PbAclInfo {
	return &fmsg.PbAclInfo{
		ResourceName: proto.String(a.ResourceName), ResourceType: proto.Int32(a.ResourceType),
		PrincipalName: proto.String(a.PrincipalName), PrincipalType: proto.String(a.PrincipalType),
		Host: proto.String(a.Host), OperationType: proto.Int32(a.Operation),
		PermissionType: proto.Int32(a.Permission),
	}
}

type ACLFilter struct {
	ResourceName  *string
	ResourceType  int32
	PrincipalName *string
	PrincipalType *string
	Host          *string
	Operation     int32
	Permission    int32
}

func (f ACLFilter) validate() error {
	if f.ResourceType < 0 || f.Operation < 0 || f.Permission < 0 {
		return fmt.Errorf("%w: invalid ACL filter", fgo.ErrInvalidConfig)
	}
	return nil
}

func (f ACLFilter) message() *fmsg.PbAclFilter {
	return &fmsg.PbAclFilter{
		ResourceName: f.ResourceName, ResourceType: proto.Int32(f.ResourceType),
		PrincipalName: f.PrincipalName, PrincipalType: f.PrincipalType, Host: f.Host,
		OperationType: proto.Int32(f.Operation), PermissionType: proto.Int32(f.Permission),
	}
}

type ACLResult struct {
	ACL ACL
	Err error
}

func (c *Client) CreateACLs(ctx context.Context, acls ...ACL) ([]ACLResult, error) {
	if len(acls) == 0 {
		return nil, fmt.Errorf("%w: no ACLs", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyCreateAcls, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.CreateAclsRequest)
	for _, acl := range acls {
		if err := acl.validate(); err != nil {
			return nil, err
		}
		message.Acl = append(message.Acl, acl.message())
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	created, ok := response.Message().(*fmsg.CreateAclsResponse)
	if !ok {
		return nil, unexpected("create ACLs", response)
	}
	if len(created.GetAclRes()) != len(acls) {
		return nil, fmt.Errorf("%w: create ACL response count mismatch", fgo.ErrValidation)
	}
	results := make([]ACLResult, len(acls))
	for index, item := range created.GetAclRes() {
		results[index] = ACLResult{
			ACL: aclFromMessage(item.GetAcl()),
			Err: fgo.ResponseError(item.GetErrorCode(), item.GetErrorMessage(), fmsg.APIKeyCreateAcls),
		}
	}
	return results, nil
}

func (c *Client) ListACLs(ctx context.Context, filter ACLFilter) ([]ACL, error) {
	if err := filter.validate(); err != nil {
		return nil, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyListAcls, 0)
	if err != nil {
		return nil, err
	}
	request.Message().(*fmsg.ListAclsRequest).AclFilter = filter.message()
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	list, ok := response.Message().(*fmsg.ListAclsResponse)
	if !ok {
		return nil, unexpected("list ACLs", response)
	}
	acls := make([]ACL, len(list.GetAcl()))
	for index, acl := range list.GetAcl() {
		acls[index] = aclFromMessage(acl)
	}
	return acls, nil
}

type DropACLResult struct {
	Filter  ACLFilter
	Matches []ACLResult
	Err     error
}

func (c *Client) DropACLs(ctx context.Context, filters ...ACLFilter) ([]DropACLResult, error) {
	if len(filters) == 0 {
		return nil, fmt.Errorf("%w: no ACL filters", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyDropAcls, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.DropAclsRequest)
	for _, filter := range filters {
		if err := filter.validate(); err != nil {
			return nil, err
		}
		message.AclFilter = append(message.AclFilter, filter.message())
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	dropped, ok := response.Message().(*fmsg.DropAclsResponse)
	if !ok {
		return nil, unexpected("drop ACLs", response)
	}
	if len(dropped.GetFilterResults()) != len(filters) {
		return nil, fmt.Errorf("%w: drop ACL response count mismatch", fgo.ErrValidation)
	}
	results := make([]DropACLResult, len(filters))
	for index, item := range dropped.GetFilterResults() {
		results[index].Filter = filters[index]
		results[index].Err = fgo.ResponseError(item.GetErrorCode(), item.GetErrorMessage(), fmsg.APIKeyDropAcls)
		for _, match := range item.GetMatchingAcls() {
			results[index].Matches = append(results[index].Matches, ACLResult{
				ACL: aclFromMessage(match.GetAcl()),
				Err: fgo.ResponseError(match.GetErrorCode(), match.GetErrorMessage(), fmsg.APIKeyDropAcls),
			})
		}
	}
	return results, nil
}

func aclFromMessage(acl *fmsg.PbAclInfo) ACL {
	return ACL{
		ResourceName: acl.GetResourceName(), ResourceType: acl.GetResourceType(),
		PrincipalName: acl.GetPrincipalName(), PrincipalType: acl.GetPrincipalType(),
		Host: acl.GetHost(), Operation: acl.GetOperationType(), Permission: acl.GetPermissionType(),
	}
}

type ClusterConfig struct {
	Key, Value, Source string
}

func (c *Client) DescribeClusterConfigs(ctx context.Context) ([]ClusterConfig, error) {
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
	configs := make([]ClusterConfig, len(message.GetConfigs()))
	for index, config := range message.GetConfigs() {
		configs[index] = ClusterConfig{
			Key: config.GetConfigKey(), Value: config.GetConfigValue(), Source: config.GetConfigSource(),
		}
	}
	return configs, nil
}

func (c *Client) AlterClusterConfigs(ctx context.Context, changes ...ConfigChange) error {
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

func (c *Client) AddServerTag(ctx context.Context, serverIDs []int32, tag int32) error {
	return c.changeServerTag(ctx, fmsg.APIKeyAddServerTag, serverIDs, tag)
}

func (c *Client) RemoveServerTag(ctx context.Context, serverIDs []int32, tag int32) error {
	return c.changeServerTag(ctx, fmsg.APIKeyRemoveServerTag, serverIDs, tag)
}

func (c *Client) changeServerTag(ctx context.Context, key fmsg.APIKey, serverIDs []int32, tag int32) error {
	if len(serverIDs) == 0 || tag < 0 {
		return fmt.Errorf("%w: server IDs and non-negative tag are required", fgo.ErrInvalidConfig)
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

type RebalanceProgress struct {
	ID     string
	Status int32
	Tables []RebalanceTableProgress
}

type RebalanceTableProgress struct {
	TableID int64
	Buckets []RebalanceBucketProgress
}

type RebalanceBucketProgress struct {
	PartitionID               int64
	Bucket                    int32
	Status                    int32
	OriginalLeader, NewLeader int32
	OriginalReplicas          []int32
	NewReplicas               []int32
}

func (c *Client) StartRebalance(ctx context.Context, goals ...int32) (string, error) {
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

func (c *Client) RebalanceProgress(ctx context.Context, id string) (RebalanceProgress, error) {
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
		progress, err := c.RebalanceProgress(ctx, id)
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

type ProducerBucketOffset struct {
	PartitionID int64
	Bucket      int32
	Offset      int64
}

type ProducerTableOffsets struct {
	TableID int64
	Offsets []ProducerBucketOffset
}

type ProducerOffsets struct {
	ProducerID string
	ExpiresAt  time.Time
	Tables     []ProducerTableOffsets
}

func (c *Client) RegisterProducerOffsets(
	ctx context.Context,
	producerID string,
	tables []ProducerTableOffsets,
	ttl time.Duration,
) (bool, error) {
	if producerID == "" || len(tables) == 0 || ttl <= 0 {
		return false, fmt.Errorf("%w: producer ID, offsets, and TTL are required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyRegisterProducerOffsets, 0)
	if err != nil {
		return false, err
	}
	message := request.Message().(*fmsg.RegisterProducerOffsetsRequest)
	message.ProducerId, message.TtlMs = proto.String(producerID), proto.Int64(ttl.Milliseconds())
	for _, table := range tables {
		item, err := producerTableOffsetsMessage(table)
		if err != nil {
			return false, err
		}
		message.TableOffsets = append(message.TableOffsets, item)
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return false, err
	}
	registered, ok := response.Message().(*fmsg.RegisterProducerOffsetsResponse)
	if !ok {
		return false, unexpected("register producer offsets", response)
	}
	return registered.GetResult() == 0, nil
}

func producerTableOffsetsMessage(table ProducerTableOffsets) (*fmsg.PbProducerTableOffsets, error) {
	if table.TableID < 0 || len(table.Offsets) == 0 {
		return nil, fmt.Errorf("%w: invalid producer table offsets", fgo.ErrInvalidConfig)
	}
	item := &fmsg.PbProducerTableOffsets{TableId: proto.Int64(table.TableID)}
	for _, offset := range table.Offsets {
		if offset.Bucket < 0 || offset.Offset < 0 || offset.PartitionID < -1 {
			return nil, fmt.Errorf("%w: invalid producer bucket offset", fgo.ErrInvalidConfig)
		}
		pbOffset := &fmsg.PbBucketOffset{
			BucketId: proto.Int32(offset.Bucket), LogEndOffset: proto.Int64(offset.Offset),
		}
		if offset.PartitionID >= 0 {
			pbOffset.PartitionId = proto.Int64(offset.PartitionID)
		}
		item.BucketOffsets = append(item.BucketOffsets, pbOffset)
	}
	return item, nil
}

func (c *Client) GetProducerOffsets(ctx context.Context, producerID string) (ProducerOffsets, error) {
	if producerID == "" {
		return ProducerOffsets{}, fmt.Errorf("%w: producer ID is required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyGetProducerOffsets, 0)
	if err != nil {
		return ProducerOffsets{}, err
	}
	request.Message().(*fmsg.GetProducerOffsetsRequest).ProducerId = proto.String(producerID)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return ProducerOffsets{}, err
	}
	message, ok := response.Message().(*fmsg.GetProducerOffsetsResponse)
	if !ok {
		return ProducerOffsets{}, unexpected("get producer offsets", response)
	}
	result := ProducerOffsets{ProducerID: message.GetProducerId(), ExpiresAt: millis(message.GetExpirationTime())}
	result.Tables = producerOffsetsFromMessage(message.GetTableOffsets())
	return result, nil
}

func (c *Client) DeleteProducerOffsets(ctx context.Context, producerID string) error {
	if producerID == "" {
		return fmt.Errorf("%w: producer ID is required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyDeleteProducerOffsets, 0)
	if err != nil {
		return err
	}
	request.Message().(*fmsg.DeleteProducerOffsetsRequest).ProducerId = proto.String(producerID)
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func producerOffsetsFromMessage(tables []*fmsg.PbProducerTableOffsets) []ProducerTableOffsets {
	result := make([]ProducerTableOffsets, len(tables))
	for index, table := range tables {
		result[index].TableID = table.GetTableId()
		for _, offset := range table.GetBucketOffsets() {
			partitionID := int64(-1)
			if offset.PartitionId != nil {
				partitionID = offset.GetPartitionId()
			}
			result[index].Offsets = append(result[index].Offsets, ProducerBucketOffset{
				PartitionID: partitionID, Bucket: offset.GetBucketId(), Offset: offset.GetLogEndOffset(),
			})
		}
	}
	return result
}

type KVSnapshot struct {
	Bucket     int32
	SnapshotID int64
	LogOffset  int64
	Available  bool
}

type LatestKVSnapshot struct {
	TableID, PartitionID int64
	Snapshots            []KVSnapshot
}

func (c *Client) LatestKVSnapshots(
	ctx context.Context,
	path fgo.TablePath,
	partition string,
) (LatestKVSnapshot, error) {
	if err := path.Validate(); err != nil {
		return LatestKVSnapshot{}, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyGetLatestKvSnapshots, 0)
	if err != nil {
		return LatestKVSnapshot{}, err
	}
	message := request.Message().(*fmsg.GetLatestKvSnapshotsRequest)
	message.TablePath = pbTablePath(path)
	if partition != "" {
		message.PartitionName = proto.String(partition)
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return LatestKVSnapshot{}, err
	}
	latest, ok := response.Message().(*fmsg.GetLatestKvSnapshotsResponse)
	if !ok {
		return LatestKVSnapshot{}, unexpected("latest KV snapshots", response)
	}
	result := LatestKVSnapshot{TableID: latest.GetTableId(), PartitionID: -1}
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

type SnapshotFile struct{ RemotePath, LocalName string }

type KVSnapshotMetadata struct {
	LogOffset int64
	Files     []SnapshotFile
}

func (c *Client) KVSnapshotMetadata(
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

type SnapshotLease struct {
	TableID, PartitionID int64
	Bucket               int32
	SnapshotID           int64
}

// AcquireKVSnapshotLease acquires a server-managed lease for duration. Fluss
// expires the lease after that duration; callers should release individual
// buckets early or drop the complete lease when they no longer need it.
func (c *Client) AcquireKVSnapshotLease(
	ctx context.Context,
	leaseID string,
	duration time.Duration,
	snapshots []SnapshotLease,
) ([]SnapshotLease, error) {
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

func leaseMessages(snapshots []SnapshotLease) ([]*fmsg.PbKvSnapshotLeaseForTable, error) {
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

func leasesFromMessage(tables []*fmsg.PbKvSnapshotLeaseForTable) []SnapshotLease {
	var result []SnapshotLease
	for _, table := range tables {
		for _, bucket := range table.GetBucketSnapshots() {
			partitionID := int64(-1)
			if bucket.PartitionId != nil {
				partitionID = bucket.GetPartitionId()
			}
			result = append(result, SnapshotLease{
				TableID: table.GetTableId(), PartitionID: partitionID,
				Bucket: bucket.GetBucketId(), SnapshotID: bucket.GetSnapshotId(),
			})
		}
	}
	return result
}

type FileSystemSecurityToken struct {
	Schema         string
	Token          []byte
	ExpiresAt      time.Time
	AdditionalInfo map[string]string
}

func (c *Client) FileSystemSecurityToken(ctx context.Context) (FileSystemSecurityToken, error) {
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

type LakeBucketSnapshot struct {
	PartitionID   int64
	PartitionName string
	Bucket        int32
	LogOffset     int64
}

type LakeSnapshot struct {
	TableID, SnapshotID int64
	Buckets             []LakeBucketSnapshot
}

func (c *Client) LakeSnapshot(
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

type TableStats struct {
	Bucket      int32
	RowCount    int64
	PartitionID int64
	Err         error
}

func (c *Client) TableStats(
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
