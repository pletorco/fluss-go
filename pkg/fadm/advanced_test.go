package fadm

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func advancedACLResponse(t *testing.T, message any, response fmsg.Response, acl ACLBinding) {
	t.Helper()
	switch message := message.(type) {
	case *fmsg.CreateAclsRequest:
		if len(message.GetAcl()) != 2 || message.GetAcl()[0].GetPrincipalName() != "alice" {
			t.Fatalf("CreateAcls request = %#v", message)
		}
		response.Message().(*fmsg.CreateAclsResponse).AclRes = []*fmsg.PbCreateAclRespInfo{
			{Acl: message.GetAcl()[0]},
			{
				Acl: message.GetAcl()[1], ErrorCode: proto.Int32(int32(fmsg.ErrorCodeNotLeaderOrFollower)),
				ErrorMessage: proto.String("moved"),
			},
		}
	case *fmsg.ListAclsRequest:
		if message.GetAclFilter().GetResourceName() != "db" {
			t.Fatalf("ListAcls request = %#v", message)
		}
		response.Message().(*fmsg.ListAclsResponse).Acl = []*fmsg.PbAclInfo{acl.message()}
	case *fmsg.DropAclsRequest:
		if len(message.GetAclFilter()) != 1 {
			t.Fatalf("DropAcls request = %#v", message)
		}
		response.Message().(*fmsg.DropAclsResponse).FilterResults = []*fmsg.PbDropAclsFilterResult{{
			MatchingAcls: []*fmsg.PbDropAclsMatchingAcl{{Acl: acl.message()}},
		}}
	}
}

func advancedConfigResponse(t *testing.T, message any, response fmsg.Response) {
	t.Helper()
	switch message := message.(type) {
	case *fmsg.DescribeClusterConfigsRequest:
		response.Message().(*fmsg.DescribeClusterConfigsResponse).Configs = []*fmsg.PbDescribeConfig{{
			ConfigKey: proto.String("cluster.config"), ConfigValue: proto.String("value"),
			ConfigSource: proto.String("dynamic"),
		}}
	case *fmsg.AlterClusterConfigsRequest:
		if len(message.GetAlterConfigs()) != 2 || message.GetAlterConfigs()[0].GetConfigValue() != "value" {
			t.Fatalf("AlterClusterConfigs request = %#v", message)
		}
	case *fmsg.AddServerTagRequest:
		if len(message.GetServerIds()) != 2 || message.GetServerTag() != ServerTagTemporaryOffline {
			t.Fatalf("AddServerTag request = %#v", message)
		}
	case *fmsg.RemoveServerTagRequest:
		if len(message.GetServerIds()) != 1 || message.GetServerTag() != ServerTagTemporaryOffline {
			t.Fatalf("RemoveServerTag request = %#v", message)
		}
	}
}

func advancedRebalanceResponse(
	t *testing.T,
	message any,
	response fmsg.Response,
	progressCalls *atomic.Int32,
) {
	t.Helper()
	switch message := message.(type) {
	case *fmsg.RebalanceRequest:
		if len(message.GetGoals()) != 2 {
			t.Fatalf("Rebalance request = %#v", message)
		}
		response.Message().(*fmsg.RebalanceResponse).RebalanceId = proto.String("rebalance-1")
	case *fmsg.ListRebalanceProgressRequest:
		call := progressCalls.Add(1)
		if message.GetRebalanceId() != "rebalance-1" {
			t.Fatalf("ListRebalanceProgress request = %#v", message)
		}
		status := int32(1)
		if call == 2 {
			status = 0
		}
		item := response.Message().(*fmsg.ListRebalanceProgressResponse)
		item.RebalanceId, item.RebalanceStatus = proto.String("rebalance-1"), proto.Int32(status)
		item.TableProgress = []*fmsg.PbRebalanceProgressForTable{{
			TableId: proto.Int64(12),
			BucketsProgress: []*fmsg.PbRebalanceProgressForBucket{{
				RebalanceStatus: proto.Int32(status),
				RebalancePlan: &fmsg.PbRebalancePlanForBucket{
					PartitionId: proto.Int64(4), BucketId: proto.Int32(2),
					OriginalLeader: proto.Int32(1), NewLeader: proto.Int32(3),
					OriginalReplicas: []int32{1, 2}, NewReplicas: []int32{2, 3},
				},
			}},
		}}
	case *fmsg.CancelRebalanceRequest:
		if message.GetRebalanceId() != "rebalance-1" {
			t.Fatalf("CancelRebalance request = %#v", message)
		}
	}
}

func advancedProducerResponse(t *testing.T, message any, response fmsg.Response) {
	t.Helper()
	switch message := message.(type) {
	case *fmsg.RegisterProducerOffsetsRequest:
		if message.GetProducerId() != "producer-1" || message.GetTtlMs() != 5000 ||
			len(message.GetTableOffsets()) != 1 || len(message.GetTableOffsets()[0].GetBucketOffsets()) != 2 {
			t.Fatalf("RegisterProducerOffsets request = %#v", message)
		}
		response.Message().(*fmsg.RegisterProducerOffsetsResponse).Result = proto.Int32(0)
	case *fmsg.GetProducerOffsetsRequest:
		if message.GetProducerId() != "producer-1" {
			t.Fatalf("GetProducerOffsets request = %#v", message)
		}
		result := response.Message().(*fmsg.GetProducerOffsetsResponse)
		result.ProducerId, result.ExpirationTime = proto.String("producer-1"), proto.Int64(2000)
		result.TableOffsets = []*fmsg.PbProducerTableOffsets{{
			TableId: proto.Int64(12),
			BucketOffsets: []*fmsg.PbBucketOffset{
				{BucketId: proto.Int32(0), LogEndOffset: proto.Int64(10)},
				{PartitionId: proto.Int64(4), BucketId: proto.Int32(1), LogEndOffset: proto.Int64(20)},
			},
		}}
	case *fmsg.DeleteProducerOffsetsRequest:
		if message.GetProducerId() != "producer-1" {
			t.Fatalf("DeleteProducerOffsets request = %#v", message)
		}
	}
}

func advancedSnapshotResponse(t *testing.T, message any, response fmsg.Response) {
	t.Helper()
	switch message := message.(type) {
	case *fmsg.GetLatestKvSnapshotsRequest:
		latestSnapshotResponse(t, message, response)
	case *fmsg.GetKvSnapshotMetadataRequest:
		snapshotMetadataResponse(t, message, response)
	case *fmsg.AcquireKvSnapshotLeaseRequest:
		acquireKVSnapshotLeaseResponse(t, message, response)
	case *fmsg.ReleaseKvSnapshotLeaseRequest:
		if message.GetLeaseId() != "lease-1" || len(message.GetBucketsToRelease()) != 2 ||
			message.GetBucketsToRelease()[0].GetPartitionId() != 4 {
			t.Fatalf("ReleaseKvSnapshotLease request = %#v", message)
		}
	case *fmsg.DropKvSnapshotLeaseRequest:
		if message.GetLeaseId() != "lease-1" {
			t.Fatalf("DropKvSnapshotLease request = %#v", message)
		}
	}
}

func latestSnapshotResponse(
	t *testing.T,
	message *fmsg.GetLatestKvSnapshotsRequest,
	response fmsg.Response,
) {
	t.Helper()
	if message.GetTablePath().GetTableName() != "users" || message.GetPartitionName() != "region=kr" {
		t.Fatalf("GetLatestKvSnapshots request = %#v", message)
	}
	latest := response.Message().(*fmsg.GetLatestKvSnapshotsResponse)
	latest.TableId, latest.PartitionId = proto.Int64(12), proto.Int64(4)
	latest.LatestSnapshots = []*fmsg.PbKvSnapshot{
		{BucketId: proto.Int32(0), SnapshotId: proto.Int64(30), LogOffset: proto.Int64(40)},
		{BucketId: proto.Int32(1)},
	}
}

func snapshotMetadataResponse(
	t *testing.T,
	message *fmsg.GetKvSnapshotMetadataRequest,
	response fmsg.Response,
) {
	t.Helper()
	if message.GetTableId() != 12 || message.GetPartitionId() != 4 ||
		message.GetBucketId() != 0 || message.GetSnapshotId() != 30 {
		t.Fatalf("GetKvSnapshotMetadata request = %#v", message)
	}
	metadata := response.Message().(*fmsg.GetKvSnapshotMetadataResponse)
	metadata.LogOffset = proto.Int64(40)
	metadata.SnapshotFiles = []*fmsg.PbRemotePathAndLocalFile{{
		RemotePath: proto.String("s3://bucket/1"), LocalFileName: proto.String("1.sst"),
	}}
}

func acquireKVSnapshotLeaseResponse(
	t *testing.T,
	message *fmsg.AcquireKvSnapshotLeaseRequest,
	response fmsg.Response,
) {
	t.Helper()
	if message.GetLeaseId() != "lease-1" || message.GetLeaseDurationMs() != 3000 {
		t.Fatalf("AcquireKvSnapshotLease request = %#v", message)
	}
	if len(message.GetSnapshotsToLease()) == 0 {
		return
	}
	if len(message.GetSnapshotsToLease()) != 2 ||
		len(message.GetSnapshotsToLease()[0].GetBucketSnapshots()) != 2 {
		t.Fatalf("AcquireKvSnapshotLease snapshots = %#v", message.GetSnapshotsToLease())
	}
	response.Message().(*fmsg.AcquireKvSnapshotLeaseResponse).UnavailableSnapshots =
		[]*fmsg.PbKvSnapshotLeaseForTable{{
			TableId: proto.Int64(13),
			BucketSnapshots: []*fmsg.PbKvSnapshotLeaseForBucket{{
				BucketId: proto.Int32(2), SnapshotId: proto.Int64(32),
			}},
		}}
}

func advancedStorageResponse(t *testing.T, message any, response fmsg.Response, tokenBytes []byte) {
	t.Helper()
	switch message := message.(type) {
	case *fmsg.GetFileSystemSecurityTokenRequest:
		token := response.Message().(*fmsg.GetFileSystemSecurityTokenResponse)
		token.Schema, token.Token, token.ExpirationTime = proto.String("hadoop"), tokenBytes, proto.Int64(3000)
		token.AdditionInfo = []*fmsg.PbKeyValue{{
			Key: proto.String("service"), Value: proto.String("filesystem"),
		}}
	case *fmsg.GetLakeSnapshotRequest:
		if message.GetTablePath().GetTableName() != "users" || message.GetSnapshotId() != 44 || message.GetReadable() {
			t.Fatalf("GetLakeSnapshot request = %#v", message)
		}
		snapshot := response.Message().(*fmsg.GetLakeSnapshotResponse)
		snapshot.TableId, snapshot.SnapshotId = proto.Int64(12), proto.Int64(44)
		snapshot.BucketSnapshots = []*fmsg.PbLakeSnapshotForBucket{
			{
				PartitionId: proto.Int64(4), PartitionName: proto.String("region=kr"),
				BucketId: proto.Int32(0), LogOffset: proto.Int64(50),
			},
			{BucketId: proto.Int32(1), LogOffset: proto.Int64(60)},
		}
	}
}

func advancedCoordinator(
	t *testing.T,
	seen map[fmsg.APIKey]int,
	acl ACLBinding,
	progressCalls *atomic.Int32,
	tokenBytes []byte,
) func(context.Context, fmsg.Request) (fmsg.Response, error) {
	return func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		seen[request.APIKey()]++
		response, err := fmsg.NewResponse(request.APIKey(), request.Version())
		if err != nil {
			t.Fatal(err)
		}
		message := request.(*fmsg.MessageRequest).Message()
		switch request.APIKey() {
		case fmsg.APIKeyCreateAcls, fmsg.APIKeyListAcls, fmsg.APIKeyDropAcls:
			advancedACLResponse(t, message, response, acl)
		case fmsg.APIKeyDescribeClusterConfigs, fmsg.APIKeyAlterClusterConfigs,
			fmsg.APIKeyAddServerTag, fmsg.APIKeyRemoveServerTag:
			advancedConfigResponse(t, message, response)
		case fmsg.APIKeyRebalance, fmsg.APIKeyListRebalanceProgress, fmsg.APIKeyCancelRebalance:
			advancedRebalanceResponse(t, message, response, progressCalls)
		case fmsg.APIKeyRegisterProducerOffsets, fmsg.APIKeyGetProducerOffsets, fmsg.APIKeyDeleteProducerOffsets:
			advancedProducerResponse(t, message, response)
		case fmsg.APIKeyGetLatestKvSnapshots, fmsg.APIKeyGetKvSnapshotMetadata,
			fmsg.APIKeyAcquireKvSnapshotLease, fmsg.APIKeyReleaseKvSnapshotLease, fmsg.APIKeyDropKvSnapshotLease:
			advancedSnapshotResponse(t, message, response)
		case fmsg.APIKeyGetFilesystemSecurityToken, fmsg.APIKeyGetLakeSnapshot:
			advancedStorageResponse(t, message, response, tokenBytes)
		default:
			t.Fatalf("unexpected coordinator request %T", message)
		}
		return response, nil
	}
}

func advancedStatsBucket(
	t *testing.T,
	physical fgo.PhysicalTablePath,
) func(context.Context, fgo.PhysicalTablePath, int32, fmsg.Request) (fmsg.Response, error) {
	return func(
		_ context.Context,
		gotPath fgo.PhysicalTablePath,
		bucket int32,
		request fmsg.Request,
	) (fmsg.Response, error) {
		if gotPath != physical || request.APIKey() != fmsg.APIKeyGetTableStats {
			t.Fatalf("table stats route = %#v, %d, %d", gotPath, bucket, request.APIKey())
		}
		message := request.(*fmsg.MessageRequest).Message().(*fmsg.GetTableStatsRequest)
		if message.GetTableId() != 12 || message.GetBucketsReq()[0].GetPartitionId() != 4 {
			t.Fatalf("GetTableStats request = %#v", message)
		}
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		item := &fmsg.PbTableStatsRespForBucket{
			PartitionId: proto.Int64(4), BucketId: proto.Int32(bucket),
			RowCount: proto.Int64(int64(bucket + 100)),
		}
		if bucket == 1 {
			item.ErrorCode = proto.Int32(int32(fmsg.ErrorCodeNotLeaderOrFollower))
			item.ErrorMessage = proto.String("moved")
		}
		response.Message().(*fmsg.GetTableStatsResponse).BucketsResp = []*fmsg.PbTableStatsRespForBucket{item}
		return response, nil
	}
}

func TestAdvancedAdminOperations(t *testing.T) {
	path := fgo.TablePath{Database: "db", Table: "users"}
	physical := fgo.PhysicalTablePath{TablePath: path, Partition: "region=kr"}
	acl := ACLBinding{
		ResourceName: "db", ResourceType: ACLResourceDatabase, PrincipalName: "alice",
		PrincipalType: ACLPrincipalUser, Host: ACLWildcardHost,
		Operation: ACLOperationDrop, Permission: ACLPermissionAllow,
	}
	filterName := "db"
	filter := ACLBindingFilter{
		ResourceName: &filterName, ResourceType: ACLResourceDatabase,
		Operation: ACLOperationDrop, Permission: ACLPermissionAllow,
	}
	var progressCalls atomic.Int32
	seen := make(map[fmsg.APIKey]int)
	tokenBytes := []byte("secret-token")
	fake := &fakeRequester{
		coordinator: advancedCoordinator(t, seen, acl, &progressCalls, tokenBytes),
		bucket:      advancedStatsBucket(t, physical),
	}
	client := newClient(fake)
	ctx := context.Background()

	exerciseACLAndConfig(t, ctx, client, acl, filter)
	exerciseRebalance(t, ctx, client, &progressCalls)
	exerciseProducerOffsets(t, ctx, client)
	exerciseSnapshots(t, ctx, client, path)
	exerciseStorageAndStats(t, ctx, client, path, physical, tokenBytes)
	if len(seen) != 20 {
		t.Fatalf("advanced coordinator APIs seen = %#v", seen)
	}
}

func exerciseACLAndConfig(t *testing.T, ctx context.Context, client *Client, acl ACLBinding, filter ACLBindingFilter) {
	t.Helper()
	created, err := client.CreateACLs(ctx, acl, acl)
	if err != nil || len(created) != 2 || created[0].Err != nil ||
		!errors.Is(created[1].Err, fgo.ErrMetadata) {
		t.Fatalf("CreateACLs() = %#v, %v", created, err)
	}
	listed, err := client.ListACLs(ctx, filter)
	if err != nil || len(listed) != 1 || listed[0] != acl {
		t.Fatalf("ListACLs() = %#v, %v", listed, err)
	}
	dropped, err := client.DropACLs(ctx, filter)
	if err != nil || len(dropped) != 1 || len(dropped[0].Matches) != 1 ||
		dropped[0].Matches[0].Binding != acl {
		t.Fatalf("DropACLs() = %#v, %v", dropped, err)
	}
	configs, err := client.DescribeClusterConfigs(ctx)
	if err != nil || len(configs) != 1 || configs[0].Source != "dynamic" {
		t.Fatalf("DescribeClusterConfigs() = %#v, %v", configs, err)
	}
	value := "value"
	if err := client.AlterClusterConfigs(ctx,
		AlterConfig{Key: "cluster.config", Value: &value, Op: ConfigSet},
		AlterConfig{Key: "cluster.list", Op: ConfigSubtract},
	); err != nil {
		t.Fatal(err)
	}
	if err := client.AddServerTag(ctx, []int32{1, 2}, ServerTagTemporaryOffline); err != nil {
		t.Fatal(err)
	}
	if err := client.RemoveServerTag(ctx, []int32{1}, ServerTagTemporaryOffline); err != nil {
		t.Fatal(err)
	}
}

func exerciseRebalance(t *testing.T, ctx context.Context, client *Client, progressCalls *atomic.Int32) {
	t.Helper()
	id, err := client.Rebalance(ctx, 1, 2)
	if err != nil || id != "rebalance-1" {
		t.Fatalf("Rebalance() = %q, %v", id, err)
	}
	progress, err := client.ListRebalanceProgress(ctx, id)
	if err != nil || progress.Status != 1 || progress.Tables[0].Buckets[0].NewLeader != 3 {
		t.Fatalf("ListRebalanceProgress() = %#v, %v", progress, err)
	}
	waited, err := client.WaitRebalance(ctx, id, time.Millisecond)
	if err != nil || waited.Status != 1 || progressCalls.Load() != 3 {
		t.Fatalf("WaitRebalance() = %#v, %v, calls=%d", waited, err, progressCalls.Load())
	}
	if err := client.CancelRebalance(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func exerciseProducerOffsets(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()
	tables := []ProducerTableOffsets{{
		TableID: 12,
		Offsets: []ProducerBucketOffset{
			{PartitionID: -1, Bucket: 0, Offset: 10},
			{PartitionID: 4, Bucket: 1, Offset: 20},
		},
	}}
	registered, err := client.RegisterProducerOffsets(ctx, "producer-1", tables, 5*time.Second)
	if err != nil || !registered {
		t.Fatalf("RegisterProducerOffsets() = %v, %v", registered, err)
	}
	offsets, err := client.GetProducerOffsets(ctx, "producer-1")
	if err != nil || offsets.ExpiresAt.UnixMilli() != 2000 ||
		offsets.Tables[0].Offsets[0].PartitionID != -1 || offsets.Tables[0].Offsets[1].PartitionID != 4 {
		t.Fatalf("GetProducerOffsets() = %#v, %v", offsets, err)
	}
	if err := client.DeleteProducerOffsets(ctx, "producer-1"); err != nil {
		t.Fatal(err)
	}
}

func exerciseSnapshots(t *testing.T, ctx context.Context, client *Client, path fgo.TablePath) {
	t.Helper()
	latest, err := client.GetLatestKVSnapshots(ctx, path, "region=kr")
	if err != nil || latest.PartitionID != 4 || !latest.Snapshots[0].Available || latest.Snapshots[1].Available {
		t.Fatalf("GetLatestKVSnapshots() = %#v, %v", latest, err)
	}
	metadata, err := client.GetKVSnapshotMetadata(ctx, 12, 4, 0, 30)
	if err != nil || metadata.LogOffset != 40 || metadata.Files[0].LocalName != "1.sst" {
		t.Fatalf("GetKVSnapshotMetadata() = %#v, %v", metadata, err)
	}
	leases := []KVSnapshotLease{
		{TableID: 12, PartitionID: 4, Bucket: 0, SnapshotID: 30},
		{TableID: 12, PartitionID: -1, Bucket: 1, SnapshotID: 31},
		{TableID: 13, PartitionID: -1, Bucket: 2, SnapshotID: 32},
	}
	unavailable, err := client.AcquireKVSnapshotLease(ctx, "lease-1", 3*time.Second, leases)
	if err != nil || len(unavailable) != 1 || unavailable[0].PartitionID != -1 {
		t.Fatalf("AcquireKVSnapshotLease() = %#v, %v", unavailable, err)
	}
	if err := client.RenewKVSnapshotLease(ctx, "lease-1", 3*time.Second); err != nil {
		t.Fatalf("RenewKVSnapshotLease() = %v", err)
	}
	if err := client.ReleaseKVSnapshotLease(ctx, "lease-1", []fgo.TableBucket{
		{TableID: 12, PartitionID: 4, BucketID: 0},
		{TableID: 12, PartitionID: -1, BucketID: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := client.DropKVSnapshotLease(ctx, "lease-1"); err != nil {
		t.Fatal(err)
	}
}

func exerciseStorageAndStats(
	t *testing.T,
	ctx context.Context,
	client *Client,
	path fgo.TablePath,
	physical fgo.PhysicalTablePath,
	tokenBytes []byte,
) {
	t.Helper()
	token, err := client.GetFileSystemSecurityToken(ctx)
	if err != nil || token.Schema != "hadoop" || token.AdditionalInfo["service"] != "filesystem" {
		t.Fatalf("FileSystemSecurityToken() = %#v, %v", token, err)
	}
	token.Token[0] = 'X'
	if tokenBytes[0] != 's' {
		t.Fatal("security token aliases response memory")
	}
	snapshotID := int64(44)
	lake, err := client.GetLakeSnapshot(ctx, path, snapshotID)
	snapshotID = 45
	if err != nil || lake.SnapshotID != 44 || lake.Buckets[0].PartitionID != 4 ||
		lake.Buckets[1].PartitionID != -1 {
		t.Fatalf("GetLakeSnapshot() = %#v, %v", lake, err)
	}
	stats := client.GetTableStats(ctx, fgo.Table{ID: 12}, physical, 4, []int32{0, 1})
	if len(stats) != 2 || stats[0].RowCount != 100 || stats[0].Err != nil ||
		!errors.Is(stats[1].Err, fgo.ErrMetadata) {
		t.Fatalf("GetTableStats() = %#v", stats)
	}
}

func TestAdvancedAdminValidation(t *testing.T) {
	never := errors.New("request should not be sent")
	fake := &fakeRequester{
		coordinator: func(context.Context, fmsg.Request) (fmsg.Response, error) { return nil, never },
		bucket: func(context.Context, fgo.PhysicalTablePath, int32, fmsg.Request) (fmsg.Response, error) {
			return nil, never
		},
	}
	client := newClient(fake)
	ctx := context.Background()
	path := fgo.TablePath{Database: "db", Table: "t"}
	validACL := ACLBinding{
		ResourceName: "db", ResourceType: ACLResourceDatabase, PrincipalName: "alice",
		PrincipalType: ACLPrincipalUser, Host: ACLWildcardHost,
		Operation: ACLOperationRead, Permission: ACLPermissionAllow,
	}
	validLease := KVSnapshotLease{TableID: 1, PartitionID: -1, Bucket: 0, SnapshotID: 1}
	checks := map[string]func() error{
		"create ACLBinding empty": func() error { _, err := client.CreateACLs(ctx); return err },
		"create ACLBinding invalid": func() error {
			invalid := validACL
			invalid.Host = ""
			_, err := client.CreateACLs(ctx, invalid)
			return err
		},
		"list ACLBinding": func() error {
			_, err := client.ListACLs(ctx, ACLBindingFilter{ResourceType: -1})
			return err
		},
		"drop ACLBinding empty": func() error { _, err := client.DropACLs(ctx); return err },
		"drop ACLBinding invalid": func() error {
			_, err := client.DropACLs(ctx, ACLBindingFilter{Operation: -1})
			return err
		},
		"alter config empty": func() error { return client.AlterClusterConfigs(ctx) },
		"alter config invalid": func() error {
			return client.AlterClusterConfigs(ctx, AlterConfig{Op: ConfigSet})
		},
		"add tag empty":    func() error { return client.AddServerTag(ctx, nil, 1) },
		"add tag negative": func() error { return client.AddServerTag(ctx, []int32{-1}, 1) },
		"add tag unknown":  func() error { return client.AddServerTag(ctx, []int32{1}, 2) },
		"remove tag":       func() error { return client.RemoveServerTag(ctx, []int32{1}, -1) },
		"rebalance empty": func() error {
			_, err := client.Rebalance(ctx)
			return err
		},
		"progress empty": func() error { _, err := client.ListRebalanceProgress(ctx, ""); return err },
		"wait interval": func() error {
			_, err := client.WaitRebalance(ctx, "id", 0)
			return err
		},
		"cancel empty": func() error { return client.CancelRebalance(ctx, "") },
		"register empty": func() error {
			_, err := client.RegisterProducerOffsets(ctx, "", nil, 0)
			return err
		},
		"register table": func() error {
			_, err := client.RegisterProducerOffsets(ctx, "p", []ProducerTableOffsets{{TableID: -1}}, time.Second)
			return err
		},
		"register bucket": func() error {
			_, err := client.RegisterProducerOffsets(ctx, "p", []ProducerTableOffsets{{
				TableID: 1, Offsets: []ProducerBucketOffset{{PartitionID: -2}},
			}}, time.Second)
			return err
		},
		"get producer":    func() error { _, err := client.GetProducerOffsets(ctx, ""); return err },
		"delete producer": func() error { return client.DeleteProducerOffsets(ctx, "") },
		"latest snapshot": func() error {
			_, err := client.GetLatestKVSnapshots(ctx, fgo.TablePath{}, "")
			return err
		},
		"snapshot metadata": func() error {
			_, err := client.GetKVSnapshotMetadata(ctx, -1, -2, -1, -1)
			return err
		},
		"acquire empty": func() error {
			_, err := client.AcquireKVSnapshotLease(ctx, "", 0, nil)
			return err
		},
		"acquire invalid": func() error {
			invalid := validLease
			invalid.SnapshotID = -1
			_, err := client.AcquireKVSnapshotLease(ctx, "lease", time.Second, []KVSnapshotLease{invalid})
			return err
		},
		"renew empty": func() error {
			return client.RenewKVSnapshotLease(ctx, "", 0)
		},
		"release empty": func() error { return client.ReleaseKVSnapshotLease(ctx, "", nil) },
		"release invalid": func() error {
			return client.ReleaseKVSnapshotLease(ctx, "lease", []fgo.TableBucket{{TableID: -1}})
		},
		"drop empty": func() error { return client.DropKVSnapshotLease(ctx, "") },
		"lake path":  func() error { _, err := client.GetLatestLakeSnapshot(ctx, fgo.TablePath{}); return err },
		"lake ID": func() error {
			id := int64(-1)
			_, err := client.GetLakeSnapshot(ctx, path, id)
			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); !errors.Is(err, fgo.ErrInvalidConfig) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	stats := client.GetTableStats(ctx, fgo.Table{ID: -1}, fgo.PhysicalTablePath{}, -2, []int32{-1})
	if len(stats) != 1 || !errors.Is(stats[0].Err, fgo.ErrInvalidConfig) {
		t.Fatalf("invalid GetTableStats() = %#v", stats)
	}
}

func TestAdvancedAdminErrorsAndCancellation(t *testing.T) {
	ctx := context.Background()
	t.Run("count mismatches", func(t *testing.T) {
		fake := &fakeRequester{coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			return response, nil
		}}
		client := newClient(fake)
		acl := ACLBinding{
			ResourceName: "r", ResourceType: ACLResourceDatabase, PrincipalName: "p",
			PrincipalType: ACLPrincipalUser, Host: ACLWildcardHost,
			Operation: ACLOperationRead, Permission: ACLPermissionAllow,
		}
		if _, err := client.CreateACLs(ctx, acl); !errors.Is(err, fgo.ErrValidation) {
			t.Fatalf("CreateACLs error = %v", err)
		}
		filter := ACLBindingFilter{
			ResourceType: ACLResourceAny,
			Operation:    ACLOperationAny,
			Permission:   ACLPermissionAny,
		}
		if _, err := client.DropACLs(ctx, filter); !errors.Is(err, fgo.ErrValidation) {
			t.Fatalf("DropACLs error = %v", err)
		}
	})
	t.Run("empty rebalance ID", func(t *testing.T) {
		fake := &fakeRequester{coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			return response, nil
		}}
		if _, err := newClient(fake).Rebalance(ctx, 1); !errors.Is(err, fgo.ErrValidation) {
			t.Fatalf("Rebalance error = %v", err)
		}
	})
	t.Run("wait cancellation", func(t *testing.T) {
		fake := &fakeRequester{coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			message := response.Message().(*fmsg.ListRebalanceProgressResponse)
			message.RebalanceId, message.RebalanceStatus = proto.String("id"), proto.Int32(0)
			return response, nil
		}}
		waitCtx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := newClient(fake).WaitRebalance(waitCtx, "id", time.Hour); !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitRebalance error = %v", err)
		}
	})
	t.Run("request and stats errors", func(t *testing.T) {
		sentinel := errors.New("network")
		fake := &fakeRequester{
			coordinator: func(context.Context, fmsg.Request) (fmsg.Response, error) { return nil, sentinel },
			bucket: func(context.Context, fgo.PhysicalTablePath, int32, fmsg.Request) (fmsg.Response, error) {
				return nil, sentinel
			},
		}
		if _, err := newClient(fake).DescribeClusterConfigs(ctx); !errors.Is(err, sentinel) {
			t.Fatalf("DescribeClusterConfigs error = %v", err)
		}
		stats := newClient(fake).GetTableStats(ctx, fgo.Table{ID: 1}, fgo.PhysicalTablePath{}, -1, []int32{0})
		if !errors.Is(stats[0].Err, sentinel) {
			t.Fatalf("TableStats error = %v", stats[0].Err)
		}
	})
	t.Run("omitted stats", func(t *testing.T) {
		fake := &fakeRequester{bucket: func(
			context.Context, fgo.PhysicalTablePath, int32, fmsg.Request,
		) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(fmsg.APIKeyGetTableStats, 0)
			return response, nil
		}}
		stats := newClient(fake).GetTableStats(ctx, fgo.Table{ID: 1}, fgo.PhysicalTablePath{}, -1, []int32{0})
		if !errors.Is(stats[0].Err, fgo.ErrValidation) || !strings.Contains(stats[0].Err.Error(), "bucket 0") {
			t.Fatalf("TableStats error = %v", stats[0].Err)
		}
	})
}
