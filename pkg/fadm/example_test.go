package fadm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func ExampleNew() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	databases, err := admin.ListDatabaseSummaries(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, database := range databases {
		log.Printf("%s: %d tables", database.Name, database.TableCount)
	}
}

func ExampleClient_CreateDatabase() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	err = admin.CreateDatabase(ctx, "analytics", fadm.DatabaseDescriptor{
		Comment: "application analytics",
		Properties: map[string]string{
			"owner": "data-platform",
		},
	}, true)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_CreateACLs() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	results, err := admin.CreateACLs(ctx, fadm.ACLBinding{
		ResourceName:  "analytics",
		ResourceType:  fadm.ACLResourceDatabase,
		PrincipalName: "alice",
		PrincipalType: fadm.ACLPrincipalUser,
		Host:          fadm.ACLWildcardHost,
		Operation:     fadm.ACLOperationDescribe,
		Permission:    fadm.ACLPermissionAllow,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			log.Printf("create ACL binding for %s: %v", result.Binding.PrincipalName, result.Err)
		}
	}
}

func ExampleClient_ListACLs() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	acls, err := admin.ListACLs(ctx, fadm.ACLBindingFilter{
		ResourceType: fadm.ACLResourceAny,
		Operation:    fadm.ACLOperationAny,
		Permission:   fadm.ACLPermissionAny,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, acl := range acls {
		log.Printf("%s:%s may perform %d on %s",
			acl.PrincipalType,
			acl.PrincipalName,
			acl.Operation,
			acl.ResourceName,
		)
	}
}

func ExampleClient_DropACLs() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	resourceName := "analytics"
	principalName := "alice"
	principalType := fadm.ACLPrincipalUser
	results, err := admin.DropACLs(ctx, fadm.ACLBindingFilter{
		ResourceName:  &resourceName,
		ResourceType:  fadm.ACLResourceDatabase,
		PrincipalName: &principalName,
		PrincipalType: &principalType,
		Operation:     fadm.ACLOperationAny,
		Permission:    fadm.ACLPermissionAny,
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			log.Printf("drop ACLs: %v", result.Err)
		}
	}
}

func ExampleClient_CreateTable() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	err = admin.CreateTable(
		ctx,
		fgo.TablePath{Database: "production", Table: "customers"},
		fadm.TableDescriptor{
			Schema: fgo.Schema{
				Columns: []fgo.Column{
					{Name: "customer_id", Type: fgo.BigIntType},
					{Name: "name", Type: fgo.StringType},
				},
				PrimaryKey: []string{"customer_id"},
			},
			BucketCount: 3,
		},
		true,
	)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_ListOffsets() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	table, err := client.GetTable(
		ctx,
		fgo.TablePath{Database: "production", Table: "events"},
	)
	if err != nil {
		log.Fatal(err)
	}
	results := admin.ListOffsets(
		ctx,
		table,
		fgo.PhysicalTablePath{TablePath: table.Path},
		-1,
		[]int32{0, 1, 2},
		fgo.Latest(),
	)
	for _, result := range results {
		if result.Err != nil {
			log.Printf("bucket %d: %v", result.Bucket, result.Err)
			continue
		}
		log.Printf("bucket %d offset %d", result.Bucket, result.Offset)
	}
}

func ExampleClient_AlterClusterConfigs() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	value := "3"
	err = admin.AlterClusterConfigs(ctx, fadm.AlterConfig{
		Key:   "table.default-bucket-number",
		Value: &value,
		Op:    fadm.ConfigSet,
	})
	if err != nil {
		log.Fatal(err)
	}

	configs, err := admin.DescribeClusterConfigs(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, config := range configs {
		if config.Key == "table.default-bucket-number" {
			log.Printf("%s=%s (%s)", config.Key, config.Value, config.Source)
		}
	}
}

func ExampleClient_WaitRebalance() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	goalIDs := []int32{1} // Goal IDs are defined by the target Fluss 0.9.1 cluster.
	rebalanceID, err := admin.Rebalance(ctx, goalIDs...)
	if err != nil {
		log.Fatal(err)
	}

	waitCtx, stopWaiting := context.WithTimeout(ctx, 10*time.Minute)
	progress, err := admin.WaitRebalance(waitCtx, rebalanceID, 2*time.Second)
	stopWaiting()
	if err != nil {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
		cancelErr := admin.CancelRebalance(cleanupCtx, rebalanceID)
		cancelCleanup()
		if cancelErr != nil {
			log.Printf("cancel rebalance %s: %v", rebalanceID, cancelErr)
		}
		log.Fatal(err)
	}
	log.Printf("rebalance %s reached terminal status %d", progress.ID, progress.Status)
}

func ExampleClient_RegisterProducerOffsets() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	const producerID = "analytics-materializer"
	registered, err := admin.RegisterProducerOffsets(ctx, producerID, []fadm.ProducerTableOffsets{{
		TableID: 42,
		Offsets: []fadm.ProducerBucketOffset{
			{PartitionID: -1, Bucket: 0, Offset: 120},
			{PartitionID: -1, Bucket: 1, Offset: 98},
		},
	}}, 15*time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	if !registered {
		log.Fatal("producer offset registration was rejected")
	}

	offsets, err := admin.GetProducerOffsets(ctx, producerID)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("producer %s expires at %s", offsets.ProducerID, offsets.ExpiresAt)
}

func ExampleClient_AcquireKVSnapshotLease() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	latest, err := admin.GetLatestKVSnapshots(
		ctx,
		fgo.TablePath{Database: "production", Table: "customers"},
		"",
	)
	if err != nil {
		log.Fatal(err)
	}
	requested := exampleKVSnapshotLeases(latest)
	if len(requested) == 0 {
		log.Print("no snapshots are currently available")
		return
	}

	const leaseID = "snapshot-reader-20260731-01"
	unavailable, err := admin.AcquireKVSnapshotLease(ctx, leaseID, 10*time.Minute, requested)
	if err != nil {
		log.Fatal(err)
	}
	defer exampleDropKVSnapshotLease(admin, leaseID)
	if err := admin.RenewKVSnapshotLease(ctx, leaseID, 10*time.Minute); err != nil {
		log.Fatal(err)
	}

	unavailableSet := make(map[fadm.KVSnapshotLease]struct{}, len(unavailable))
	for _, snapshot := range unavailable {
		unavailableSet[snapshot] = struct{}{}
	}
	for _, leased := range requested {
		if _, failed := unavailableSet[leased]; failed {
			log.Printf("snapshot for bucket %d was not leased", leased.Bucket)
			continue
		}
		exampleLogSnapshotMetadata(ctx, admin, leased)
	}
}

func exampleKVSnapshotLeases(latest fadm.KVSnapshots) []fadm.KVSnapshotLease {
	requested := make([]fadm.KVSnapshotLease, 0, len(latest.Snapshots))
	for _, snapshot := range latest.Snapshots {
		if !snapshot.Available {
			continue
		}
		requested = append(requested, fadm.KVSnapshotLease{
			TableID: latest.TableID, PartitionID: latest.PartitionID,
			Bucket: snapshot.Bucket, SnapshotID: snapshot.SnapshotID,
		})
	}
	return requested
}

func exampleDropKVSnapshotLease(admin *fadm.Client, leaseID string) {
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCleanup()
	if err := admin.DropKVSnapshotLease(cleanupCtx, leaseID); err != nil {
		log.Printf("drop snapshot lease %s: %v", leaseID, err)
	}
}

func exampleLogSnapshotMetadata(ctx context.Context, admin *fadm.Client, leased fadm.KVSnapshotLease) {
	metadata, err := admin.GetKVSnapshotMetadata(
		ctx,
		leased.TableID,
		leased.PartitionID,
		leased.Bucket,
		leased.SnapshotID,
	)
	if err != nil {
		log.Printf("bucket %d metadata: %v", leased.Bucket, err)
		return
	}
	log.Printf("bucket %d has %d snapshot files", leased.Bucket, len(metadata.Files))
}

func ExampleClient_GetTableStats() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := fadm.New(client)
	if err != nil {
		log.Fatal(err)
	}
	table, err := client.GetTable(
		ctx,
		fgo.TablePath{Database: "production", Table: "customers"},
	)
	if err != nil {
		log.Fatal(err)
	}
	results := admin.GetTableStats(
		ctx,
		table,
		fgo.PhysicalTablePath{TablePath: table.Path},
		-1,
		[]int32{0, 1, 2},
	)
	for _, result := range results {
		if result.Err != nil {
			log.Printf("bucket %d stats: %v", result.Bucket, result.Err)
			continue
		}
		log.Printf("bucket %d rows: %d", result.Bucket, result.RowCount)
	}
}

func ExampleTableDescriptor_JSON() {
	definition := fadm.TableDescriptor{
		Schema: fgo.Schema{
			Columns: []fgo.Column{
				{Name: "customer_id", Type: fgo.BigIntType},
				{Name: "name", Type: fgo.StringType},
			},
			PrimaryKey: []string{"customer_id"},
		},
		BucketCount: 3,
	}
	encoded, err := definition.JSON()
	fmt.Println(err == nil, json.Valid(encoded))
	// Output:
	// true true
}
