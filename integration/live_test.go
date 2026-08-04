//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pletorco/fluss-go/internal/transport"
	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

const (
	expectedVersion = "0.9.1-incubating"
	expectedCommit  = "6bf969f71af8d6f9cc37383ab89ae46a58b0e227"
	expectedImage   = "apache/fluss@sha256:65f5513b33dde10ace4f8adb3956f17226a2a1e2663f92b3096e4769b0ee1d1c"
)

func TestFluss091Integration(t *testing.T) {
	requireEnvironment(t)
	plainAddress := net.JoinHostPort("127.0.0.1", env("FLUSS_PLAIN_COORDINATOR_PORT", "19123"))
	saslAddress := net.JoinHostPort("127.0.0.1", env("FLUSS_SASL_COORDINATOR_PORT", "19223"))
	plainSeeds := []string{"127.0.0.1:1", plainAddress}

	t.Run("protocol negotiation and server type", func(t *testing.T) {
		verifyProtocolRegistry(t, protocolEndpoints(plainAddress))
	})

	client := openClient(t, plainSeeds)
	defer client.Close()

	t.Run("plaintext bootstrap failover", func(t *testing.T) {
		testPlaintextBootstrap(t, client)
	})

	t.Run("SASL PLAIN", func(t *testing.T) {
		testSASLPlain(t, saslAddress)
	})

	t.Run("ACL authorization", func(t *testing.T) {
		testACLAuthorization(t, saslAddress)
	})

	t.Run("filesystem security token refresh", func(t *testing.T) {
		testManagedFileSystemToken(t, plainAddress)
	})

	database := fmt.Sprintf("go_it_%d", time.Now().UnixNano())
	logPath := fgo.TablePath{Database: database, Table: "events"}
	kvPath := fgo.TablePath{Database: database, Table: "users"}
	admin, err := fadm.New(client)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("canceled request isolation", func(t *testing.T) {
		testCanceledRequestIsolation(t, admin)
	})
	if err := admin.CreateDatabase(context.Background(), database, fadm.DatabaseDescriptor{
		Comment: "fluss-go live integration",
	}, false); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := admin.DropDatabase(context.Background(), database, true, true); err != nil {
			t.Errorf("cleanup database: %v", err)
		}
	}()

	t.Run("catalog", func(t *testing.T) {
		testCatalog(t, admin, database, logPath, kvPath)
	})

	t.Run("dynamic partition creation and routing", func(t *testing.T) {
		testDynamicPartition(t, plainAddress, admin, database)
	})

	t.Run("append and scan", func(t *testing.T) {
		testLogData(t, client, logPath)
	})

	t.Run("schema evolution", func(t *testing.T) {
		testSchemaEvolution(t, client, admin, database)
	})

	t.Run("log write formats", func(t *testing.T) {
		testLogFormats(t, client, admin, database)
	})

	t.Run("upsert delete lookup and prefix lookup", func(t *testing.T) {
		testKVData(t, client, kvPath)
	})

	t.Run("typed data API", func(t *testing.T) {
		testTypedData(t, client, logPath, kvPath)
	})

	t.Run("partial upsert and merge modes", func(t *testing.T) {
		testPartialUpsert(t, client, admin, database)
	})

	t.Run("snapshot and advanced admin APIs", func(t *testing.T) {
		testAdvancedAdmin(t, client, admin, logPath, kvPath)
	})

	t.Run("post-failure data I/O", func(t *testing.T) {
		testLeaderFailover(t, client, admin, logPath, kvPath)
	})

	t.Run("coordinator restart recovery", func(t *testing.T) {
		testCoordinatorRecovery(t, client, logPath)
	})
}

func testCanceledRequestIsolation(t *testing.T, admin *fadm.Client) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := admin.GetServerNodes(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetServerNodes() canceled error = %v", err)
	}
	database := fmt.Sprintf("cancel_repro_%d", time.Now().UnixNano())
	if err := admin.CreateDatabase(
		context.Background(), database, fadm.DatabaseDescriptor{}, false,
	); err != nil {
		t.Fatalf("CreateDatabase() after canceled request = %v", err)
	}
	if err := admin.DropDatabase(
		context.Background(), database, true, true,
	); err != nil {
		t.Fatalf("DropDatabase() cancellation reproduction cleanup = %v", err)
	}
}

func protocolEndpoints(plainAddress string) map[string]fgo.ServerType {
	return map[string]fgo.ServerType{
		plainAddress: fgo.Coordinator,
		net.JoinHostPort("127.0.0.1", env("FLUSS_PLAIN_TABLET_0_PORT", "19124")): fgo.TabletServer,
		net.JoinHostPort("127.0.0.1", env("FLUSS_PLAIN_TABLET_1_PORT", "19125")): fgo.TabletServer,
		net.JoinHostPort("127.0.0.1", env("FLUSS_PLAIN_TABLET_2_PORT", "19126")): fgo.TabletServer,
	}
}

func testPlaintextBootstrap(t *testing.T, client *fgo.Client) {
	t.Helper()
	admin, err := fadm.New(client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.DatabaseExists(context.Background(), "fluss_go_missing"); err != nil {
		t.Fatal(err)
	}
}

func testSASLPlain(t *testing.T, address string) {
	t.Helper()
	username, password := os.Getenv("FLUSS_SASL_USERNAME"), os.Getenv("FLUSS_SASL_PASSWORD")
	authenticated := openClient(t, []string{address}, fgo.WithAuthenticator(fgo.SASLPlainAuthenticator(username, password)))
	defer authenticated.Close()
	admin, err := fadm.New(authenticated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ListDatabaseSummaries(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = fgo.Open(ctx,
		fgo.WithBootstrapServers(address),
		fgo.WithAuthenticator(fgo.SASLPlainAuthenticator(username, password+"-wrong")),
	)
	if !errors.Is(err, fgo.ErrAuthentication) || strings.Contains(fmt.Sprint(err), password) {
		t.Fatalf("invalid credential error = %v", err)
	}
}

func testACLAuthorization(t *testing.T, address string) {
	t.Helper()
	ctx := context.Background()
	adminClient, admin := openAuthenticatedAdmin(
		t,
		address,
		os.Getenv("FLUSS_SASL_USERNAME"),
		os.Getenv("FLUSS_SASL_PASSWORD"),
	)
	defer adminClient.Close()

	username := os.Getenv("FLUSS_SASL_ACL_USERNAME")
	userClient, userAdmin := openAuthenticatedAdmin(
		t,
		address,
		username,
		os.Getenv("FLUSS_SASL_ACL_PASSWORD"),
	)
	defer userClient.Close()

	deniedBefore := fmt.Sprintf("acl_denied_before_%d", time.Now().UnixNano())
	requireDatabaseCreateAuthorization(t, ctx, userAdmin, deniedBefore, false)

	acl := fadm.ACLBinding{
		ResourceName:  fadm.ACLClusterResourceName,
		ResourceType:  fadm.ACLResourceCluster,
		PrincipalName: username,
		PrincipalType: fadm.ACLPrincipalUser,
		Host:          fadm.ACLWildcardHost,
		Operation:     fadm.ACLOperationCreate,
		Permission:    fadm.ACLPermissionAllow,
	}
	requireCreatedACL(t, ctx, admin, acl)

	resourceName := acl.ResourceName
	principalName := acl.PrincipalName
	principalType := acl.PrincipalType
	host := acl.Host
	filter := fadm.ACLBindingFilter{
		ResourceName:  &resourceName,
		ResourceType:  acl.ResourceType,
		PrincipalName: &principalName,
		PrincipalType: &principalType,
		Host:          &host,
		Operation:     acl.Operation,
		Permission:    acl.Permission,
	}
	dropped := false
	defer func() {
		if dropped {
			return
		}
		if _, err := admin.DropACLs(context.Background(), filter); err != nil {
			t.Errorf("cleanup ACL: %v", err)
		}
	}()

	requireListedACL(t, ctx, admin, filter, acl)

	allowed := fmt.Sprintf("acl_allowed_%d", time.Now().UnixNano())
	requireDatabaseCreateAuthorization(t, ctx, userAdmin, allowed, true)
	defer func() {
		if err := admin.DropDatabase(context.Background(), allowed, true, true); err != nil {
			t.Errorf("cleanup ACL database: %v", err)
		}
	}()

	requireDroppedACL(t, ctx, admin, filter, acl)
	dropped = true

	deniedAfter := fmt.Sprintf("acl_denied_after_%d", time.Now().UnixNano())
	requireDatabaseCreateAuthorization(t, ctx, userAdmin, deniedAfter, false)
}

func openAuthenticatedAdmin(
	t *testing.T,
	address, username, password string,
) (*fgo.Client, *fadm.Client) {
	t.Helper()
	client := openClient(
		t,
		[]string{address},
		fgo.WithAuthenticator(fgo.SASLPlainAuthenticator(username, password)),
	)
	admin, err := fadm.New(client)
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	return client, admin
}

func requireDatabaseCreateAuthorization(
	t *testing.T,
	ctx context.Context,
	admin *fadm.Client,
	name string,
	allowed bool,
) {
	t.Helper()
	err := admin.CreateDatabase(ctx, name, fadm.DatabaseDescriptor{}, false)
	if allowed && err != nil {
		t.Fatalf("CreateDatabase(%q) with ACL: %v", name, err)
	}
	if !allowed && !errors.Is(err, fgo.ErrAuthorization) {
		t.Fatalf("CreateDatabase(%q) authorization error = %v", name, err)
	}
}

func requireCreatedACL(t *testing.T, ctx context.Context, admin *fadm.Client, acl fadm.ACLBinding) {
	t.Helper()
	created, err := admin.CreateACLs(ctx, acl)
	if err != nil || len(created) != 1 || created[0].Err != nil || created[0].Binding != acl {
		t.Fatalf("CreateACLs() = %#v, %v", created, err)
	}
}

func requireListedACL(
	t *testing.T,
	ctx context.Context,
	admin *fadm.Client,
	filter fadm.ACLBindingFilter,
	acl fadm.ACLBinding,
) {
	t.Helper()
	listed, err := admin.ListACLs(ctx, filter)
	if err != nil || len(listed) != 1 || listed[0] != acl {
		t.Fatalf("ListACLs() = %#v, %v", listed, err)
	}
}

func requireDroppedACL(
	t *testing.T,
	ctx context.Context,
	admin *fadm.Client,
	filter fadm.ACLBindingFilter,
	acl fadm.ACLBinding,
) {
	t.Helper()
	results, err := admin.DropACLs(ctx, filter)
	if err != nil || len(results) != 1 || results[0].Err != nil ||
		len(results[0].Matches) != 1 || results[0].Matches[0].Err != nil ||
		results[0].Matches[0].Binding != acl {
		t.Fatalf("DropACLs() = %#v, %v", results, err)
	}
}

func testManagedFileSystemToken(t *testing.T, address string) {
	t.Helper()
	client := openClient(
		t, []string{address},
		fgo.WithFileSystemSecurityTokenRefresh(fgo.FileSystemSecurityTokenRefreshConfig{}),
	)
	defer client.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		token, ok := client.CurrentFileSystemSecurityToken()
		if ok {
			if token.Schema == "" || !strings.Contains(fmt.Sprintf("%#v", token), "[REDACTED]") {
				t.Fatalf("managed filesystem token = %#v", token)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("managed filesystem token was not published")
}

func testCatalog(t *testing.T, admin *fadm.Client, database string, logPath, kvPath fgo.TablePath) {
	t.Helper()
	createTables(t, admin, logPath, kvPath)
	tables, err := admin.ListTables(context.Background(), database)
	if err != nil || len(tables) != 2 {
		t.Fatalf("ListTables() = %#v, %v", tables, err)
	}
	const leaseID = "fluss-go-integration-renewal"
	if err := admin.RenewKVSnapshotLease(
		context.Background(), leaseID, time.Minute,
	); err != nil {
		t.Fatalf("RenewKVSnapshotLease() = %v", err)
	}
	if err := admin.DropKVSnapshotLease(context.Background(), leaseID); err != nil {
		t.Fatalf("DropKVSnapshotLease() = %v", err)
	}
}

func requireEnvironment(t *testing.T) {
	t.Helper()
	for key, want := range map[string]string{
		"FLUSS_INTEGRATION": "1",
		"FLUSS_VERSION":     expectedVersion,
		"FLUSS_COMMIT":      expectedCommit,
		"FLUSS_IMAGE":       expectedImage,
	} {
		if got := os.Getenv(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	upstream, err := os.ReadFile(filepath.Join("..", "third_party", "apache-fluss", "UPSTREAM.md"))
	if err != nil || !strings.Contains(string(upstream), expectedCommit) {
		t.Fatalf("pinned upstream metadata does not contain %s: %v", expectedCommit, err)
	}
}

func verifyProtocolRegistry(t *testing.T, endpoints map[string]fgo.ServerType) {
	t.Helper()
	server := make(map[fmsg.APIKey][2]int32)
	for address, expectedServerType := range endpoints {
		mergeVersions(server, endpointVersions(t, address, expectedServerType))
	}
	verifyLocalVersions(t, server)
}

func endpointVersions(t *testing.T, address string, expectedServerType fgo.ServerType) map[fmsg.APIKey][2]int32 {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rpc, err := transport.New(conn, transport.Config{})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	defer rpc.Close()
	request, err := fmsg.NewRequest(fmsg.APIKeyApiVersions, 0)
	if err != nil {
		t.Fatal(err)
	}
	message := request.Message().(*fmsg.ApiVersionsRequest)
	message.ClientSoftwareName, message.ClientSoftwareVersion = proto.String("fluss-go-integration"), proto.String("0.1")
	response, err := rpc.Request(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	versions := response.Message().(*fmsg.ApiVersionsResponse)
	if got := fgo.ServerType(versions.GetServerType()); got != expectedServerType {
		t.Fatalf("%s server type = %d, want %d", address, got, expectedServerType)
	}
	result := make(map[fmsg.APIKey][2]int32)
	for _, version := range versions.GetApiVersions() {
		result[fmsg.APIKey(version.GetApiKey())] = [2]int32{version.GetMinVersion(), version.GetMaxVersion()}
	}
	return result
}

func mergeVersions(target, source map[fmsg.APIKey][2]int32) {
	for key, versions := range source {
		target[key] = versions
	}
}

func verifyLocalVersions(t *testing.T, server map[fmsg.APIKey][2]int32) {
	t.Helper()
	for _, local := range fmsg.APIKeys() {
		if !local.Public {
			continue
		}
		got, ok := server[local.Key]
		if !ok || got != [2]int32{int32(local.MinVersion), int32(local.MaxVersion)} {
			t.Fatalf("API %s versions = %v, present=%v, want [%d %d]", local.Name, got, ok, local.MinVersion, local.MaxVersion)
		}
	}
}

func openClient(t *testing.T, seeds []string, options ...fgo.Option) *fgo.Client {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		all := []fgo.Option{fgo.WithBootstrapServers(seeds...), fgo.WithDialTimeout(3 * time.Second)}
		all = append(all, options...)
		client, err := fgo.Open(ctx, all...)
		cancel()
		if err == nil {
			return client
		}
		lastErr = err
		time.Sleep(time.Second)
	}
	t.Fatalf("open Fluss client: %v", lastErr)
	return nil
}

func createTables(t *testing.T, admin *fadm.Client, logPath, kvPath fgo.TablePath) {
	t.Helper()
	logSchema := fgo.Schema{Columns: []fgo.Column{
		{Name: "id", Type: fgo.IntType},
		{Name: "message", Type: fgo.StringType},
	}}
	if err := admin.CreateTable(context.Background(), logPath, fadm.TableDescriptor{
		Schema: logSchema, BucketCount: 3,
	}, false); err != nil {
		t.Fatalf("create log table: %v", err)
	}
	kvSchema := fgo.Schema{
		Columns: []fgo.Column{
			{Name: "tenant", Type: fgo.StringType},
			{Name: "id", Type: fgo.IntType},
			{Name: "name", Type: fgo.StringType, Nullable: true},
		},
		PrimaryKey: []string{"tenant", "id"},
		BucketKey:  []string{"tenant"},
	}
	if err := admin.CreateTable(context.Background(), kvPath, fadm.TableDescriptor{
		Schema: kvSchema, BucketCount: 3,
	}, false); err != nil {
		t.Fatalf("create KV table: %v", err)
	}
	waitForTableReady(t, admin, logPath)
	waitForTableReady(t, admin, kvPath)
}

func waitForTableReady(t *testing.T, admin *fadm.Client, path fgo.TablePath) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var last []fadm.OffsetResult
	for time.Now().Before(deadline) {
		table, err := admin.GetTableInfo(context.Background(), path)
		if err == nil {
			buckets := make([]int32, table.BucketCount)
			for index := range buckets {
				buckets[index] = int32(index)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			last = admin.ListOffsets(
				ctx, table, fgo.PhysicalTablePath{TablePath: path}, -1, buckets, fgo.Earliest(),
			)
			cancel()
			ready := len(last) == table.BucketCount
			for _, result := range last {
				ready = ready && result.Err == nil
			}
			if ready {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("table %s did not become ready: %#v", path, last)
}

func testLogData(t *testing.T, client *fgo.Client, path fgo.TablePath) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := client.NewAppendWriter(ctx, table, fgo.WithAppendLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	for index, message := range []string{"first", "second", "third"} {
		result := writer.Append(ctx, fgo.Row{int32(index + 1), message}).Await(ctx)
		if result.Err != nil {
			t.Fatalf("append %d: %v", index, result.Err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	scanner, err := client.NewLogScanner(ctx, table, fgo.Earliest(), fgo.WithScanLimits(1<<20, 1<<20, 1, 100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	found := make(map[int32]string)
	for len(found) < 3 && ctx.Err() == nil {
		result, err := scanner.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range result.Records {
			found[record.Record.Value[0].(int32)] = record.Record.Value[1].(string)
		}
		result.Release()
	}
	if found[1] != "first" || found[2] != "second" || found[3] != "third" {
		t.Fatalf("scanned rows = %#v", found)
	}
	testBoundedLogScan(t, ctx, client, table)
	testExplicitOffsetInsideBatch(t, ctx, client, table)
}

func testExplicitOffsetInsideBatch(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
) {
	t.Helper()
	const records = 20
	writer, err := client.NewAppendWriter(
		ctx,
		table,
		fgo.WithAppendLinger(time.Hour),
		fgo.WithAppendBucketAssignment(fgo.AssignmentSticky),
	)
	if err != nil {
		t.Fatal(err)
	}
	futures := make([]*fgo.WriteFuture, records)
	for index := range records {
		futures[index] = writer.Append(ctx, fgo.Row{int32(-2_000_000_000 + index), "offset-boundary"})
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	results := make([]fgo.WriteResult, records)
	for index := range futures {
		results[index] = futures[index].Await(ctx)
		if results[index].Err != nil || !results[index].OffsetKnown {
			t.Fatalf("batched append %d = %#v", index, results[index])
		}
		if index > 0 && (results[index].Bucket != results[0].Bucket ||
			results[index].BaseOffset != results[index-1].BaseOffset+1) {
			t.Fatalf("writes were not one contiguous bucket batch: %#v", results)
		}
	}

	middle := records / 2
	start := results[middle].BaseOffset
	scanner, err := client.NewLogScanner(
		ctx,
		table,
		fgo.AtOffset(start),
		fgo.WithScanRowLimit(int64(records-middle)),
		fgo.WithScanLimits(1<<20, 1<<20, 1, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	for bucket := int32(0); bucket < int32(table.BucketCount); bucket++ {
		if bucket != results[0].Bucket {
			scanner.Unsubscribe(bucket)
		}
	}
	var offsets []int64
	for !scanner.Done() && ctx.Err() == nil {
		result, pollErr := scanner.Poll(ctx)
		if pollErr != nil {
			t.Fatal(pollErr)
		}
		for _, record := range result.Records {
			if record.Record.Offset < start {
				result.Release()
				t.Fatalf("scan exposed offset %d before requested %d", record.Record.Offset, start)
			}
			offsets = append(offsets, record.Record.Offset)
		}
		result.Release()
	}
	if len(offsets) != records-middle || offsets[0] != start {
		t.Fatalf("explicit offset scan = %v, want %d records starting at %d", offsets, records-middle, start)
	}
}

func testSchemaEvolution(
	t *testing.T,
	client *fgo.Client,
	admin *fadm.Client,
	database string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "schema_evolution"}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema: fgo.Schema{Columns: []fgo.Column{
			{Name: "id", Type: fgo.IntType},
			{Name: "message", Type: fgo.StringType, Nullable: true},
		}},
		BucketCount: 1,
	}, false); err != nil {
		t.Fatal(err)
	}
	waitForTableReady(t, admin, path)
	oldTable, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	appendSchemaEvolutionRow(t, ctx, client, oldTable, fgo.Row{int32(1), "old"})
	if err := admin.AlterTable(ctx, path, fadm.AlterTable{Add: []fadm.AddColumn{{
		Name: "extra",
		Type: fgo.LogicalType{Root: "BIGINT", Nullable: true},
	}}}, false); err != nil {
		t.Fatal(err)
	}
	current := waitForSchemaChange(t, ctx, client, path, oldTable.SchemaID)
	appendSchemaEvolutionRow(t, ctx, client, current, fgo.Row{int32(2), "new", int64(9)})
	rows := scanSchemaEvolutionRows(t, ctx, client, current)
	if len(rows[1]) != 3 || rows[1][2] != nil || rows[2][2] != int64(9) {
		t.Fatalf("schema-evolved rows = %#v", rows)
	}
}

func appendSchemaEvolutionRow(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	row fgo.Row,
) {
	t.Helper()
	writer, err := client.NewAppendWriter(ctx, table, fgo.WithAppendLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	if result := writer.Append(ctx, row).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func scanSchemaEvolutionRows(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
) map[int32]fgo.Row {
	t.Helper()
	scanner, err := client.NewLogScanner(
		ctx,
		table,
		fgo.Earliest(),
		fgo.WithScanRowLimit(2),
		fgo.WithScanLimits(1<<20, 1<<20, 1, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	rows := make(map[int32]fgo.Row)
	for len(rows) < 2 && ctx.Err() == nil {
		result, pollErr := scanner.Poll(ctx)
		if pollErr != nil {
			t.Fatal(pollErr)
		}
		for _, record := range result.Records {
			rows[record.Record.Value[0].(int32)] = record.Record.Value
		}
		result.Release()
	}
	return rows
}

func waitForSchemaChange(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	path fgo.TablePath,
	previous int32,
) fgo.Table {
	t.Helper()
	for ctx.Err() == nil {
		table, err := client.GetTable(ctx, path)
		if err == nil && table.SchemaID != previous {
			return table
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("schema %s did not change from %d: %v", path, previous, ctx.Err())
	return fgo.Table{}
}

func testDynamicPartition(t *testing.T, address string, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	path := fgo.TablePath{Database: database, Table: "partitioned_events"}
	schema := fgo.Schema{
		Columns: []fgo.Column{
			{Name: "id", Type: fgo.IntType},
			{Name: "region", Type: fgo.StringType},
			{Name: "message", Type: fgo.StringType},
		},
		PartitionKey: []string{"region"},
	}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema: schema, BucketCount: 1,
	}, false); err != nil {
		t.Fatal(err)
	}
	client := openClient(t, []string{address}, fgo.WithDynamicPartitionCreation(
		fgo.DynamicPartitionCreationConfig{MetadataAttempts: 10, RetryBackoff: 100 * time.Millisecond},
	))
	defer client.Close()
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	spec := fgo.PartitionSpec{"region": "kr"}
	writer, err := client.NewAppendWriter(
		ctx, table, fgo.WithAppendPartitionSpec(table.Schema, spec), fgo.WithAppendLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result := writer.Append(ctx, fgo.Row{int32(1), "kr", "created"}).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	scanner, err := client.NewLogScanner(
		ctx, table, fgo.Earliest(),
		fgo.WithScanPartitionSpec(table.Schema, spec),
		fgo.WithScanLimits(1<<20, 1<<20, 1, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	for ctx.Err() == nil {
		result, err := scanner.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range result.Records {
			if record.Record.Value[2] == "created" {
				result.Release()
				return
			}
		}
		result.Release()
	}
	t.Fatal(ctx.Err())
}

func testBoundedLogScan(t *testing.T, ctx context.Context, client *fgo.Client, table fgo.Table) {
	t.Helper()
	scanner, err := client.NewLogScanner(
		ctx, table, fgo.Earliest(),
		fgo.WithScanLimits(1<<20, 1<<20, 1, 100*time.Millisecond),
		fgo.WithScanRowLimit(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	var rows int64
	for !scanner.Done() && ctx.Err() == nil {
		result, err := scanner.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rows += int64(len(result.Records))
		for _, batch := range result.ArrowBatches {
			rows += batch.Batch.Record.NumRows()
		}
		result.Release()
	}
	if rows != 2 {
		t.Fatalf("bounded scan returned %d rows, want 2", rows)
	}
	result, err := scanner.Poll(ctx)
	if err != nil || !result.Done || len(result.Records) != 0 || len(result.ArrowBatches) != 0 {
		t.Fatalf("terminal bounded poll = %#v, %v", result, err)
	}
}

func testLogFormats(
	t *testing.T,
	client *fgo.Client,
	admin *fadm.Client,
	database string,
) {
	t.Helper()
	schema := fgo.Schema{Columns: []fgo.Column{
		{Name: "id", Type: fgo.IntType},
		{Name: "message", Type: fgo.StringType, Nullable: true},
	}}
	for _, format := range []fgo.LogFormat{
		fgo.LogFormatCompacted,
		fgo.LogFormatIndexed,
		fgo.LogFormatArrow,
	} {
		testLogFormat(t, client, admin, database, schema, format)
	}
}

func testLogFormat(
	t *testing.T,
	client *fgo.Client,
	admin *fadm.Client,
	database string,
	schema fgo.Schema,
	format fgo.LogFormat,
) {
	t.Helper()
	path := fgo.TablePath{Database: database, Table: "format_" + string(format)}
	if err := admin.CreateTable(context.Background(), path, fadm.TableDescriptor{
		Schema: schema, BucketCount: 1,
		Properties: map[string]string{"table.log.format": strings.ToUpper(string(format))},
	}, false); err != nil {
		t.Fatalf("create %s table: %v", format, err)
	}
	waitForTableReady(t, admin, path)
	table, err := client.GetTable(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	options := []fgo.AppendWriterOption{
		fgo.WithAppendLogFormat(format),
		fgo.WithAppendLinger(0),
	}
	if format == fgo.LogFormatArrow {
		options = append(options, fgo.WithAppendArrowCompression(fgo.ArrowCompressionZSTD))
	}
	writer, err := client.NewAppendWriter(context.Background(), table, options...)
	if err != nil {
		t.Fatalf("create %s writer: %v", format, err)
	}
	if format == fgo.LogFormatArrow {
		appendArrowFormatRow(t, writer, table)
	} else {
		result := writer.Append(context.Background(), fgo.Row{int32(1), string(format)}).
			Await(context.Background())
		if result.Err != nil {
			t.Fatalf("append %s row: %v", format, result.Err)
		}
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertFormatRow(t, client, table, format)
	if format == fgo.LogFormatArrow {
		testDefaultFetchWithLargeArrowBatch(t, client, table)
	}
}

func appendArrowFormatRow(t *testing.T, writer *fgo.AppendWriter, table fgo.Table) {
	t.Helper()
	schema, err := table.Schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*array.Int32Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append("arrow")
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	result := writer.AppendArrow(context.Background(), 0, record, []fgo.ChangeType{fgo.Append}).
		Await(context.Background())
	if result.Err != nil {
		t.Fatalf("append Arrow row: %v", result.Err)
	}
}

func testDefaultFetchWithLargeArrowBatch(t *testing.T, client *fgo.Client, table fgo.Table) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	schema, err := table.Schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	value := strings.Repeat("x", 600<<10)
	for id := int32(2); id <= 3; id++ {
		builder.Field(0).(*array.Int32Builder).Append(id)
		builder.Field(1).(*array.StringBuilder).Append(value)
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()

	writer, err := client.NewAppendWriter(
		ctx, table,
		fgo.WithAppendLogFormat(fgo.LogFormatArrow),
		fgo.WithAppendLinger(0),
		fgo.WithAppendBatchLimits(4<<20, 10),
	)
	if err != nil {
		t.Fatal(err)
	}
	write := writer.AppendArrow(ctx, 0, record, []fgo.ChangeType{fgo.Append, fgo.Append}).Await(ctx)
	if write.Err != nil {
		t.Fatalf("append oversized Arrow batch: %v", write.Err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	scanner, err := client.NewLogScanner(
		ctx, table, fgo.Earliest(),
		fgo.WithScanLimits(16<<20, 1<<20, 1, 100*time.Millisecond),
		fgo.WithScanRowLimit(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	rows := int64(0)
	for !scanner.Done() && ctx.Err() == nil {
		result, pollErr := scanner.Poll(ctx)
		if pollErr != nil {
			t.Fatal(pollErr)
		}
		if len(result.BucketErrors) != 0 {
			result.Release()
			t.Fatalf("scan oversized Arrow batch: %#v", result.BucketErrors)
		}
		rows += int64(len(result.Records))
		for _, batch := range result.ArrowBatches {
			rows += batch.Batch.Record.NumRows()
		}
		result.Release()
	}
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	if rows != 3 {
		t.Fatalf("default 1 MiB bucket fetch returned %d rows, want 3", rows)
	}
}

func assertFormatRow(
	t *testing.T,
	client *fgo.Client,
	table fgo.Table,
	format fgo.LogFormat,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scanner, err := client.NewLogScanner(
		ctx, table, fgo.Earliest(),
		fgo.WithScanLimits(1<<20, 1<<20, 1, 100*time.Millisecond),
		fgo.WithScanRowLimit(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	result, err := scanner.Poll(ctx)
	if err != nil {
		t.Fatalf("scan %s row: %v", format, err)
	}
	defer result.Release()
	rows := len(result.Records)
	for _, batch := range result.ArrowBatches {
		rows += int(batch.Batch.Record.NumRows())
	}
	if rows != 1 || !result.Done {
		t.Fatalf("%s scan result = %#v", format, result)
	}
}

func testKVData(t *testing.T, client *fgo.Client, path fgo.TablePath) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := client.NewUpsertWriter(ctx, table, fgo.WithUpsertLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	rows := []fgo.Row{
		{"team-a", int32(1), "alice"},
		{"team-a", int32(2), "bob"},
		{"team-b", int32(1), "carol"},
	}
	upsertRows(t, ctx, writer, rows)
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	lookup, err := client.NewLookuper(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	testPointAndPrefixLookup(t, ctx, lookup)
	testCurrentBatchScan(t, ctx, client, table)
	deleteLookupRow(t, ctx, client, table, lookup)
	testConcurrentInsertLookup(t, ctx, client, table)
}

func testCurrentBatchScan(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
) {
	t.Helper()
	buckets, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{TablePath: table.Path})
	if err != nil {
		t.Fatal(err)
	}
	found := make(map[string]string)
	for _, bucket := range buckets {
		scanner, err := client.NewBatchScanner(
			ctx, table, bucket,
			fgo.WithBatchLimit(100), fgo.WithBatchProjection("tenant", "name"),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := scanner.Poll(ctx)
		if err != nil {
			_ = scanner.Close()
			t.Fatal(err)
		}
		for _, row := range result.Rows {
			found[row[1].(string)] = row[0].(string)
		}
		result.Release()
		if !result.Done {
			t.Fatalf("current batch scan for bucket %d is not complete", bucket.BucketID)
		}
		if err := scanner.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if found["alice"] != "team-a" || found["bob"] != "team-a" || found["carol"] != "team-b" {
		t.Fatalf("current batch rows = %#v", found)
	}
}

func upsertRows(t *testing.T, ctx context.Context, writer *fgo.UpsertWriter, rows []fgo.Row) {
	t.Helper()
	for index, row := range rows {
		if result := writer.Upsert(ctx, row).Await(ctx); result.Err != nil {
			t.Fatalf("upsert %d: %v", index, result.Err)
		}
	}
}

func testPointAndPrefixLookup(t *testing.T, ctx context.Context, lookup *fgo.Lookuper) {
	t.Helper()
	points := lookup.Lookup(ctx, fgo.PrimaryKey{"team-a", int32(1)}, fgo.PrimaryKey{"team-b", int32(1)})
	for index, point := range points {
		if point.Err != nil {
			t.Fatalf("point lookup %d for %#v: %v", index, point.Key, point.Err)
		}
	}
	if len(points) != 2 || points[0].Err != nil || !points[0].Found || points[0].Row[2] != "alice" ||
		points[1].Err != nil || !points[1].Found || points[1].Row[2] != "carol" {
		t.Fatalf("point lookup = %#v", points)
	}
	prefix := lookup.PrefixLookup(ctx, fgo.PrimaryKey{"team-a"})
	if len(prefix) != 1 || prefix[0].Err != nil || len(prefix[0].Rows) != 2 {
		t.Fatalf("prefix lookup = %#v", prefix)
	}
}

func deleteLookupRow(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	lookup *fgo.Lookuper,
) {
	t.Helper()
	deleteWriter, err := client.NewUpsertWriter(ctx, table, fgo.WithUpsertLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	if result := deleteWriter.Delete(ctx, fgo.PrimaryKey{"team-a", int32(1)}).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	if err := deleteWriter.Close(ctx); err != nil {
		t.Fatal(err)
	}
	deleted := lookup.Lookup(ctx, fgo.PrimaryKey{"team-a", int32(1)})
	if len(deleted) != 1 || !errors.Is(deleted[0].Err, fgo.ErrNotFound) || deleted[0].Found {
		t.Fatalf("deleted lookup = %#v", deleted)
	}
}

func testConcurrentInsertLookup(t *testing.T, ctx context.Context, client *fgo.Client, table fgo.Table) {
	t.Helper()
	lookup, err := client.NewLookuper(
		ctx, table, fgo.WithLookupInsertIfNotExists(10*time.Second, -1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	const callers = 12
	results := make(chan fgo.LookupResult, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- lookup.Lookup(ctx, fgo.PrimaryKey{"inserted", int32(7)})[0]
		}()
	}
	wait.Wait()
	close(results)
	for result := range results {
		if result.Err != nil || !result.Found || result.Row[0] != "inserted" ||
			result.Row[1] != int32(7) || result.Row[2] != nil {
			t.Fatalf("concurrent insert lookup = %#v", result)
		}
	}
}

type typedLogEvent struct {
	ID      int32
	Message string
}

type typedUser struct {
	Tenant string
	ID     int32
	Name   *string
}

type typedUserKey struct {
	Tenant string
	ID     *int32
}

func logEventCodec() fgo.Codec[typedLogEvent] {
	return fgo.CodecFuncs[typedLogEvent]{
		EncodeFunc: func(value typedLogEvent) (fgo.Row, error) {
			return fgo.Row{value.ID, value.Message}, nil
		},
		DecodeFunc: func(row fgo.Row) (typedLogEvent, error) {
			if len(row) != 2 {
				return typedLogEvent{}, fmt.Errorf("typed log row has %d fields", len(row))
			}
			id, idOK := row[0].(int32)
			message, messageOK := row[1].(string)
			if !idOK || !messageOK {
				return typedLogEvent{}, fmt.Errorf("typed log row has unexpected values %#v", row)
			}
			return typedLogEvent{ID: id, Message: message}, nil
		},
	}
}

func userCodec() fgo.Codec[typedUser] {
	return fgo.CodecFuncs[typedUser]{
		EncodeFunc: func(value typedUser) (fgo.Row, error) {
			var name any
			if value.Name != nil {
				name = *value.Name
			}
			return fgo.Row{value.Tenant, value.ID, name}, nil
		},
		DecodeFunc: func(row fgo.Row) (typedUser, error) {
			if len(row) != 3 {
				return typedUser{}, fmt.Errorf("typed user row has %d fields", len(row))
			}
			tenant, tenantOK := row[0].(string)
			id, idOK := row[1].(int32)
			if !tenantOK || !idOK {
				return typedUser{}, fmt.Errorf("typed user row has unexpected key %#v", row)
			}
			result := typedUser{Tenant: tenant, ID: id}
			if row[2] != nil {
				name, ok := row[2].(string)
				if !ok {
					return typedUser{}, fmt.Errorf("typed user row has unexpected name %#v", row[2])
				}
				result.Name = &name
			}
			return result, nil
		},
	}
}

func userKeyCodec() fgo.KeyCodec[typedUserKey] {
	return fgo.KeyCodecFunc[typedUserKey](func(key typedUserKey) (fgo.PrimaryKey, error) {
		if key.ID == nil {
			return fgo.PrimaryKey{key.Tenant}, nil
		}
		return fgo.PrimaryKey{key.Tenant, *key.ID}, nil
	})
}

func testTypedData(t *testing.T, client *fgo.Client, logPath, kvPath fgo.TablePath) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	testTypedLogData(t, ctx, client, logPath)
	kvTable, users := testTypedKVData(t, ctx, client, kvPath)
	testTypedBatchData(t, ctx, client, kvTable, kvPath, users)
}

func testTypedLogData(t *testing.T, ctx context.Context, client *fgo.Client, path fgo.TablePath) {
	t.Helper()
	logTable, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	scanner, err := fgo.NewTypedLogScanner(
		ctx,
		client,
		logTable,
		fgo.Latest(),
		logEventCodec(),
		fgo.WithScanRowLimit(2),
		fgo.WithScanLimits(1<<20, 1<<20, 1, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	written := writeTypedLogEvents(t, ctx, client, logTable)
	found := scanTypedLogEvents(t, ctx, scanner)
	for _, event := range written {
		if found[event.ID] != event {
			t.Fatalf("typed scanned event %d = %#v, want %#v", event.ID, found[event.ID], event)
		}
	}
}

func writeTypedLogEvents(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
) []typedLogEvent {
	t.Helper()
	writer, err := fgo.NewTypedAppendWriter(
		ctx, client, table, logEventCodec(), fgo.WithAppendLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	written := []typedLogEvent{{ID: 3001, Message: "typed-one"}, {ID: 3002, Message: "typed-two"}}
	for _, event := range written {
		if result := writer.Append(ctx, event).Await(ctx); result.Err != nil {
			t.Fatalf("typed append %#v: %v", event, result.Err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return written
}

func scanTypedLogEvents(
	t *testing.T,
	ctx context.Context,
	scanner *fgo.TypedLogScanner[typedLogEvent],
) map[int32]typedLogEvent {
	t.Helper()
	found := make(map[int32]typedLogEvent)
	for !scanner.Done() {
		result, pollErr := scanner.Poll(ctx)
		if pollErr != nil {
			t.Fatal(pollErr)
		}
		if len(result.BucketErrors) != 0 {
			t.Fatalf("typed scan bucket errors = %#v", result.BucketErrors)
		}
		for _, record := range result.Records {
			found[record.Value.ID] = record.Value
		}
	}
	return found
}

func testTypedKVData(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	path fgo.TablePath,
) (fgo.Table, []typedUser) {
	t.Helper()
	kvTable, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	firstName, secondName := "typed-alice", "typed-bob"
	users := []typedUser{
		{Tenant: "typed-team", ID: 1, Name: &firstName},
		{Tenant: "typed-team", ID: 2, Name: &secondName},
	}
	upsertWriter, err := fgo.NewTypedUpsertWriter(
		ctx, client, kvTable, userCodec(), userKeyCodec(), fgo.WithUpsertLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, user := range users {
		if result := upsertWriter.Upsert(ctx, user).Await(ctx); result.Err != nil {
			t.Fatalf("typed upsert %#v: %v", user, result.Err)
		}
	}
	if err := upsertWriter.Close(ctx); err != nil {
		t.Fatal(err)
	}
	lookup, err := fgo.NewTypedLookuper(ctx, client, kvTable, userCodec(), userKeyCodec())
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	id := int32(1)
	points := lookup.Lookup(ctx, typedUserKey{Tenant: "typed-team", ID: &id})
	if len(points) != 1 || points[0].Err != nil || !points[0].Found ||
		points[0].Value.Name == nil || *points[0].Value.Name != firstName {
		t.Fatalf("typed point lookup = %#v", points)
	}
	prefix := lookup.PrefixLookup(ctx, typedUserKey{Tenant: "typed-team"})
	if len(prefix) != 1 || prefix[0].Err != nil || len(prefix[0].Values) != 2 {
		t.Fatalf("typed prefix lookup = %#v", prefix)
	}
	return kvTable, users
}

func testTypedBatchData(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	path fgo.TablePath,
	users []typedUser,
) {
	t.Helper()
	buckets, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{TablePath: path})
	if err != nil {
		t.Fatal(err)
	}
	batchFound := make(map[int32]bool)
	for _, bucket := range buckets {
		for _, user := range scanTypedBatchBucket(t, ctx, client, table, bucket) {
			if user.Tenant == "typed-team" {
				batchFound[user.ID] = true
			}
		}
	}
	for _, user := range users {
		if !batchFound[user.ID] {
			t.Fatalf("typed batch rows = %#v", batchFound)
		}
	}
}

func scanTypedBatchBucket(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	bucket fgo.TableBucket,
) []typedUser {
	t.Helper()
	batch, err := fgo.NewTypedBatchScanner(
		ctx, client, table, bucket, userCodec(), fgo.WithBatchLimit(1000),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, pollErr := batch.Poll(ctx)
	closeErr := batch.Close()
	if pollErr != nil {
		t.Fatal(pollErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if !result.Done {
		t.Fatalf("typed batch bucket %d did not finish", bucket.BucketID)
	}
	return result.Values
}

func testPartialUpsert(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	testPartialUpdatePreservesFields(t, ctx, client, admin, database)
	testKVMergeModes(t, ctx, client, admin, database)
}

func testPartialUpdatePreservesFields(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	admin *fadm.Client,
	database string,
) {
	t.Helper()
	path := fgo.TablePath{Database: database, Table: "partial_users"}
	if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
		Schema: fgo.Schema{
			Columns: []fgo.Column{
				{Name: "tenant", Type: fgo.StringType},
				{Name: "id", Type: fgo.IntType},
				{Name: "name", Type: fgo.StringType, Nullable: true},
				{Name: "note", Type: fgo.StringType, Nullable: true},
			},
			PrimaryKey: []string{"tenant", "id"},
			BucketKey:  []string{"tenant"},
		},
		BucketCount: 3,
	}, false); err != nil {
		t.Fatal(err)
	}
	waitForTableReady(t, admin, path)
	table, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	writeKVOperation(t, ctx, client, table, []fgo.UpsertWriterOption{fgo.WithUpsertLinger(0)}, func(writer *fgo.UpsertWriter) *fgo.WriteFuture {
		return writer.Upsert(ctx, fgo.Row{"partial", int32(1), "before", "preserved"})
	})
	writeKVOperation(t, ctx, client, table, []fgo.UpsertWriterOption{fgo.WithUpsertLinger(0)}, func(writer *fgo.UpsertWriter) *fgo.WriteFuture {
		return writer.PartialUpsert(
			ctx, []string{"tenant", "id", "name"}, fgo.Row{"partial", int32(1), "after"},
		)
	})
	lookup, err := client.NewLookuper(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	row := lookup.Lookup(ctx, fgo.PrimaryKey{"partial", int32(1)})
	if len(row) != 1 || row[0].Err != nil || !row[0].Found ||
		row[0].Row[2] != "after" || row[0].Row[3] != "preserved" {
		t.Fatalf("partial upsert row = %#v", row)
	}
}

func testKVMergeModes(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	admin *fadm.Client,
	database string,
) {
	t.Helper()
	overwritePath := fgo.TablePath{Database: database, Table: "overwrite_users"}
	if err := admin.CreateTable(ctx, overwritePath, fadm.TableDescriptor{
		Schema: fgo.Schema{
			Columns: []fgo.Column{
				{Name: "tenant", Type: fgo.StringType},
				{Name: "id", Type: fgo.IntType},
				{Name: "name", Type: fgo.StringType, Nullable: true},
			},
			PrimaryKey: []string{"tenant", "id"},
			BucketKey:  []string{"tenant"},
		},
		BucketCount: 1,
		Properties:  map[string]string{"table.merge-engine": "FIRST_ROW"},
	}, false); err != nil {
		t.Fatal(err)
	}
	waitForTableReady(t, admin, overwritePath)
	overwriteTable, err := client.GetTable(ctx, overwritePath)
	if err != nil {
		t.Fatal(err)
	}
	writeOverwrite := func(mode fgo.MergeMode, name string) {
		writeKVOperation(t, ctx, client, overwriteTable, []fgo.UpsertWriterOption{
			fgo.WithUpsertLinger(0), fgo.WithUpsertMergeMode(mode),
		}, func(writer *fgo.UpsertWriter) *fgo.WriteFuture {
			return writer.Upsert(ctx, fgo.Row{"merge", int32(1), name})
		})
	}
	writeOverwrite(fgo.MergeModeDefault, "first")
	writeOverwrite(fgo.MergeModeDefault, "ignored")
	overwriteLookup, err := client.NewLookuper(ctx, overwriteTable)
	if err != nil {
		t.Fatal(err)
	}
	defer overwriteLookup.Close()
	merged := overwriteLookup.Lookup(ctx, fgo.PrimaryKey{"merge", int32(1)})
	if len(merged) != 1 || merged[0].Err != nil || merged[0].Row[2] != "first" {
		t.Fatalf("first-row merge result = %#v", merged)
	}
	writeOverwrite(fgo.MergeModeOverwrite, "overwrite")
	merged = overwriteLookup.Lookup(ctx, fgo.PrimaryKey{"merge", int32(1)})
	if len(merged) != 1 || merged[0].Err != nil || merged[0].Row[2] != "overwrite" {
		t.Fatalf("overwrite-mode row = %#v", merged)
	}
}

func writeKVOperation(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	options []fgo.UpsertWriterOption,
	operation func(*fgo.UpsertWriter) *fgo.WriteFuture,
) {
	t.Helper()
	writer, err := client.NewUpsertWriter(ctx, table, options...)
	if err != nil {
		t.Fatal(err)
	}
	if result := operation(writer).Await(ctx); result.Err != nil {
		t.Fatal(result.Err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func testAdvancedAdmin(
	t *testing.T,
	client *fgo.Client,
	admin *fadm.Client,
	logPath, kvPath fgo.TablePath,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	testConfigEntryAndNodes(t, ctx, admin)
	testProducerOffsets(t, ctx, client, admin, logPath)
	testTableStatistics(t, ctx, client, admin, kvPath)
	testKVSnapshotLease(t, ctx, client, admin, kvPath)
	testLakeSnapshotError(t, ctx, admin, kvPath)
}

func testConfigEntryAndNodes(t *testing.T, ctx context.Context, admin *fadm.Client) {
	t.Helper()
	configs, err := admin.DescribeClusterConfigs(ctx)
	if err != nil || len(configs) == 0 {
		t.Fatalf("DescribeClusterConfigs() = %#v, %v", configs, err)
	}
	nodes, err := admin.GetServerNodes(ctx)
	if err != nil || len(nodes) != 4 {
		t.Fatalf("GetServerNodes() = %#v, %v", nodes, err)
	}
}

func testProducerOffsets(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	admin *fadm.Client,
	path fgo.TablePath,
) {
	t.Helper()
	logTable, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	bucketIDs := make([]int32, logTable.BucketCount)
	for index := range bucketIDs {
		bucketIDs[index] = int32(index)
	}
	offsets := admin.ListOffsets(
		ctx, logTable, fgo.PhysicalTablePath{TablePath: path}, -1, bucketIDs, fgo.Latest(),
	)
	producerTable := fadm.ProducerTableOffsets{TableID: logTable.ID}
	for _, offset := range offsets {
		if offset.Err != nil {
			t.Fatalf("producer offset bucket %d: %v", offset.Bucket, offset.Err)
		}
		producerTable.Offsets = append(producerTable.Offsets, fadm.ProducerBucketOffset{
			PartitionID: -1, Bucket: offset.Bucket, Offset: offset.Offset,
		})
	}
	producerID := fmt.Sprintf("fluss-go-integration-%d", time.Now().UnixNano())
	deleted := false
	defer func() {
		if !deleted {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if err := admin.DeleteProducerOffsets(cleanupCtx, producerID); err != nil {
				t.Errorf("delete producer offsets cleanup: %v", err)
			}
		}
	}()
	registered, err := admin.RegisterProducerOffsets(ctx, producerID, []fadm.ProducerTableOffsets{producerTable}, time.Minute)
	if err != nil || !registered {
		t.Fatalf("RegisterProducerOffsets() = %v, %v", registered, err)
	}
	stored, err := admin.GetProducerOffsets(ctx, producerID)
	if err != nil || stored.ProducerID != producerID || len(stored.Tables) != 1 ||
		len(stored.Tables[0].Offsets) != len(producerTable.Offsets) || !stored.ExpiresAt.After(time.Now()) {
		t.Fatalf("GetProducerOffsets() = %#v, %v", stored, err)
	}
	if err := admin.DeleteProducerOffsets(ctx, producerID); err != nil {
		t.Fatal(err)
	}
	deleted = true
}

func testTableStatistics(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	admin *fadm.Client,
	path fgo.TablePath,
) {
	t.Helper()
	kvTable, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	bucketIDs := make([]int32, kvTable.BucketCount)
	for index := range bucketIDs {
		bucketIDs[index] = int32(index)
	}
	stats := admin.GetTableStats(
		ctx, kvTable, fgo.PhysicalTablePath{TablePath: path}, -1, bucketIDs,
	)
	if len(stats) != len(bucketIDs) {
		t.Fatalf("GetTableStats() count = %d, want %d", len(stats), len(bucketIDs))
	}
	for index, stat := range stats {
		if stat.Err != nil || stat.Bucket != bucketIDs[index] || stat.RowCount < 0 {
			t.Fatalf("GetTableStats()[%d] = %#v", index, stat)
		}
	}
}

func testKVSnapshotLease(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	admin *fadm.Client,
	path fgo.TablePath,
) {
	t.Helper()
	kvTable, err := client.GetTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	latest := waitForKVSnapshots(t, ctx, admin, path, kvTable.BucketCount)
	leases := availableKVSnapshotLeases(latest)
	if len(leases) == 0 {
		t.Fatalf("available snapshot leases = %#v", leases)
	}
	metadata, err := admin.GetKVSnapshotMetadata(
		ctx, leases[0].TableID, leases[0].PartitionID, leases[0].Bucket, leases[0].SnapshotID,
	)
	if err != nil || metadata.LogOffset < 0 || len(metadata.Files) == 0 {
		t.Fatalf("GetKVSnapshotMetadata() = %#v, %v", metadata, err)
	}
	testKVSnapshotLeaseLifecycle(t, ctx, admin, leases)
}

func availableKVSnapshotLeases(latest fadm.KVSnapshots) []fadm.KVSnapshotLease {
	var leases []fadm.KVSnapshotLease
	for _, snapshot := range latest.Snapshots {
		if !snapshot.Available {
			continue
		}
		leases = append(leases, fadm.KVSnapshotLease{
			TableID: latest.TableID, PartitionID: latest.PartitionID,
			Bucket: snapshot.Bucket, SnapshotID: snapshot.SnapshotID,
		})
	}
	return leases
}

func testKVSnapshotLeaseLifecycle(
	t *testing.T,
	ctx context.Context,
	admin *fadm.Client,
	leases []fadm.KVSnapshotLease,
) {
	t.Helper()
	leaseID := fmt.Sprintf("fluss-go-integration-lease-%d", time.Now().UnixNano())
	dropped := false
	defer func() {
		if !dropped {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if err := admin.DropKVSnapshotLease(cleanupCtx, leaseID); err != nil {
				t.Errorf("drop snapshot lease cleanup: %v", err)
			}
		}
	}()
	unavailable, err := admin.AcquireKVSnapshotLease(ctx, leaseID, time.Minute, leases)
	if err != nil || len(unavailable) != 0 {
		t.Fatalf("AcquireKVSnapshotLease() unavailable = %#v, %v", unavailable, err)
	}
	if err := admin.RenewKVSnapshotLease(ctx, leaseID, time.Minute); err != nil {
		t.Fatal(err)
	}
	bucket := fgo.TableBucket{
		TableID: leases[0].TableID, PartitionID: leases[0].PartitionID, BucketID: leases[0].Bucket,
		Leader: fgo.ServerNode{Address: "lease-only", ServerType: fgo.TabletServer},
	}
	if err := admin.ReleaseKVSnapshotLease(ctx, leaseID, []fgo.TableBucket{bucket}); err != nil {
		t.Fatal(err)
	}
	if err := admin.DropKVSnapshotLease(ctx, leaseID); err != nil {
		t.Fatal(err)
	}
	dropped = true
}

func testLakeSnapshotError(
	t *testing.T,
	ctx context.Context,
	admin *fadm.Client,
	path fgo.TablePath,
) {
	t.Helper()
	if _, err := admin.GetReadableLakeSnapshot(ctx, path); err == nil {
		t.Fatal("GetReadableLakeSnapshot() without lake storage error = nil")
	} else if !errors.Is(err, fgo.ErrStorage) && !errors.Is(err, fgo.ErrValidation) {
		t.Fatalf("GetReadableLakeSnapshot() unsupported environment error = %v", err)
	}
}

func waitForKVSnapshots(
	t *testing.T,
	ctx context.Context,
	admin *fadm.Client,
	path fgo.TablePath,
	bucketCount int,
) fadm.KVSnapshots {
	t.Helper()
	var latest fadm.KVSnapshots
	err := waitForCondition(ctx, 500*time.Millisecond, func() (bool, error) {
		candidate, snapshotErr := admin.GetLatestKVSnapshots(ctx, path, "")
		if snapshotErr != nil {
			return false, nil
		}
		latest = candidate
		if len(candidate.Snapshots) != bucketCount {
			return false, nil
		}
		available := false
		for _, snapshot := range candidate.Snapshots {
			available = available || snapshot.Available
		}
		return available, nil
	})
	if err != nil {
		t.Fatalf("KV snapshots did not become available: %#v: %v", latest, err)
	}
	return latest
}

type expectedLogWrite struct {
	id     int32
	offset int64
}

func testLeaderFailover(
	t *testing.T,
	client *fgo.Client,
	admin *fadm.Client,
	logPath, kvPath fgo.TablePath,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	logTable, err := client.GetTable(ctx, logPath)
	if err != nil {
		t.Fatal(err)
	}
	kvTable, err := client.GetTable(ctx, kvPath)
	if err != nil {
		t.Fatal(err)
	}

	expectedLog := appendFailoverLogRows(t, ctx, client, logTable, 1000, false)
	kvKeys := seedFailoverKVRows(t, ctx, client, kvTable)
	before := metadataLeaders(t, client, logPath)
	var stopped int32 = -1
	for _, leader := range before {
		stopped = leader
		break
	}
	if stopped < 0 || stopped > 2 {
		t.Fatalf("initial leaders = %#v", before)
	}
	service := fmt.Sprintf("plaintext-tablet-%d", stopped)
	compose(t, "stop", service)
	restarted := false
	defer func() {
		if !restarted {
			compose(t, "up", "--detach", "--wait", "--wait-timeout", "120", service)
		}
	}()

	if err := waitForCondition(ctx, 500*time.Millisecond, func() (bool, error) {
		leaders, metadataErr := tryMetadataLeaders(client, logPath)
		return metadataErr == nil && leadersMoved(before, leaders, stopped), nil
	}); err != nil {
		t.Fatalf("leaders did not move away from tablet %d; before=%#v: %v", stopped, before, err)
	}

	mergeExpectedLogWrites(expectedLog, appendFailoverLogRows(t, ctx, client, logTable, 2000, true))
	updateAndVerifyFailoverKVRows(t, ctx, client, kvTable, kvKeys)
	verifyFailoverLogRows(t, ctx, client, admin, logTable, expectedLog)

	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := client.GetTable(canceled, logPath); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetTable() canceled after failover = %v", err)
	}
	if _, err := client.GetTable(ctx, logPath); err != nil {
		t.Fatalf("GetTable() after canceled failover request = %v", err)
	}
	if _, err := client.NewAppendWriter(
		ctx,
		logTable,
		fgo.WithAppendRequest(5*time.Second, 1),
		fgo.WithAppendRetryPolicy(fgo.WriterRetryPolicy{MaxAttempts: 2}),
	); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("unsafe mutation retry configuration error = %v", err)
	}

	compose(t, "up", "--detach", "--wait", "--wait-timeout", "120", service)
	restarted = true
}

func appendFailoverLogRows(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	base int32,
	retry bool,
) map[int32][]expectedLogWrite {
	t.Helper()
	writer := openFailoverAppendWriter(t, ctx, client, table, retry)
	expected := make(map[int32][]expectedLogWrite, table.BucketCount)
	for index := range table.BucketCount {
		id := base + int32(index)
		result := writer.Append(ctx, fgo.Row{id, fmt.Sprintf("failover-%d", id)}).Await(ctx)
		if result.Err != nil || !result.OffsetKnown || result.Records != 1 {
			t.Fatalf("append failover row %d = %#v", id, result)
		}
		expected[result.Bucket] = append(expected[result.Bucket], expectedLogWrite{id: id, offset: result.BaseOffset})
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(expected) != table.BucketCount {
		t.Fatalf("round-robin writes reached %d of %d buckets: %#v", len(expected), table.BucketCount, expected)
	}
	return expected
}

func openFailoverAppendWriter(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	retry bool,
) *fgo.AppendWriter {
	t.Helper()
	options := []fgo.AppendWriterOption{
		fgo.WithAppendLinger(0),
		fgo.WithAppendBucketAssignment(fgo.AssignmentRoundRobin),
		fgo.WithAppendBatchLimits(1<<20, 1),
		fgo.WithAppendRequest(10*time.Second, -1),
	}
	if retry {
		options = append(options, fgo.WithAppendRetryPolicy(fgo.WriterRetryPolicy{
			MaxAttempts: 5,
			Backoff: func(int) time.Duration {
				return 100 * time.Millisecond
			},
		}))
	}
	var writer *fgo.AppendWriter
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		writer, err = client.NewAppendWriter(ctx, table, options...)
		if err == nil {
			break
		}
		if !retry || !isTransientConnectionFailure(err) {
			t.Fatalf("create failover append writer: %v", err)
		}
		if waitErr := waitRetryInterval(ctx, 100*time.Millisecond); waitErr != nil {
			t.Fatalf("retry failover append writer: %v (last error: %v)", waitErr, err)
		}
	}
	if writer == nil {
		t.Fatalf("create failover append writer after retries: %v", err)
	}
	return writer
}

func mergeExpectedLogWrites(target, source map[int32][]expectedLogWrite) {
	for bucket, writes := range source {
		target[bucket] = append(target[bucket], writes...)
	}
}

func seedFailoverKVRows(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
) map[int32]fgo.PrimaryKey {
	t.Helper()
	writer, err := client.NewUpsertWriter(
		ctx, table, fgo.WithUpsertLinger(0), fgo.WithUpsertBatchLimits(1<<20, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	keys := make(map[int32]fgo.PrimaryKey, table.BucketCount)
	for index := 0; len(keys) < table.BucketCount && index < 100; index++ {
		key := fgo.PrimaryKey{fmt.Sprintf("failover-%03d", index), int32(1)}
		result := writer.Upsert(ctx, fgo.Row{key[0], key[1], "before"}).Await(ctx)
		if result.Err != nil || !result.OffsetKnown || result.Records != 1 {
			t.Fatalf("seed failover KV row %d = %#v", index, result)
		}
		if _, exists := keys[result.Bucket]; !exists {
			keys[result.Bucket] = key
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(keys) != table.BucketCount {
		t.Fatalf("KV seeds reached %d of %d buckets: %#v", len(keys), table.BucketCount, keys)
	}
	return keys
}

func updateAndVerifyFailoverKVRows(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	keys map[int32]fgo.PrimaryKey,
) {
	t.Helper()
	writer := openFailoverUpsertWriter(t, ctx, client, table)
	ordered := updateFailoverKVRows(t, ctx, writer, table, keys)
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	verifyFailoverKVLookups(t, ctx, client, table, ordered)
}

func openFailoverUpsertWriter(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
) *fgo.UpsertWriter {
	t.Helper()
	options := []fgo.UpsertWriterOption{
		fgo.WithUpsertLinger(0),
		fgo.WithUpsertBatchLimits(1<<20, 1),
		fgo.WithUpsertRequest(10*time.Second, -1),
		fgo.WithUpsertRetryPolicy(fgo.WriterRetryPolicy{
			MaxAttempts: 5,
			Backoff: func(int) time.Duration {
				return 100 * time.Millisecond
			},
		}),
	}
	var writer *fgo.UpsertWriter
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		writer, err = client.NewUpsertWriter(ctx, table, options...)
		if err == nil {
			break
		}
		if !isTransientConnectionFailure(err) {
			t.Fatalf("create failover upsert writer: %v", err)
		}
		if waitErr := waitRetryInterval(ctx, 100*time.Millisecond); waitErr != nil {
			t.Fatalf("retry failover upsert writer: %v (last error: %v)", waitErr, err)
		}
	}
	if writer == nil {
		t.Fatalf("create failover upsert writer after retries: %v", err)
	}
	return writer
}

func updateFailoverKVRows(
	t *testing.T,
	ctx context.Context,
	writer *fgo.UpsertWriter,
	table fgo.Table,
	keys map[int32]fgo.PrimaryKey,
) []fgo.PrimaryKey {
	t.Helper()
	ordered := make([]fgo.PrimaryKey, 0, len(keys))
	for bucket := int32(0); bucket < int32(table.BucketCount); bucket++ {
		key, ok := keys[bucket]
		if !ok {
			t.Fatalf("missing failover key for bucket %d", bucket)
		}
		result := writer.Upsert(ctx, fgo.Row{key[0], key[1], "after"}).Await(ctx)
		if result.Err != nil || result.Bucket != bucket || result.Records != 1 {
			t.Fatalf("post-failover KV bucket %d = %#v", bucket, result)
		}
		ordered = append(ordered, key)
	}
	return ordered
}

func verifyFailoverKVLookups(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	ordered []fgo.PrimaryKey,
) {
	t.Helper()
	lookup, err := client.NewLookuper(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	defer lookup.Close()
	results := lookup.Lookup(ctx, ordered...)
	if len(results) != len(ordered) {
		t.Fatalf("post-failover lookup count = %d, want %d", len(results), len(ordered))
	}
	for index, result := range results {
		if result.Err != nil || !result.Found || result.Row[2] != "after" {
			t.Fatalf("post-failover lookup %d = %#v", index, result)
		}
	}
	missing := lookup.Lookup(ctx, fgo.PrimaryKey{"failover-missing", int32(1)})
	if len(missing) != 1 || !errors.Is(missing[0].Err, fgo.ErrNotFound) || missing[0].Found {
		t.Fatalf("terminal missing-key result = %#v", missing)
	}
}

func verifyFailoverLogRows(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	admin *fadm.Client,
	table fgo.Table,
	expected map[int32][]expectedLogWrite,
) {
	t.Helper()
	ends := failoverLogEnds(t, ctx, admin, table)
	wanted := expectedFailoverLogRows(expected)
	actual := scanFailoverLogRows(t, ctx, client, table, ends, wanted)
	assertFailoverLogRows(t, expected, actual)
}

func failoverLogEnds(
	t *testing.T,
	ctx context.Context,
	admin *fadm.Client,
	table fgo.Table,
) map[int32]int64 {
	t.Helper()
	buckets := make([]int32, table.BucketCount)
	for index := range buckets {
		buckets[index] = int32(index)
	}
	ends := make(map[int32]int64, len(buckets))
	for _, result := range admin.ListOffsets(
		ctx, table, fgo.PhysicalTablePath{TablePath: table.Path}, -1, buckets, fgo.Latest(),
	) {
		if result.Err != nil {
			t.Fatalf("latest offset for bucket %d: %v", result.Bucket, result.Err)
		}
		ends[result.Bucket] = result.Offset
	}
	return ends
}

func expectedFailoverLogRows(expected map[int32][]expectedLogWrite) map[int32]expectedLogWrite {
	wanted := make(map[int32]expectedLogWrite)
	for _, writes := range expected {
		for _, write := range writes {
			wanted[write.id] = write
		}
	}
	return wanted
}

func scanFailoverLogRows(
	t *testing.T,
	ctx context.Context,
	client *fgo.Client,
	table fgo.Table,
	ends map[int32]int64,
	wanted map[int32]expectedLogWrite,
) map[int32][]expectedLogWrite {
	t.Helper()
	scanner, err := client.NewLogScanner(
		ctx,
		table,
		fgo.Earliest(),
		fgo.WithScanLimits(1<<20, 1<<20, 1, 100*time.Millisecond),
		fgo.WithScanStoppingOffsets(ends),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	actual := make(map[int32][]expectedLogWrite)
	for !scanner.Done() {
		result, pollErr := scanner.Poll(ctx)
		if pollErr != nil {
			t.Fatal(pollErr)
		}
		if len(result.BucketErrors) != 0 {
			result.Release()
			t.Fatalf("post-failover bucket errors = %#v", result.BucketErrors)
		}
		if err := collectFailoverLogRows(result.Records, wanted, actual); err != nil {
			result.Release()
			t.Fatal(err)
		}
		result.Release()
	}
	return actual
}

func collectFailoverLogRows(
	records []fgo.ScanRecord,
	wanted map[int32]expectedLogWrite,
	actual map[int32][]expectedLogWrite,
) error {
	for _, scanned := range records {
		id, ok := scanned.Record.Value[0].(int32)
		if !ok {
			continue
		}
		write, tracked := wanted[id]
		if !tracked {
			continue
		}
		actual[scanned.Bucket] = append(actual[scanned.Bucket], expectedLogWrite{
			id: id, offset: scanned.Record.Offset,
		})
		if scanned.Record.Offset != write.offset {
			return fmt.Errorf("row %d offset = %d, want %d", id, scanned.Record.Offset, write.offset)
		}
	}
	return nil
}

func assertFailoverLogRows(
	t *testing.T,
	expected, actual map[int32][]expectedLogWrite,
) {
	t.Helper()
	for bucket, writes := range expected {
		got := actual[bucket]
		if len(got) != len(writes) {
			t.Fatalf("bucket %d failover rows = %#v, want %#v", bucket, got, writes)
		}
		for index := range writes {
			if got[index] != writes[index] {
				t.Fatalf("bucket %d row %d = %#v, want %#v", bucket, index, got[index], writes[index])
			}
		}
	}
}

func testCoordinatorRecovery(
	t *testing.T,
	client *fgo.Client,
	path fgo.TablePath,
) {
	t.Helper()
	compose(t, "stop", "plaintext-coordinator")
	restarted := false
	defer func() {
		if !restarted {
			startPlaintextCoordinator(t)
		}
	}()

	admin, err := fadm.New(client)
	if err != nil {
		t.Fatal(err)
	}
	failureCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, failure := admin.GetServerNodes(failureCtx)
	cancel()
	if failure == nil || !isTransientConnectionFailure(failure) {
		t.Fatalf("stopped coordinator error = %v, want transient connection failure", failure)
	}

	startPlaintextCoordinator(t)
	restarted = true
	ctx, stop := context.WithTimeout(context.Background(), 45*time.Second)
	defer stop()
	if err := waitForCondition(ctx, 250*time.Millisecond, func() (bool, error) {
		nodes, nodesErr := admin.GetServerNodes(ctx)
		if nodesErr != nil {
			return false, nil
		}
		_, tableErr := client.GetTable(ctx, path)
		return len(nodes) == 4 && tableErr == nil, nil
	}); err != nil {
		t.Fatalf("long-lived client did not recover after coordinator restart: %v", err)
	}
	canceled, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	if _, err := admin.GetServerNodes(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled recovered admin request = %v", err)
	}
	if _, err := admin.GetServerNodes(ctx); err != nil {
		t.Fatalf("admin request after cancellation = %v", err)
	}
}

func isTransientConnectionFailure(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) ||
		errors.Is(err, transport.ErrClosed) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

func waitForCondition(
	ctx context.Context,
	interval time.Duration,
	condition func() (bool, error),
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ready, err := condition()
		if ready || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitRetryInterval(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func leadersMoved(before, after map[int32]int32, stopped int32) bool {
	if len(after) != len(before) {
		return false
	}
	changed := false
	for bucket, leader := range after {
		if leader == stopped {
			return false
		}
		changed = changed || before[bucket] != leader
	}
	return changed
}

func metadataLeaders(t *testing.T, client *fgo.Client, path fgo.TablePath) map[int32]int32 {
	t.Helper()
	leaders, err := tryMetadataLeaders(client, path)
	if err != nil {
		t.Fatal(err)
	}
	return leaders
}

func tryMetadataLeaders(client *fgo.Client, path fgo.TablePath) (map[int32]int32, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyGetMetadata, 0)
	if err != nil {
		return nil, err
	}
	request.Message().(*fmsg.MetadataRequest).TablePath = []*fmsg.PbTablePath{{
		DatabaseName: proto.String(path.Database), TableName: proto.String(path.Table),
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := client.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	metadata := response.Message().(*fmsg.MetadataResponse)
	for _, table := range metadata.GetTableMetadata() {
		if table.GetTablePath().GetDatabaseName() != path.Database ||
			table.GetTablePath().GetTableName() != path.Table {
			continue
		}
		leaders := make(map[int32]int32, len(table.GetBucketMetadata()))
		for _, bucket := range table.GetBucketMetadata() {
			if bucket.LeaderId == nil {
				return nil, errors.New("metadata contains a bucket without a leader")
			}
			leaders[bucket.GetBucketId()] = bucket.GetLeaderId()
		}
		return leaders, nil
	}
	return nil, errors.New("metadata omitted integration table")
}

func compose(t *testing.T, arguments ...string) {
	t.Helper()
	file, project := os.Getenv("FLUSS_COMPOSE_FILE"), os.Getenv("FLUSS_COMPOSE_PROJECT")
	args := []string{"compose", "--project-name", project, "--file", file}
	args = append(args, arguments...)
	command := exec.Command("docker", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v: %s", strings.Join(arguments, " "), err, output)
	}
}

func startPlaintextCoordinator(t *testing.T) {
	t.Helper()
	compose(t, "start", "plaintext-coordinator")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	address := net.JoinHostPort("127.0.0.1", env("FLUSS_PLAIN_COORDINATOR_PORT", "19123"))
	if err := waitForCondition(ctx, 250*time.Millisecond, func() (bool, error) {
		connection, dialErr := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", address)
		if dialErr != nil {
			return false, nil
		}
		return true, connection.Close()
	}); err != nil {
		t.Fatalf("plaintext coordinator did not restart: %v", err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
