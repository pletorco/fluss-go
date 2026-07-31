//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
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

	t.Run("protocol negotiation and role", func(t *testing.T) {
		verifyProtocolRegistry(t, protocolEndpoints(plainAddress))
	})

	client := openClient(t, []string{"127.0.0.1:1", plainAddress})
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
	if err := admin.CreateDatabase(context.Background(), database, fadm.DatabaseDefinition{
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
		testLogWriteFormats(t, client, admin, database)
	})

	t.Run("upsert delete lookup and prefix lookup", func(t *testing.T) {
		testKVData(t, client, kvPath)
	})

	t.Run("multi-node routing and leader failover", func(t *testing.T) {
		testLeaderFailover(t, client, logPath)
	})
}

func protocolEndpoints(plainAddress string) map[string]fgo.ServerRole {
	return map[string]fgo.ServerRole{
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
	authenticated := openClient(t, []string{address}, fgo.WithAuthenticator(fgo.PlainAuthenticator(username, password)))
	defer authenticated.Close()
	admin, err := fadm.New(authenticated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ListDatabases(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = fgo.Open(ctx,
		fgo.WithSeedBrokers(address),
		fgo.WithAuthenticator(fgo.PlainAuthenticator(username, password+"-wrong")),
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

	acl := fadm.ACL{
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
	filter := fadm.ACLFilter{
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
		fgo.WithAuthenticator(fgo.PlainAuthenticator(username, password)),
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
	err := admin.CreateDatabase(ctx, name, fadm.DatabaseDefinition{}, false)
	if allowed && err != nil {
		t.Fatalf("CreateDatabase(%q) with ACL: %v", name, err)
	}
	if !allowed && !errors.Is(err, fgo.ErrAuthorization) {
		t.Fatalf("CreateDatabase(%q) authorization error = %v", name, err)
	}
}

func requireCreatedACL(t *testing.T, ctx context.Context, admin *fadm.Client, acl fadm.ACL) {
	t.Helper()
	created, err := admin.CreateACLs(ctx, acl)
	if err != nil || len(created) != 1 || created[0].Err != nil || created[0].ACL != acl {
		t.Fatalf("CreateACLs() = %#v, %v", created, err)
	}
}

func requireListedACL(
	t *testing.T,
	ctx context.Context,
	admin *fadm.Client,
	filter fadm.ACLFilter,
	acl fadm.ACL,
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
	filter fadm.ACLFilter,
	acl fadm.ACL,
) {
	t.Helper()
	results, err := admin.DropACLs(ctx, filter)
	if err != nil || len(results) != 1 || results[0].Err != nil ||
		len(results[0].Matches) != 1 || results[0].Matches[0].Err != nil ||
		results[0].Matches[0].ACL != acl {
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

func verifyProtocolRegistry(t *testing.T, endpoints map[string]fgo.ServerRole) {
	t.Helper()
	server := make(map[fmsg.APIKey][2]int32)
	for address, expectedRole := range endpoints {
		mergeVersions(server, endpointVersions(t, address, expectedRole))
	}
	verifyLocalVersions(t, server)
}

func endpointVersions(t *testing.T, address string, expectedRole fgo.ServerRole) map[fmsg.APIKey][2]int32 {
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
	if got := fgo.ServerRole(versions.GetServerType()); got != expectedRole {
		t.Fatalf("%s server role = %d, want %d", address, got, expectedRole)
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
		all := []fgo.Option{fgo.WithSeedBrokers(seeds...), fgo.WithDialTimeout(3 * time.Second)}
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
	if err := admin.CreateTable(context.Background(), logPath, fadm.TableDefinition{
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
	if err := admin.CreateTable(context.Background(), kvPath, fadm.TableDefinition{
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
		table, err := admin.DescribeTable(context.Background(), path)
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
	table, err := client.OpenTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := client.NewLogWriter(ctx, table, fgo.WithLogLinger(0))
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
	if err := admin.CreateTable(ctx, path, fadm.TableDefinition{
		Schema: fgo.Schema{Columns: []fgo.Column{
			{Name: "id", Type: fgo.IntType},
			{Name: "message", Type: fgo.StringType, Nullable: true},
		}},
		BucketCount: 1,
	}, false); err != nil {
		t.Fatal(err)
	}
	waitForTableReady(t, admin, path)
	oldTable, err := client.OpenTable(ctx, path)
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
	writer, err := client.NewLogWriter(ctx, table, fgo.WithLogLinger(0))
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
		table, err := client.OpenTable(ctx, path)
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
	if err := admin.CreateTable(ctx, path, fadm.TableDefinition{
		Schema: schema, BucketCount: 1,
	}, false); err != nil {
		t.Fatal(err)
	}
	client := openClient(t, []string{address}, fgo.WithDynamicPartitionCreation(
		fgo.DynamicPartitionCreationConfig{MetadataAttempts: 10, RetryBackoff: 100 * time.Millisecond},
	))
	defer client.Close()
	table, err := client.OpenTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	spec := fgo.PartitionSpec{"region": "kr"}
	writer, err := client.NewLogWriter(
		ctx, table, fgo.WithLogPartitionSpec(table.Schema, spec), fgo.WithLogLinger(0),
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

func testLogWriteFormats(
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
	for _, format := range []fgo.LogWriteFormat{
		fgo.LogWriteFormatCompacted,
		fgo.LogWriteFormatIndexed,
		fgo.LogWriteFormatArrow,
	} {
		testLogWriteFormat(t, client, admin, database, schema, format)
	}
}

func testLogWriteFormat(
	t *testing.T,
	client *fgo.Client,
	admin *fadm.Client,
	database string,
	schema fgo.Schema,
	format fgo.LogWriteFormat,
) {
	t.Helper()
	path := fgo.TablePath{Database: database, Table: "format_" + string(format)}
	if err := admin.CreateTable(context.Background(), path, fadm.TableDefinition{
		Schema: schema, BucketCount: 1,
		Properties: map[string]string{"table.log.format": strings.ToUpper(string(format))},
	}, false); err != nil {
		t.Fatalf("create %s table: %v", format, err)
	}
	waitForTableReady(t, admin, path)
	table, err := client.OpenTable(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	options := []fgo.LogWriterOption{
		fgo.WithLogWriteFormat(format),
		fgo.WithLogLinger(0),
	}
	if format == fgo.LogWriteFormatArrow {
		options = append(options, fgo.WithLogArrowCompression(fgo.ArrowCompressionZSTD))
	}
	writer, err := client.NewLogWriter(context.Background(), table, options...)
	if err != nil {
		t.Fatalf("create %s writer: %v", format, err)
	}
	if format == fgo.LogWriteFormatArrow {
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
}

func appendArrowFormatRow(t *testing.T, writer *fgo.LogWriter, table fgo.Table) {
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

func assertFormatRow(
	t *testing.T,
	client *fgo.Client,
	table fgo.Table,
	format fgo.LogWriteFormat,
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
	table, err := client.OpenTable(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := client.NewKVWriter(ctx, table, fgo.WithKVLinger(0))
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
	lookup, err := client.NewLookupClient(ctx, table)
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

func upsertRows(t *testing.T, ctx context.Context, writer *fgo.KVWriter, rows []fgo.Row) {
	t.Helper()
	for index, row := range rows {
		if result := writer.Upsert(ctx, row).Await(ctx); result.Err != nil {
			t.Fatalf("upsert %d: %v", index, result.Err)
		}
	}
}

func testPointAndPrefixLookup(t *testing.T, ctx context.Context, lookup *fgo.LookupClient) {
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
	lookup *fgo.LookupClient,
) {
	t.Helper()
	deleteWriter, err := client.NewKVWriter(ctx, table, fgo.WithKVLinger(0))
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
	lookup, err := client.NewLookupClient(
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

func testLeaderFailover(t *testing.T, client *fgo.Client, path fgo.TablePath) {
	t.Helper()
	before := metadataLeaders(t, client, path)
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
	defer compose(t, "up", "--detach", service)

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		leaders, err := tryMetadataLeaders(client, path)
		if err == nil && leadersMoved(before, leaders, stopped) {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("leaders did not move away from tablet %d; before=%#v", stopped, before)
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

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
