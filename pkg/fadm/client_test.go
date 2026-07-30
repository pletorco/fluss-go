package fadm

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type fakeRequester struct {
	coordinator func(context.Context, fmsg.Request) (fmsg.Response, error)
	bucket      func(context.Context, fgo.PhysicalTablePath, int32, fmsg.Request) (fmsg.Response, error)
	table       fgo.Table
	openErr     error
}

func (f *fakeRequester) RequestCoordinator(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
	return f.coordinator(ctx, request)
}

func (f *fakeRequester) RequestBucket(
	ctx context.Context,
	path fgo.PhysicalTablePath,
	bucket int32,
	request fmsg.Request,
) (fmsg.Response, error) {
	return f.bucket(ctx, path, bucket, request)
}

func (f *fakeRequester) OpenTable(context.Context, fgo.TablePath) (fgo.Table, error) {
	return f.table, f.openErr
}

func adminSchema() fgo.Schema {
	return fgo.Schema{
		Columns: []fgo.Column{
			{Name: "id", Type: fgo.IntType},
			{Name: "name", Type: fgo.StringType, Nullable: true},
		},
		PrimaryKey: []string{"id"}, BucketKey: []string{"id"}, PartitionKey: []string{"name"},
	}
}

func TestCoreAdminCatalogLifecycle(t *testing.T) {
	path := fgo.TablePath{Database: "db", Table: "users"}
	schema := adminSchema()
	schemaJSON, _ := schema.JSON()
	opened := fgo.Table{ID: 7, SchemaID: 2, Path: path, Kind: fgo.PrimaryKeyTable, Schema: schema, BucketCount: 2}
	seen := make(map[fmsg.APIKey]int)
	fake := &fakeRequester{table: opened}
	fake.coordinator = func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		seen[request.APIKey()]++
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		switch message := request.(*fmsg.MessageRequest).Message().(type) {
		case *fmsg.CreateDatabaseRequest:
			if message.GetDatabaseName() != "db" || !message.GetIgnoreIfExists() ||
				!json.Valid(message.GetDatabaseJson()) {
				t.Fatalf("CreateDatabase request = %#v", message)
			}
			var descriptor struct {
				Version          int               `json:"version"`
				Comment          string            `json:"comment"`
				CustomProperties map[string]string `json:"custom_properties"`
			}
			if err := json.Unmarshal(message.GetDatabaseJson(), &descriptor); err != nil ||
				descriptor.Version != 1 || descriptor.Comment != "main" ||
				descriptor.CustomProperties["owner"] != "team" {
				t.Fatalf("database descriptor = %s, %v", message.GetDatabaseJson(), err)
			}
		case *fmsg.DropDatabaseRequest:
			if !message.GetIgnoreIfNotExists() || !message.GetCascade() {
				t.Fatalf("DropDatabase request = %#v", message)
			}
		case *fmsg.ListDatabasesRequest:
			if !message.GetIncludeSummary() {
				t.Fatal("database summaries not requested")
			}
			response.Message().(*fmsg.ListDatabasesResponse).DatabaseSummary = []*fmsg.PbDatabaseSummary{{
				DatabaseName: proto.String("db"), CreatedTime: proto.Int64(1000), TableCount: proto.Int32(2),
			}}
		case *fmsg.DatabaseExistsRequest:
			response.Message().(*fmsg.DatabaseExistsResponse).Exists = proto.Bool(true)
		case *fmsg.GetDatabaseInfoRequest:
			info := response.Message().(*fmsg.GetDatabaseInfoResponse)
			info.DatabaseJson = []byte(`{"comment":"main","custom_properties":{"owner":"team"}}`)
			info.CreatedTime, info.ModifiedTime = proto.Int64(1000), proto.Int64(2000)
		case *fmsg.CreateTableRequest:
			if message.GetTablePath().GetTableName() != "users" || !message.GetIgnoreIfExists() {
				t.Fatalf("CreateTable request = %#v", message)
			}
			var descriptor struct {
				Version     int             `json:"version"`
				BucketCount int             `json:"bucket_count"`
				Schema      json.RawMessage `json:"schema"`
			}
			if err := json.Unmarshal(message.GetTableJson(), &descriptor); err != nil ||
				descriptor.Version != 1 || descriptor.BucketCount != 2 || len(descriptor.Schema) == 0 {
				t.Fatalf("table descriptor = %s, %v", message.GetTableJson(), err)
			}
		case *fmsg.DropTableRequest:
			if !message.GetIgnoreIfNotExists() {
				t.Fatal("DropTable did not preserve ignore flag")
			}
		case *fmsg.ListTablesRequest:
			response.Message().(*fmsg.ListTablesResponse).TableName = []string{"users", "events"}
		case *fmsg.TableExistsRequest:
			response.Message().(*fmsg.TableExistsResponse).Exists = proto.Bool(true)
		case *fmsg.GetTableSchemaRequest:
			if message.GetSchemaId() != 2 {
				t.Fatalf("schema ID = %d", message.GetSchemaId())
			}
			schemaResponse := response.Message().(*fmsg.GetTableSchemaResponse)
			schemaResponse.SchemaId, schemaResponse.SchemaJson = proto.Int32(2), schemaJSON
		case *fmsg.AlterTableRequest:
			if len(message.GetConfigChanges()) != 1 || len(message.GetAddColumns()) != 1 ||
				len(message.GetDropColumns()) != 1 || len(message.GetRenameColumns()) != 1 {
				t.Fatalf("AlterTable request = %#v", message)
			}
		case *fmsg.CreatePartitionRequest:
			keys := message.GetPartitionSpec().GetPartitionKeyValues()
			if len(keys) != 2 || keys[0].GetKey() != "day" || keys[1].GetKey() != "region" ||
				!message.GetIgnoreIfNotExists() {
				t.Fatalf("CreatePartition request = %#v", message)
			}
		case *fmsg.DropPartitionRequest:
			if !message.GetIgnoreIfNotExists() {
				t.Fatal("DropPartition did not preserve ignore flag")
			}
		case *fmsg.ListPartitionInfosRequest:
			if message.GetPartialPartitionSpec() == nil {
				t.Fatal("partial partition filter missing")
			}
			response.Message().(*fmsg.ListPartitionInfosResponse).PartitionsInfo = []*fmsg.PbPartitionInfo{{
				PartitionId: proto.Int64(9), PartitionSpec: (&PartitionSpec{"day": "1", "region": "kr"}).proto(),
			}}
		default:
			t.Fatalf("unexpected coordinator request %T", message)
		}
		return response, nil
	}
	fake.bucket = func(_ context.Context, physical fgo.PhysicalTablePath, bucket int32, request fmsg.Request) (fmsg.Response, error) {
		if physical.TablePath != path || request.APIKey() != fmsg.APIKeyListOffsets {
			t.Fatalf("bucket request = %s %d %d", physical, bucket, request.APIKey())
		}
		message := request.(*fmsg.MessageRequest).Message().(*fmsg.ListOffsetsRequest)
		if message.GetOffsetType() != 2 || message.GetStartTimestamp() != 3000 || message.GetPartitionId() != 9 {
			t.Fatalf("ListOffsets request = %#v", message)
		}
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		item := &fmsg.PbListOffsetsRespForBucket{BucketId: proto.Int32(bucket), Offset: proto.Int64(int64(bucket + 10))}
		if bucket == 1 {
			item.ErrorCode = proto.Int32(int32(fmsg.ErrorCodeNotLeaderOrFollower))
		}
		response.Message().(*fmsg.ListOffsetsResponse).BucketsResp = []*fmsg.PbListOffsetsRespForBucket{item}
		return response, nil
	}
	client := newClient(fake)

	if err := client.CreateDatabase(context.Background(), "db", DatabaseDefinition{
		Comment: "main", Properties: map[string]string{"owner": "team"},
	}, true); err != nil {
		t.Fatal(err)
	}
	if err := client.DropDatabase(context.Background(), "db", true, true); err != nil {
		t.Fatal(err)
	}
	databases, err := client.ListDatabases(context.Background())
	if err != nil || len(databases) != 1 || databases[0].TableCount != 2 || databases[0].CreatedAt.UnixMilli() != 1000 {
		t.Fatalf("ListDatabases() = %#v, %v", databases, err)
	}
	exists, err := client.DatabaseExists(context.Background(), "db")
	if err != nil || !exists {
		t.Fatalf("DatabaseExists() = %v, %v", exists, err)
	}
	database, err := client.DescribeDatabase(context.Background(), "db")
	if err != nil || database.Comment != "main" || database.Properties["owner"] != "team" ||
		database.ModifiedAt.UnixMilli() != 2000 {
		t.Fatalf("DescribeDatabase() = %#v, %v", database, err)
	}
	definition := TableDefinition{
		Schema: schema, Comment: "users", BucketCount: 2,
		Properties: map[string]string{"table.log.format": "COMPACTED"},
	}
	if err := client.CreateTable(context.Background(), path, definition, true); err != nil {
		t.Fatal(err)
	}
	if err := client.DropTable(context.Background(), path, true); err != nil {
		t.Fatal(err)
	}
	tables, err := client.ListTables(context.Background(), "db")
	if err != nil || len(tables) != 2 || tables[1].Table != "events" {
		t.Fatalf("ListTables() = %#v, %v", tables, err)
	}
	exists, err = client.TableExists(context.Background(), path)
	if err != nil || !exists {
		t.Fatalf("TableExists() = %v, %v", exists, err)
	}
	table, err := client.DescribeTable(context.Background(), path)
	if err != nil || table.ID != 7 {
		t.Fatalf("DescribeTable() = %#v, %v", table, err)
	}
	gotSchema, err := client.TableSchema(context.Background(), path, 2)
	if err != nil || len(gotSchema.Columns) != 2 {
		t.Fatalf("TableSchema() = %#v, %v", gotSchema, err)
	}
	value := "compact"
	if err := client.AlterTable(context.Background(), path, AlterTable{
		Config: []ConfigChange{{Key: "format", Value: &value, Op: ConfigSet}},
		Add: []AddColumn{{
			Name: "age", Type: fgo.LogicalType{Root: "INTEGER", Nullable: true}, Description: "age", First: true,
		}},
		Drop: []string{"old"}, Rename: []RenameColumn{{Old: "name", New: "display_name"}},
	}, false); err != nil {
		t.Fatal(err)
	}
	spec := PartitionSpec{"region": "kr", "day": "1"}
	if err := client.CreatePartition(context.Background(), path, spec, true); err != nil {
		t.Fatal(err)
	}
	if err := client.DropPartition(context.Background(), path, spec, true); err != nil {
		t.Fatal(err)
	}
	partitions, err := client.ListPartitions(context.Background(), path, PartitionSpec{"day": "1"})
	if err != nil || len(partitions) != 1 || partitions[0].ID != 9 || partitions[0].Spec["region"] != "kr" {
		t.Fatalf("ListPartitions() = %#v, %v", partitions, err)
	}
	offsets := client.ListOffsets(
		context.Background(), table, fgo.PhysicalTablePath{TablePath: path, Partition: "day=1"},
		9, []int32{0, 1}, fgo.AtTimestamp(time.UnixMilli(3000)),
	)
	if offsets[0].Offset != 10 || offsets[0].Err != nil || !errors.Is(offsets[1].Err, fgo.ErrMetadata) {
		t.Fatalf("ListOffsets() = %#v", offsets)
	}
	if len(seen) != 14 {
		t.Fatalf("coordinator APIs seen = %#v", seen)
	}
}

func TestAdminValidation(t *testing.T) {
	fake := &fakeRequester{
		coordinator: func(context.Context, fmsg.Request) (fmsg.Response, error) {
			return nil, errors.New("should not request")
		},
		bucket: func(context.Context, fgo.PhysicalTablePath, int32, fmsg.Request) (fmsg.Response, error) {
			return nil, errors.New("should not request")
		},
	}
	client := newClient(fake)
	path := fgo.TablePath{Database: "db", Table: "t"}
	if _, err := New(nil); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("New(nil) error = %v", err)
	}
	if _, err := New(&fgo.Client{}); err != nil {
		t.Fatal(err)
	}
	for name, err := range map[string]error{
		"create db": client.CreateDatabase(context.Background(), " ", DatabaseDefinition{}, false),
		"drop db":   client.DropDatabase(context.Background(), "", false, false),
		"db exists": func() error {
			_, err := client.DatabaseExists(context.Background(), "")
			return err
		}(),
		"describe db": func() error {
			_, err := client.DescribeDatabase(context.Background(), "")
			return err
		}(),
		"create table": client.CreateTable(context.Background(), path, TableDefinition{Schema: adminSchema()}, false),
		"alter empty":  client.AlterTable(context.Background(), path, AlterTable{}, false),
		"create part":  client.CreatePartition(context.Background(), path, nil, false),
		"drop part":    client.DropPartition(context.Background(), path, PartitionSpec{"day": ""}, false),
		"schema": func() error {
			_, err := client.TableSchema(context.Background(), path, -1)
			return err
		}(),
	} {
		if !errors.Is(err, fgo.ErrInvalidConfig) {
			t.Errorf("%s error = %v", name, err)
		}
	}
	offsets := client.ListOffsets(
		context.Background(), fgo.Table{ID: 1}, fgo.PhysicalTablePath{TablePath: path},
		-1, []int32{0}, fgo.AtOffset(1),
	)
	if !errors.Is(offsets[0].Err, fgo.ErrInvalidConfig) {
		t.Fatalf("explicit offsets = %#v", offsets)
	}
}

func TestAdminFallbackNamesAndUnexpectedResponses(t *testing.T) {
	fake := &fakeRequester{}
	fake.coordinator = func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		if request.APIKey() == fmsg.APIKeyListDatabases {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			response.Message().(*fmsg.ListDatabasesResponse).DatabaseName = []string{"a", "b"}
			return response, nil
		}
		response, _ := fmsg.NewResponse(fmsg.APIKeyApiVersions, 0)
		return response, nil
	}
	client := newClient(fake)
	databases, err := client.ListDatabases(context.Background())
	if err != nil || len(databases) != 2 || databases[0].Name != "a" {
		t.Fatalf("fallback databases = %#v, %v", databases, err)
	}
	if _, err := client.ListTables(context.Background(), "db"); err == nil {
		t.Fatal("unexpected list tables response succeeded")
	}
	if _, err := client.TableExists(context.Background(), fgo.TablePath{Database: "db", Table: "t"}); err == nil {
		t.Fatal("unexpected table exists response succeeded")
	}
}
