// Package fadm provides administrative operations for Apache Fluss 0.9.1.
package fadm

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type requester interface {
	RequestCoordinator(context.Context, fmsg.Request) (fmsg.Response, error)
	RequestBucket(context.Context, fgo.PhysicalTablePath, int32, fmsg.Request) (fmsg.Response, error)
	OpenTable(context.Context, fgo.TablePath) (fgo.Table, error)
}

type Client struct{ requester requester }

// New shares the supplied data client's negotiated connections and pool.
func New(client *fgo.Client) (*Client, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil fgo client", fgo.ErrInvalidConfig)
	}
	return &Client{requester: client}, nil
}

func newClient(requester requester) *Client { return &Client{requester: requester} }

type Database struct {
	Name       string
	Comment    string
	Properties map[string]string
	CreatedAt  time.Time
	ModifiedAt time.Time
	TableCount int32
}

type DatabaseDefinition struct {
	Comment    string            `json:"comment,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

func (c *Client) CreateDatabase(ctx context.Context, name string, definition DatabaseDefinition, ignoreIfExists bool) error {
	if err := validateName("database", name); err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		Version int `json:"version"`
		DatabaseDefinition
	}{Version: 1, DatabaseDefinition: definition})
	if err != nil {
		return err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyCreateDatabase, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.CreateDatabaseRequest)
	message.DatabaseName, message.IgnoreIfExists, message.DatabaseJson = proto.String(name), proto.Bool(ignoreIfExists), body
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func (c *Client) DropDatabase(ctx context.Context, name string, ignoreIfNotExists, cascade bool) error {
	if err := validateName("database", name); err != nil {
		return err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyDropDatabase, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.DropDatabaseRequest)
	message.DatabaseName = proto.String(name)
	message.IgnoreIfNotExists = proto.Bool(ignoreIfNotExists)
	message.Cascade = proto.Bool(cascade)
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func (c *Client) ListDatabases(ctx context.Context) ([]Database, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyListDatabases, 0)
	if err != nil {
		return nil, err
	}
	request.Message().(*fmsg.ListDatabasesRequest).IncludeSummary = proto.Bool(true)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	message, ok := response.Message().(*fmsg.ListDatabasesResponse)
	if !ok {
		return nil, unexpected("list databases", response)
	}
	databases := make([]Database, 0, max(len(message.GetDatabaseName()), len(message.GetDatabaseSummary())))
	if len(message.GetDatabaseSummary()) != 0 {
		for _, summary := range message.GetDatabaseSummary() {
			databases = append(databases, Database{
				Name: summary.GetDatabaseName(), CreatedAt: millis(summary.GetCreatedTime()),
				TableCount: summary.GetTableCount(),
			})
		}
	} else {
		for _, name := range message.GetDatabaseName() {
			databases = append(databases, Database{Name: name})
		}
	}
	return databases, nil
}

func (c *Client) DatabaseExists(ctx context.Context, name string) (bool, error) {
	if err := validateName("database", name); err != nil {
		return false, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyDatabaseExists, 0)
	if err != nil {
		return false, err
	}
	request.Message().(*fmsg.DatabaseExistsRequest).DatabaseName = proto.String(name)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return false, err
	}
	message, ok := response.Message().(*fmsg.DatabaseExistsResponse)
	if !ok {
		return false, unexpected("database exists", response)
	}
	return message.GetExists(), nil
}

func (c *Client) DescribeDatabase(ctx context.Context, name string) (Database, error) {
	if err := validateName("database", name); err != nil {
		return Database{}, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyGetDatabaseInfo, 0)
	if err != nil {
		return Database{}, err
	}
	request.Message().(*fmsg.GetDatabaseInfoRequest).DatabaseName = proto.String(name)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return Database{}, err
	}
	message, ok := response.Message().(*fmsg.GetDatabaseInfoResponse)
	if !ok {
		return Database{}, unexpected("describe database", response)
	}
	var definition struct {
		Comment    string            `json:"comment"`
		Properties map[string]string `json:"properties"`
	}
	if err := json.Unmarshal(message.GetDatabaseJson(), &definition); err != nil {
		return Database{}, fmt.Errorf("%w: invalid database descriptor: %v", fgo.ErrValidation, err)
	}
	return Database{
		Name: name, Comment: definition.Comment, Properties: definition.Properties,
		CreatedAt: millis(message.GetCreatedTime()), ModifiedAt: millis(message.GetModifiedTime()),
	}, nil
}

type TableDefinition struct {
	Schema           fgo.Schema
	Comment          string
	BucketCount      int
	Properties       map[string]string
	CustomProperties map[string]string
}

func (d TableDefinition) JSON() ([]byte, error) {
	if err := d.Schema.Validate(); err != nil {
		return nil, err
	}
	if d.BucketCount <= 0 {
		return nil, fmt.Errorf("%w: table bucket count must be positive", fgo.ErrInvalidConfig)
	}
	schema, err := d.Schema.JSON()
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Version          int               `json:"version"`
		Schema           json.RawMessage   `json:"schema"`
		Comment          string            `json:"comment,omitempty"`
		PartitionKey     []string          `json:"partition_key"`
		BucketKey        []string          `json:"bucket_key"`
		BucketCount      int               `json:"bucket_count"`
		Properties       map[string]string `json:"properties"`
		CustomProperties map[string]string `json:"custom_properties"`
	}{
		Version: 1, Schema: schema, Comment: d.Comment, PartitionKey: d.Schema.PartitionKey,
		BucketKey: d.Schema.BucketKey, BucketCount: d.BucketCount, Properties: nonNilMap(d.Properties),
		CustomProperties: nonNilMap(d.CustomProperties),
	})
}

func (c *Client) CreateTable(ctx context.Context, path fgo.TablePath, definition TableDefinition, ignoreIfExists bool) error {
	if err := path.Validate(); err != nil {
		return err
	}
	body, err := definition.JSON()
	if err != nil {
		return err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyCreateTable, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.CreateTableRequest)
	message.TablePath = pbTablePath(path)
	message.TableJson = body
	message.IgnoreIfExists = proto.Bool(ignoreIfExists)
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func (c *Client) DropTable(ctx context.Context, path fgo.TablePath, ignoreIfNotExists bool) error {
	if err := path.Validate(); err != nil {
		return err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyDropTable, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.DropTableRequest)
	message.TablePath, message.IgnoreIfNotExists = pbTablePath(path), proto.Bool(ignoreIfNotExists)
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func (c *Client) ListTables(ctx context.Context, database string) ([]fgo.TablePath, error) {
	if err := validateName("database", database); err != nil {
		return nil, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyListTables, 0)
	if err != nil {
		return nil, err
	}
	request.Message().(*fmsg.ListTablesRequest).DatabaseName = proto.String(database)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	message, ok := response.Message().(*fmsg.ListTablesResponse)
	if !ok {
		return nil, unexpected("list tables", response)
	}
	tables := make([]fgo.TablePath, len(message.GetTableName()))
	for index, name := range message.GetTableName() {
		tables[index] = fgo.TablePath{Database: database, Table: name}
	}
	return tables, nil
}

func (c *Client) TableExists(ctx context.Context, path fgo.TablePath) (bool, error) {
	if err := path.Validate(); err != nil {
		return false, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyTableExists, 0)
	if err != nil {
		return false, err
	}
	request.Message().(*fmsg.TableExistsRequest).TablePath = pbTablePath(path)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return false, err
	}
	message, ok := response.Message().(*fmsg.TableExistsResponse)
	if !ok {
		return false, unexpected("table exists", response)
	}
	return message.GetExists(), nil
}

func (c *Client) DescribeTable(ctx context.Context, path fgo.TablePath) (fgo.Table, error) {
	if err := path.Validate(); err != nil {
		return fgo.Table{}, err
	}
	return c.requester.OpenTable(ctx, path)
}

func (c *Client) TableSchema(ctx context.Context, path fgo.TablePath, schemaID int32) (fgo.Schema, error) {
	if err := path.Validate(); err != nil {
		return fgo.Schema{}, err
	}
	if schemaID < 0 {
		return fgo.Schema{}, fmt.Errorf("%w: negative schema ID", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyGetTableSchema, 0)
	if err != nil {
		return fgo.Schema{}, err
	}
	message := request.Message().(*fmsg.GetTableSchemaRequest)
	message.TablePath, message.SchemaId = pbTablePath(path), proto.Int32(schemaID)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return fgo.Schema{}, err
	}
	schema, ok := response.Message().(*fmsg.GetTableSchemaResponse)
	if !ok {
		return fgo.Schema{}, unexpected("table schema", response)
	}
	return fgo.ParseSchemaJSON(schema.GetSchemaJson())
}

type ConfigOp int32

const (
	ConfigSet ConfigOp = iota
	ConfigDelete
	ConfigAppend
	ConfigSubtract
)

type ConfigChange struct {
	Key   string
	Value *string
	Op    ConfigOp
}

type AddColumn struct {
	Name        string
	Type        fgo.LogicalType
	Description string
	First       bool
}

type RenameColumn struct{ Old, New string }

type AlterTable struct {
	Config []ConfigChange
	Add    []AddColumn
	Drop   []string
	Rename []RenameColumn
}

func (c *Client) AlterTable(ctx context.Context, path fgo.TablePath, changes AlterTable, ignoreIfNotExists bool) error {
	if err := path.Validate(); err != nil {
		return err
	}
	if len(changes.Config)+len(changes.Add)+len(changes.Drop)+len(changes.Rename) == 0 {
		return fmt.Errorf("%w: alter table has no changes", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyAlterTable, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.AlterTableRequest)
	message.TablePath, message.IgnoreIfNotExists = pbTablePath(path), proto.Bool(ignoreIfNotExists)
	for _, change := range changes.Config {
		if change.Key == "" || change.Op < ConfigSet || change.Op > ConfigSubtract {
			return fmt.Errorf("%w: invalid table config change", fgo.ErrInvalidConfig)
		}
		item := &fmsg.PbAlterConfig{ConfigKey: proto.String(change.Key), OpType: proto.Int32(int32(change.Op))}
		if change.Value != nil {
			item.ConfigValue = proto.String(*change.Value)
		}
		message.ConfigChanges = append(message.ConfigChanges, item)
	}
	for _, change := range changes.Add {
		if change.Name == "" {
			return fmt.Errorf("%w: add column name is empty", fgo.ErrInvalidConfig)
		}
		if err := change.Type.Validate(); err != nil {
			return err
		}
		dataType, marshalErr := json.Marshal(change.Type)
		if marshalErr != nil {
			return marshalErr
		}
		position := int32(0)
		if change.First {
			position = 1
		}
		message.AddColumns = append(message.AddColumns, &fmsg.PbAddColumn{
			ColumnName: proto.String(change.Name), DataTypeJson: dataType,
			Comment: proto.String(change.Description), ColumnPositionType: proto.Int32(position),
		})
	}
	for _, name := range changes.Drop {
		if name == "" {
			return fmt.Errorf("%w: drop column name is empty", fgo.ErrInvalidConfig)
		}
		message.DropColumns = append(message.DropColumns, &fmsg.PbDropColumn{ColumnName: proto.String(name)})
	}
	for _, rename := range changes.Rename {
		if rename.Old == "" || rename.New == "" {
			return fmt.Errorf("%w: rename column names are required", fgo.ErrInvalidConfig)
		}
		message.RenameColumns = append(message.RenameColumns, &fmsg.PbRenameColumn{
			OldColumnName: proto.String(rename.Old), NewColumnName: proto.String(rename.New),
		})
	}
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

type PartitionSpec map[string]string

func (p PartitionSpec) validate() error {
	if len(p) == 0 {
		return fmt.Errorf("%w: partition spec is empty", fgo.ErrInvalidConfig)
	}
	for key, value := range p {
		if key == "" || value == "" {
			return fmt.Errorf("%w: partition keys and values are required", fgo.ErrInvalidConfig)
		}
	}
	return nil
}

func (p PartitionSpec) proto() *fmsg.PbPartitionSpec {
	keys := make([]string, 0, len(p))
	for key := range p {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	spec := &fmsg.PbPartitionSpec{}
	for _, key := range keys {
		spec.PartitionKeyValues = append(spec.PartitionKeyValues, &fmsg.PbKeyValue{
			Key: proto.String(key), Value: proto.String(p[key]),
		})
	}
	return spec
}

type Partition struct {
	ID   int64
	Spec PartitionSpec
}

func (c *Client) CreatePartition(ctx context.Context, path fgo.TablePath, spec PartitionSpec, ignoreIfExists bool) error {
	if err := path.Validate(); err != nil {
		return err
	}
	if err := spec.validate(); err != nil {
		return err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyCreatePartition, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.CreatePartitionRequest)
	message.TablePath, message.PartitionSpec = pbTablePath(path), spec.proto()
	message.IgnoreIfNotExists = proto.Bool(ignoreIfExists)
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func (c *Client) DropPartition(ctx context.Context, path fgo.TablePath, spec PartitionSpec, ignoreIfNotExists bool) error {
	if err := path.Validate(); err != nil {
		return err
	}
	if err := spec.validate(); err != nil {
		return err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyDropPartition, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.DropPartitionRequest)
	message.TablePath, message.PartitionSpec = pbTablePath(path), spec.proto()
	message.IgnoreIfNotExists = proto.Bool(ignoreIfNotExists)
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func (c *Client) ListPartitions(ctx context.Context, path fgo.TablePath, partial PartitionSpec) ([]Partition, error) {
	if err := path.Validate(); err != nil {
		return nil, err
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyListPartitionInfos, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.ListPartitionInfosRequest)
	message.TablePath = pbTablePath(path)
	if len(partial) != 0 {
		if err := partial.validate(); err != nil {
			return nil, err
		}
		message.PartialPartitionSpec = partial.proto()
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return nil, err
	}
	list, ok := response.Message().(*fmsg.ListPartitionInfosResponse)
	if !ok {
		return nil, unexpected("list partitions", response)
	}
	partitions := make([]Partition, len(list.GetPartitionsInfo()))
	for index, item := range list.GetPartitionsInfo() {
		partitions[index] = Partition{ID: item.GetPartitionId(), Spec: partitionSpec(item.GetPartitionSpec())}
	}
	return partitions, nil
}

type OffsetResult struct {
	Bucket int32
	Offset int64
	Err    error
}

func (c *Client) ListOffsets(
	ctx context.Context,
	table fgo.Table,
	partition fgo.PhysicalTablePath,
	partitionID int64,
	buckets []int32,
	spec fgo.ScanOffset,
) []OffsetResult {
	results := make([]OffsetResult, len(buckets))
	if err := spec.Validate(); err != nil || spec.Kind == fgo.ScanFromOffset {
		if err == nil {
			err = fmt.Errorf("%w: explicit offset is not a ListOffsets query", fgo.ErrInvalidConfig)
		}
		for index, bucket := range buckets {
			results[index] = OffsetResult{Bucket: bucket, Err: err}
		}
		return results
	}
	for index, bucket := range buckets {
		results[index].Bucket = bucket
		request, err := fmsg.NewRequest(fmsg.APIKeyListOffsets, 0)
		if err != nil {
			results[index].Err = err
			continue
		}
		message := request.Message().(*fmsg.ListOffsetsRequest)
		message.FollowerServerId = proto.Int32(-1)
		message.TableId = proto.Int64(table.ID)
		message.BucketId = []int32{bucket}
		switch spec.Kind {
		case fgo.ScanFromEarliest:
			message.OffsetType = proto.Int32(0)
		case fgo.ScanFromLatest:
			message.OffsetType = proto.Int32(1)
		case fgo.ScanFromTimestamp:
			message.OffsetType = proto.Int32(2)
			message.StartTimestamp = proto.Int64(spec.Timestamp.UnixMilli())
		}
		if partitionID >= 0 {
			message.PartitionId = proto.Int64(partitionID)
		}
		response, err := c.requester.RequestBucket(ctx, partition, bucket, request)
		if err != nil {
			results[index].Err = err
			continue
		}
		offsets, ok := response.Message().(*fmsg.ListOffsetsResponse)
		if !ok || len(offsets.GetBucketsResp()) != 1 || offsets.GetBucketsResp()[0].GetBucketId() != bucket {
			results[index].Err = fmt.Errorf("%w: ListOffsets omitted bucket %d", fgo.ErrValidation, bucket)
			continue
		}
		item := offsets.GetBucketsResp()[0]
		results[index].Offset = item.GetOffset()
		results[index].Err = fgo.ResponseError(item.GetErrorCode(), item.GetErrorMessage(), fmsg.APIKeyListOffsets)
	}
	return results
}

func partitionSpec(spec *fmsg.PbPartitionSpec) PartitionSpec {
	result := make(PartitionSpec)
	for _, item := range spec.GetPartitionKeyValues() {
		result[item.GetKey()] = item.GetValue()
	}
	return result
}

func pbTablePath(path fgo.TablePath) *fmsg.PbTablePath {
	return &fmsg.PbTablePath{DatabaseName: proto.String(path.Database), TableName: proto.String(path.Table)}
}

func validateName(kind, name string) error {
	if name == "" || strings.TrimSpace(name) != name {
		return fmt.Errorf("%w: invalid %s name", fgo.ErrInvalidConfig, kind)
	}
	return nil
}

func unexpected(operation string, response fmsg.Response) error {
	return fmt.Errorf("fadm: %s: unexpected response %T", operation, response.Message())
}

func millis(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.UnixMilli(value)
}

func nonNilMap(value map[string]string) map[string]string {
	if value == nil {
		return map[string]string{}
	}
	return value
}
