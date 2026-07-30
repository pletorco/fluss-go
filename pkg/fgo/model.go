package fgo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidSchema = errors.New("fgo: invalid schema")
	ErrInvalidRow    = errors.New("fgo: invalid row")
	ErrTableKind     = errors.New("fgo: unsupported table operation")
)

type DataType string

const (
	BoolType         DataType = "BOOLEAN"
	CharType         DataType = "CHAR"
	IntType          DataType = "INT"
	TinyIntType      DataType = "TINYINT"
	SmallIntType     DataType = "SMALLINT"
	BigIntType       DataType = "BIGINT"
	FloatType        DataType = "FLOAT"
	DoubleType       DataType = "DOUBLE"
	StringType       DataType = "STRING"
	BinaryType       DataType = "BINARY"
	BytesType        DataType = "BYTES"
	DecimalType      DataType = "DECIMAL"
	DateType         DataType = "DATE"
	TimeType         DataType = "TIME_WITHOUT_TIME_ZONE"
	TimestampType    DataType = "TIMESTAMP"
	TimestampLTZType DataType = "TIMESTAMP_WITH_LOCAL_TIME_ZONE"
	ArrayType        DataType = "ARRAY"
	MapType          DataType = "MAP"
	RowType          DataType = "ROW"
)

func (t DataType) Validate() error {
	switch t {
	case BoolType, CharType, IntType, TinyIntType, SmallIntType, BigIntType, FloatType, DoubleType, StringType, BinaryType, BytesType, DecimalType, DateType, TimeType, TimestampType, TimestampLTZType, ArrayType, MapType, RowType:
		return nil
	default:
		return fmt.Errorf("%w: unsupported data type %q", ErrInvalidSchema, t)
	}
}

type Column struct {
	Name        string
	Type        DataType
	Nullable    bool
	LogicalType *LogicalType
	Description string
	ID          int
}

type Schema struct {
	Columns        []Column
	PrimaryKey     []string
	BucketKey      []string
	PartitionKey   []string
	AutoIncrement  []string
	HighestFieldID int
}

func (s Schema) Validate() error {
	if len(s.Columns) == 0 {
		return fmt.Errorf("%w: schema has no columns", ErrInvalidSchema)
	}
	columns, err := s.validateColumns()
	if err != nil {
		return err
	}
	for _, keys := range [][]string{s.PrimaryKey, s.BucketKey, s.PartitionKey} {
		if err := validateSchemaKeys(columns, keys); err != nil {
			return err
		}
	}
	return nil
}

func (s Schema) validateColumns() (map[string]Column, error) {
	columns := make(map[string]Column, len(s.Columns))
	for _, column := range s.Columns {
		if column.Name == "" || strings.TrimSpace(column.Name) != column.Name {
			return nil, fmt.Errorf("%w: invalid column name", ErrInvalidSchema)
		}
		if _, exists := columns[column.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate column %q", ErrInvalidSchema, column.Name)
		}
		if column.LogicalType != nil {
			if err := column.LogicalType.Validate(); err != nil {
				return nil, err
			}
		} else if err := column.Type.Validate(); err != nil {
			return nil, err
		}
		columns[column.Name] = column
	}
	return columns, nil
}

func validateSchemaKeys(columns map[string]Column, keys []string) error {
	for _, key := range keys {
		if _, exists := columns[key]; !exists {
			return fmt.Errorf("%w: key column %q does not exist", ErrInvalidSchema, key)
		}
	}
	return nil
}

func (s Schema) JSON() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	assignIDs := s.hasUnassignedFieldIDs()
	nextID := 0
	columns := make([]schemaColumnJSON, len(s.Columns))
	for i, column := range s.Columns {
		logicalType := logicalTypeForColumn(column)
		id := column.ID
		if assignIDs {
			id = nextID
			nextID++
			assignLogicalFieldIDs(&logicalType, &nextID)
		}
		columns[i] = schemaColumnJSON{Name: column.Name, DataType: logicalType, Comment: column.Description, ID: id}
	}
	highestFieldID := s.HighestFieldID
	if assignIDs {
		highestFieldID = nextID - 1
	}
	return json.Marshal(schemaJSON{
		Version: 1, Columns: columns, PrimaryKey: s.PrimaryKey,
		AutoIncrement: s.AutoIncrement, HighestFieldID: highestFieldID,
	})
}

func (s Schema) hasUnassignedFieldIDs() bool {
	if s.HighestFieldID != 0 {
		return false
	}
	for _, column := range s.Columns {
		if column.ID != 0 || !logicalFieldIDsAreZero(logicalTypeForColumn(column)) {
			return false
		}
	}
	return true
}

func logicalFieldIDsAreZero(logicalType LogicalType) bool {
	for _, field := range logicalType.Fields {
		if field.ID != 0 || !logicalFieldIDsAreZero(field.Type) {
			return false
		}
	}
	if logicalType.Element != nil && !logicalFieldIDsAreZero(*logicalType.Element) {
		return false
	}
	if logicalType.Key != nil && !logicalFieldIDsAreZero(*logicalType.Key) {
		return false
	}
	return logicalType.Value == nil || logicalFieldIDsAreZero(*logicalType.Value)
}

func assignLogicalFieldIDs(logicalType *LogicalType, nextID *int) {
	for index := range logicalType.Fields {
		logicalType.Fields[index].ID = *nextID
		*nextID++
		assignLogicalFieldIDs(&logicalType.Fields[index].Type, nextID)
	}
	if logicalType.Element != nil {
		assignLogicalFieldIDs(logicalType.Element, nextID)
	}
	if logicalType.Key != nil {
		assignLogicalFieldIDs(logicalType.Key, nextID)
	}
	if logicalType.Value != nil {
		assignLogicalFieldIDs(logicalType.Value, nextID)
	}
}

func ParseSchemaJSON(data []byte) (Schema, error) {
	var encoded schemaJSON
	if err := json.Unmarshal(data, &encoded); err != nil {
		return Schema{}, fmt.Errorf("%w: %v", ErrInvalidSchema, err)
	}
	schema := Schema{PrimaryKey: encoded.PrimaryKey, AutoIncrement: encoded.AutoIncrement, HighestFieldID: encoded.HighestFieldID, Columns: make([]Column, len(encoded.Columns))}
	for i, column := range encoded.Columns {
		if err := column.DataType.Validate(); err != nil {
			return Schema{}, err
		}
		schema.Columns[i] = Column{Name: column.Name, Type: dataTypeForLogicalType(column.DataType), Nullable: column.DataType.Nullable, LogicalType: &column.DataType, Description: column.Comment, ID: column.ID}
	}
	if err := schema.Validate(); err != nil {
		return Schema{}, err
	}
	return schema, nil
}

type schemaJSON struct {
	Version        int                `json:"version"`
	Columns        []schemaColumnJSON `json:"columns"`
	PrimaryKey     []string           `json:"primary_key,omitempty"`
	AutoIncrement  []string           `json:"auto_increment_column,omitempty"`
	HighestFieldID int                `json:"highest_field_id"`
}

type schemaColumnJSON struct {
	Name     string      `json:"name"`
	DataType LogicalType `json:"data_type"`
	Comment  string      `json:"comment,omitempty"`
	ID       int         `json:"id"`
}

func logicalTypeForColumn(column Column) LogicalType {
	if column.LogicalType != nil {
		return *column.LogicalType
	}
	return LogicalType{Root: logicalRoot(column.Type), Nullable: column.Nullable}
}

func dataTypeForLogicalType(logicalType LogicalType) DataType {
	switch logicalType.Root {
	case "INTEGER":
		return IntType
	case "TIMESTAMP_WITHOUT_TIME_ZONE":
		return TimestampType
	default:
		return DataType(logicalType.Root)
	}
}

func logicalRoot(dataType DataType) string {
	switch dataType {
	case IntType:
		return "INTEGER"
	case TimestampType:
		return "TIMESTAMP_WITHOUT_TIME_ZONE"
	default:
		return string(dataType)
	}
}

type Row []any

// MapEntry preserves Fluss map key types and iteration order. Go maps with string keys are also
// accepted as input, but decoded map values always use Map.
type MapEntry struct {
	Key   any
	Value any
}

type Map []MapEntry

func (s Schema) ValidateRow(row Row, columns []string) error {
	if err := s.Validate(); err != nil {
		return err
	}
	columns = s.selectedColumns(columns)
	if len(row) != len(columns) {
		return fmt.Errorf("%w: got %d values for %d columns", ErrInvalidRow, len(row), len(columns))
	}
	byName := s.columnsByName()
	for i, name := range columns {
		column, ok := byName[name]
		if !ok {
			return fmt.Errorf("%w: unknown column %q", ErrInvalidRow, name)
		}
		if err := validateColumnValue(column, row[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s Schema) selectedColumns(columns []string) []string {
	if len(columns) != 0 {
		return columns
	}
	columns = make([]string, len(s.Columns))
	for i, column := range s.Columns {
		columns[i] = column.Name
	}
	return columns
}

func (s Schema) columnsByName() map[string]Column {
	columns := make(map[string]Column, len(s.Columns))
	for _, column := range s.Columns {
		columns[column.Name] = column
	}
	return columns
}

func validateColumnValue(column Column, value any) error {
	if value == nil {
		if column.Nullable {
			return nil
		}
		return fmt.Errorf("%w: column %q is not nullable", ErrInvalidRow, column.Name)
	}
	if column.LogicalType == nil && validUntypedValue(column.Type, value) {
		return nil
	}
	if column.LogicalType != nil && validLogicalValue(*column.LogicalType, value) {
		return nil
	}
	return fmt.Errorf("%w: column %q expects %s", ErrInvalidRow, column.Name, column.Type)
}

func validUntypedValue(kind DataType, value any) bool {
	switch kind {
	case BoolType:
		_, ok := value.(bool)
		return ok
	case IntType:
		_, ok := value.(int32)
		return ok
	case TinyIntType:
		_, ok := value.(int8)
		return ok
	case SmallIntType:
		_, ok := value.(int16)
		return ok
	case BigIntType:
		_, ok := value.(int64)
		return ok
	case FloatType:
		_, ok := value.(float32)
		return ok
	case DoubleType:
		_, ok := value.(float64)
		return ok
	case StringType, CharType:
		_, ok := value.(string)
		return ok
	case BinaryType, BytesType:
		_, ok := value.([]byte)
		return ok
	case DecimalType:
		_, ok := value.(*big.Rat)
		return ok
	case DateType, TimeType, TimestampType, TimestampLTZType:
		_, ok := value.(time.Time)
		return ok
	case ArrayType:
		_, ok := value.([]any)
		return ok
	case MapType:
		_, ok := mapEntries(value)
		return ok
	case RowType:
		_, ok := value.(Row)
		return ok
	default:
		return false
	}
}

func validLogicalValue(logicalType LogicalType, value any) bool {
	if value == nil {
		return logicalType.Nullable
	}
	kind := dataTypeForLogicalType(logicalType)
	switch kind {
	case ArrayType:
		return validLogicalArray(logicalType.Element, value)
	case MapType:
		return validLogicalMap(logicalType.Key, logicalType.Value, value)
	case RowType:
		return validLogicalRow(logicalType.Fields, value)
	default:
		return validUntypedValue(kind, value)
	}
}

func validLogicalArray(elementType *LogicalType, value any) bool {
	values, ok := value.([]any)
	if !ok || elementType == nil {
		return false
	}
	for _, element := range values {
		if !validLogicalValue(*elementType, element) {
			return false
		}
	}
	return true
}

func validLogicalMap(keyType, valueType *LogicalType, value any) bool {
	if keyType == nil || valueType == nil {
		return false
	}
	entries, ok := mapEntries(value)
	if !ok {
		return false
	}
	for _, entry := range entries {
		if !validLogicalValue(*keyType, entry.Key) || !validLogicalValue(*valueType, entry.Value) {
			return false
		}
	}
	return true
}

func validLogicalRow(fields []LogicalField, value any) bool {
	row, ok := value.(Row)
	if !ok || len(row) != len(fields) {
		return false
	}
	for index, field := range fields {
		if !validLogicalValue(field.Type, row[index]) {
			return false
		}
	}
	return true
}

func mapEntries(value any) (Map, bool) {
	switch value := value.(type) {
	case Map:
		return value, true
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		entries := make(Map, len(keys))
		for i, key := range keys {
			entries[i] = MapEntry{Key: key, Value: value[key]}
		}
		return entries, true
	default:
		return nil, false
	}
}

type TableKind string

const (
	LogTable        TableKind = "LOG"
	PrimaryKeyTable TableKind = "PRIMARY_KEY"
)

type Table struct {
	ID          int64
	SchemaID    int32
	Path        TablePath
	Kind        TableKind
	Schema      Schema
	BucketCount int
}

func (t Table) RequireLog() error {
	if t.Kind != LogTable {
		return fmt.Errorf("%w: %s is not a log table", ErrTableKind, t.Path)
	}
	return nil
}

func (t Table) RequirePrimaryKey() error {
	if t.Kind != PrimaryKeyTable {
		return fmt.Errorf("%w: %s is not a primary-key table", ErrTableKind, t.Path)
	}
	return nil
}

// OpenTable loads authoritative table metadata and schema from the coordinator.
func (c *Client) OpenTable(ctx context.Context, path TablePath) (Table, error) {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return Table{}, ErrClosed
	}
	if err := path.Validate(); err != nil {
		return Table{}, err
	}
	infoRequest, err := fmsg.NewRequest(fmsg.APIKeyGetTableInfo, 0)
	if err != nil {
		return Table{}, err
	}
	infoRequest.Message().(*fmsg.GetTableInfoRequest).TablePath = pbTablePath(path)
	infoResponse, err := c.RequestCoordinator(ctx, infoRequest)
	if err != nil {
		return Table{}, err
	}
	info, ok := infoResponse.Message().(*fmsg.GetTableInfoResponse)
	if !ok {
		return Table{}, fmt.Errorf("fgo: get table info: unexpected response %T", infoResponse.Message())
	}
	schemaRequest, err := fmsg.NewRequest(fmsg.APIKeyGetTableSchema, 0)
	if err != nil {
		return Table{}, err
	}
	schemaRequest.Message().(*fmsg.GetTableSchemaRequest).TablePath = pbTablePath(path)
	schemaRequest.Message().(*fmsg.GetTableSchemaRequest).SchemaId = proto.Int32(info.GetSchemaId())
	schemaResponse, err := c.RequestCoordinator(ctx, schemaRequest)
	if err != nil {
		return Table{}, err
	}
	schemaMessage, ok := schemaResponse.Message().(*fmsg.GetTableSchemaResponse)
	if !ok {
		return Table{}, fmt.Errorf("fgo: get table schema: unexpected response %T", schemaResponse.Message())
	}
	schema, err := ParseSchemaJSON(schemaMessage.GetSchemaJson())
	if err != nil {
		return Table{}, err
	}
	var descriptor struct {
		BucketKey    []string `json:"bucket_key"`
		PartitionKey []string `json:"partition_key"`
		BucketCount  int      `json:"bucket_count"`
	}
	if len(info.GetTableJson()) != 0 {
		if err := json.Unmarshal(info.GetTableJson(), &descriptor); err != nil {
			return Table{}, fmt.Errorf("%w: invalid table descriptor: %v", ErrInvalidSchema, err)
		}
		schema.BucketKey = descriptor.BucketKey
		schema.PartitionKey = descriptor.PartitionKey
		if err := schema.Validate(); err != nil {
			return Table{}, err
		}
	}
	kind := LogTable
	if len(schema.PrimaryKey) != 0 {
		kind = PrimaryKeyTable
	}
	return Table{
		ID: info.GetTableId(), SchemaID: schemaMessage.GetSchemaId(), Path: path,
		Kind: kind, Schema: schema, BucketCount: descriptor.BucketCount,
	}, nil
}
