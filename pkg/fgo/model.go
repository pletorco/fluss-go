package fgo

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
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
	Name     string
	Type     DataType
	Nullable bool
}

type Schema struct {
	Columns      []Column
	PrimaryKey   []string
	BucketKey    []string
	PartitionKey []string
}

func (s Schema) Validate() error {
	if len(s.Columns) == 0 {
		return fmt.Errorf("%w: schema has no columns", ErrInvalidSchema)
	}
	columns := make(map[string]Column, len(s.Columns))
	for _, column := range s.Columns {
		if column.Name == "" || strings.TrimSpace(column.Name) != column.Name {
			return fmt.Errorf("%w: invalid column name", ErrInvalidSchema)
		}
		if _, exists := columns[column.Name]; exists {
			return fmt.Errorf("%w: duplicate column %q", ErrInvalidSchema, column.Name)
		}
		if err := column.Type.Validate(); err != nil {
			return err
		}
		columns[column.Name] = column
	}
	for _, keys := range [][]string{s.PrimaryKey, s.BucketKey, s.PartitionKey} {
		for _, key := range keys {
			if _, exists := columns[key]; !exists {
				return fmt.Errorf("%w: key column %q does not exist", ErrInvalidSchema, key)
			}
		}
	}
	return nil
}

func (s Schema) JSON() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(s)
}

func ParseSchemaJSON(data []byte) (Schema, error) {
	var schema Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return Schema{}, fmt.Errorf("%w: %v", ErrInvalidSchema, err)
	}
	if err := schema.Validate(); err != nil {
		return Schema{}, err
	}
	return schema, nil
}

type Row []any

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
	if validValue(column.Type, value) {
		return nil
	}
	return fmt.Errorf("%w: column %q expects %s", ErrInvalidRow, column.Name, column.Type)
}

func validValue(kind DataType, value any) bool {
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
		return isSlice(value)
	case MapType:
		return isMap(value)
	case RowType:
		_, ok := value.(Row)
		return ok
	default:
		return false
	}
}

func isSlice(value any) bool {
	_, ok := value.([]any)
	return ok
}

func isMap(value any) bool {
	_, ok := value.(map[string]any)
	return ok
}

type TableKind string

const (
	LogTable        TableKind = "LOG"
	PrimaryKeyTable TableKind = "PRIMARY_KEY"
)

type Table struct {
	Path   TablePath
	Kind   TableKind
	Schema Schema
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

func (c *Client) OpenTable(path TablePath, kind TableKind, schema Schema) (Table, error) {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return Table{}, ErrClosed
	}
	if err := path.Validate(); err != nil {
		return Table{}, err
	}
	if kind != LogTable && kind != PrimaryKeyTable {
		return Table{}, fmt.Errorf("%w: table kind %q", ErrTableKind, kind)
	}
	if err := schema.Validate(); err != nil {
		return Table{}, err
	}
	return Table{Path: path, Kind: kind, Schema: schema}, nil
}
