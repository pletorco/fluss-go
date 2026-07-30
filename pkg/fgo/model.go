package fgo

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidSchema = errors.New("fgo: invalid schema")
	ErrInvalidRow    = errors.New("fgo: invalid row")
	ErrTableKind     = errors.New("fgo: unsupported table operation")
)

type DataType string

const (
	BoolType      DataType = "BOOLEAN"
	IntType       DataType = "INT"
	BigIntType    DataType = "BIGINT"
	FloatType     DataType = "FLOAT"
	DoubleType    DataType = "DOUBLE"
	StringType    DataType = "STRING"
	BinaryType    DataType = "BINARY"
	DateType      DataType = "DATE"
	TimestampType DataType = "TIMESTAMP"
)

func (t DataType) Validate() error {
	switch t {
	case BoolType, IntType, BigIntType, FloatType, DoubleType, StringType, BinaryType, DateType, TimestampType:
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
	if len(columns) == 0 {
		columns = make([]string, len(s.Columns))
		for i, column := range s.Columns {
			columns[i] = column.Name
		}
	}
	if len(row) != len(columns) {
		return fmt.Errorf("%w: got %d values for %d columns", ErrInvalidRow, len(row), len(columns))
	}
	byName := make(map[string]Column, len(s.Columns))
	for _, column := range s.Columns {
		byName[column.Name] = column
	}
	for i, name := range columns {
		column, ok := byName[name]
		if !ok {
			return fmt.Errorf("%w: unknown column %q", ErrInvalidRow, name)
		}
		if row[i] == nil {
			if !column.Nullable {
				return fmt.Errorf("%w: column %q is not nullable", ErrInvalidRow, name)
			}
			continue
		}
		if !validValue(column.Type, row[i]) {
			return fmt.Errorf("%w: column %q expects %s", ErrInvalidRow, name, column.Type)
		}
	}
	return nil
}

func validValue(kind DataType, value any) bool {
	switch kind {
	case BoolType:
		_, ok := value.(bool)
		return ok
	case IntType:
		_, ok := value.(int32)
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
	case StringType:
		_, ok := value.(string)
		return ok
	case BinaryType:
		_, ok := value.([]byte)
		return ok
	default:
		return true
	}
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
