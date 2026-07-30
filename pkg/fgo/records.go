package fgo

import (
	"fmt"
	"strings"
	"time"
)

type PhysicalTablePath struct {
	TablePath
	Partition map[string]string
}

func (p PhysicalTablePath) Validate() error {
	if err := p.TablePath.Validate(); err != nil {
		return err
	}
	for key, value := range p.Partition {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: invalid partition spec", ErrInvalidConfig)
		}
	}
	return nil
}

type TableInfo struct {
	ID       int64
	Table    Table
	SchemaID int64
}

type TableBucket struct {
	TableID     int64
	PartitionID int64
	BucketID    int32
	Leader      Node
}

func (b TableBucket) Validate() error {
	if b.TableID < 0 || b.PartitionID < -1 || b.BucketID < 0 || b.Leader.Address == "" || b.Leader.Role != TabletServer {
		return fmt.Errorf("%w: invalid table bucket", ErrInvalidConfig)
	}
	return nil
}

type NamedRow map[string]any

func (s Schema) RowFromNamed(row NamedRow) (Row, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if len(row) != len(s.Columns) {
		return nil, fmt.Errorf("%w: got %d values for %d columns", ErrInvalidRow, len(row), len(s.Columns))
	}
	values := make(Row, len(s.Columns))
	for i, column := range s.Columns {
		value, ok := row[column.Name]
		if !ok {
			return nil, fmt.Errorf("%w: missing column %q", ErrInvalidRow, column.Name)
		}
		values[i] = value
	}
	if err := s.ValidateRow(values, nil); err != nil {
		return nil, err
	}
	return values, nil
}

type PrimaryKey Row

func (s Schema) ValidatePrimaryKey(key PrimaryKey) error {
	return s.ValidateRow(Row(key), s.PrimaryKey)
}

type ChangeType int8

const (
	Append ChangeType = iota
	Upsert
	Delete
)

func (c ChangeType) Validate() error {
	if c < Append || c > Delete {
		return fmt.Errorf("%w: invalid change type", ErrInvalidRow)
	}
	return nil
}

type Record struct {
	Key       PrimaryKey
	Value     Row
	Change    ChangeType
	Timestamp time.Time
	Offset    int64
}

func (r Record) Validate(schema Schema) error {
	if err := r.Change.Validate(); err != nil {
		return err
	}
	if r.Offset < -1 {
		return fmt.Errorf("%w: invalid offset", ErrInvalidRow)
	}
	if err := schema.ValidatePrimaryKey(r.Key); err != nil {
		return err
	}
	if r.Change != Delete {
		return schema.ValidateRow(r.Value, nil)
	}
	return nil
}

type OffsetSpec struct {
	Offset    int64
	Timestamp time.Time
}

func (o OffsetSpec) Validate() error {
	if o.Offset < -1 {
		return fmt.Errorf("%w: invalid offset", ErrInvalidConfig)
	}
	if o.Offset >= 0 && !o.Timestamp.IsZero() {
		return fmt.Errorf("%w: offset and timestamp are mutually exclusive", ErrInvalidConfig)
	}
	return nil
}
