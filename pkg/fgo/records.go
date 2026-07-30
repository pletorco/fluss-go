package fgo

import (
	"fmt"
	"strings"
	"time"
)

// PhysicalTablePath identifies a logical table and optional named partition.
type PhysicalTablePath struct {
	TablePath
	Partition string
}

// Validate checks the logical path and optional partition name.
func (p PhysicalTablePath) Validate() error {
	if err := p.TablePath.Validate(); err != nil {
		return err
	}
	if p.Partition != "" && strings.TrimSpace(p.Partition) == "" {
		return fmt.Errorf("%w: invalid partition name", ErrInvalidConfig)
	}
	return nil
}

func (p PhysicalTablePath) String() string {
	if p.Partition == "" {
		return p.TablePath.String()
	}
	return p.TablePath.String() + "(p=" + p.Partition + ")"
}

// TableInfo combines table identity, schema identity, and table metadata.
type TableInfo struct {
	ID       int64
	Table    Table
	SchemaID int64
}

// TableBucket is a resolved bucket and its current tablet leader.
type TableBucket struct {
	TableID     int64
	PartitionID int64
	BucketID    int32
	Leader      Node
}

// Validate checks IDs and leader metadata.
func (b TableBucket) Validate() error {
	if b.TableID < 0 || b.PartitionID < -1 || b.BucketID < 0 || b.Leader.Address == "" || b.Leader.Role != TabletServer {
		return fmt.Errorf("%w: invalid table bucket", ErrInvalidConfig)
	}
	return nil
}

// NamedRow maps every schema column name to one value.
type NamedRow map[string]any

// RowFromNamed validates a named row and returns values in schema order.
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

// PrimaryKey stores key values in schema primary-key order.
type PrimaryKey Row

// ValidatePrimaryKey checks key values against primary-key columns.
func (s Schema) ValidatePrimaryKey(key PrimaryKey) error {
	return s.ValidateRow(Row(key), s.PrimaryKey)
}

// ChangeType identifies the row-level change carried by a log record.
type ChangeType int8

// Change types encoded by Apache Fluss 0.9.1.
const (
	Append ChangeType = iota
	Insert
	UpdateBefore
	UpdateAfter
	Delete

	// Upsert is the idempotent update-after change used by primary-key tables.
	Upsert = UpdateAfter
)

// Validate reports an error for an unsupported change type.
func (c ChangeType) Validate() error {
	if c < Append || c > Delete {
		return fmt.Errorf("%w: invalid change type", ErrInvalidRow)
	}
	return nil
}

// Record is one keyed or unkeyed row change and its log metadata.
type Record struct {
	Key       PrimaryKey
	Value     Row
	Change    ChangeType
	Timestamp time.Time
	Offset    int64
}

// Validate checks the change, offset, key, and value against schema.
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

// OffsetSpec selects an explicit offset or timestamp lookup.
type OffsetSpec struct {
	Offset    int64
	Timestamp time.Time
}

// Validate checks offset range and timestamp exclusivity.
func (o OffsetSpec) Validate() error {
	if o.Offset < -1 {
		return fmt.Errorf("%w: invalid offset", ErrInvalidConfig)
	}
	if o.Offset >= 0 && !o.Timestamp.IsZero() {
		return fmt.Errorf("%w: offset and timestamp are mutually exclusive", ErrInvalidConfig)
	}
	return nil
}
