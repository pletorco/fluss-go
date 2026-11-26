package fgo

import (
	"maps"
	"slices"
)

// snapshotTable retains detached metadata, not caller rows.
func snapshotTable(table Table) Table {
	table.Schema = snapshotSchema(table.Schema)
	table.Properties = maps.Clone(table.Properties)
	return table
}

// snapshotSchema detaches every mutable part of schema metadata. Callers use it
// at ownership boundaries; internal schema resolvers borrow immutable snapshots.
func snapshotSchema(schema Schema) Schema {
	schema.Columns = slices.Clone(schema.Columns)
	for index := range schema.Columns {
		if logical := schema.Columns[index].LogicalType; logical != nil {
			snapshot := snapshotLogicalType(*logical)
			schema.Columns[index].LogicalType = &snapshot
		}
	}
	schema.PrimaryKey = slices.Clone(schema.PrimaryKey)
	schema.BucketKey = slices.Clone(schema.BucketKey)
	schema.PartitionKey = slices.Clone(schema.PartitionKey)
	schema.AutoIncrement = slices.Clone(schema.AutoIncrement)
	return schema
}

func snapshotLogicalType(logical LogicalType) LogicalType {
	for _, pointer := range []**LogicalType{&logical.Element, &logical.Key, &logical.Value} {
		if *pointer != nil {
			snapshot := snapshotLogicalType(**pointer)
			*pointer = &snapshot
		}
	}
	logical.Fields = slices.Clone(logical.Fields)
	for index := range logical.Fields {
		logical.Fields[index].Type = snapshotLogicalType(logical.Fields[index].Type)
	}
	return logical
}
