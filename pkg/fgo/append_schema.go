package fgo

import (
	"maps"
	"slices"
)

// Snapshot only metadata, never caller rows. The writer owns this tree for its
// lifetime, so per-row validation need not rebuild or revalidate schema metadata.
func snapshotAppendTable(table Table) Table {
	schema := table.Schema
	schema.Columns = slices.Clone(schema.Columns)
	for index := range schema.Columns {
		if logical := schema.Columns[index].LogicalType; logical != nil {
			snapshot := snapshotAppendLogicalType(*logical)
			schema.Columns[index].LogicalType = &snapshot
		}
	}
	schema.PrimaryKey = slices.Clone(schema.PrimaryKey)
	schema.BucketKey = slices.Clone(schema.BucketKey)
	schema.PartitionKey = slices.Clone(schema.PartitionKey)
	schema.AutoIncrement = slices.Clone(schema.AutoIncrement)
	table.Schema, table.Properties = schema, maps.Clone(table.Properties)
	return table
}

func snapshotAppendLogicalType(logical LogicalType) LogicalType {
	for _, pointer := range []**LogicalType{&logical.Element, &logical.Key, &logical.Value} {
		if *pointer != nil {
			snapshot := snapshotAppendLogicalType(**pointer)
			*pointer = &snapshot
		}
	}
	logical.Fields = slices.Clone(logical.Fields)
	for index := range logical.Fields {
		logical.Fields[index].Type = snapshotAppendLogicalType(logical.Fields[index].Type)
	}
	return logical
}

func appendBucketKeyPositions(schema Schema) []int {
	if len(schema.BucketKey) == 0 {
		return nil
	}
	byName := make(map[string]int, len(schema.Columns))
	for index, column := range schema.Columns {
		byName[column.Name] = index
	}
	positions := make([]int, len(schema.BucketKey))
	for index, name := range schema.BucketKey {
		positions[index] = byName[name] // References were validated before snapshotting.
	}
	return positions
}
