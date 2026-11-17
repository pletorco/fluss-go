package fgo

import "maps"

// Snapshot only metadata, never caller rows. The writer owns this tree for its
// lifetime, so per-row validation need not rebuild or revalidate schema metadata.
func snapshotAppendTable(table Table) Table {
	table.Schema, table.Properties = snapshotSchema(table.Schema), maps.Clone(table.Properties)
	return table
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
