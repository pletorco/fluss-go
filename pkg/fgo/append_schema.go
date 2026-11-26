package fgo

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
