package fgo

import (
	"fmt"
	"slices"
)

// kvKeyLayout derives wire keys without changing the application's logical
// primary key or the row schema. Fluss addresses a partition separately.
type kvKeyLayout struct {
	primary   []string
	bucket    []string
	partition []physicalPartitionColumn
}

func newKVKeyLayout(schema Schema, partition string) (kvKeyLayout, error) {
	keys := kvKeyLayout{primary: slices.DeleteFunc(slices.Clone(schema.PrimaryKey), func(name string) bool {
		return slices.Contains(schema.PartitionKey, name)
	})}
	if len(keys.primary) == 0 {
		return kvKeyLayout{}, fmt.Errorf("%w: no physical primary key columns", ErrInvalidSchema)
	}
	keys.bucket = slices.Clone(schema.BucketKey)
	if len(keys.bucket) == 0 {
		keys.bucket = keys.primary
	}
	for _, name := range keys.bucket {
		if !slices.Contains(keys.primary, name) {
			return kvKeyLayout{}, fmt.Errorf("%w: bucket key %q is not part of the physical primary key", ErrInvalidSchema, name)
		}
	}
	columns, err := kvPartitionColumns(schema, partition)
	if err != nil {
		return kvKeyLayout{}, err
	}
	keys.partition = columns
	return keys, nil
}

func (k kvKeyLayout) validatePartition(values map[string]any) error {
	for _, partition := range k.partition {
		// A leading logical prefix can omit partition columns that appear
		// later in the primary key. The selected partition still scopes it.
		if value, present := values[partition.name]; present && !partition.matches(value) {
			return fmt.Errorf("%w: key column %q differs from the selected partition", ErrInvalidRow, partition.name)
		}
	}
	return nil
}

func (k kvKeyLayout) encodePoint(schema Schema, values map[string]any, bucketCount int32) ([]byte, int32, error) {
	return k.encode(schema, k.primary, values, bucketCount)
}

func (k kvKeyLayout) encodePrefix(schema Schema, values map[string]any, bucketCount int32) ([]byte, int32, error) {
	if len(k.bucket) > len(k.primary) || !slices.Equal(k.primary[:len(k.bucket)], k.bucket) {
		return nil, 0, fmt.Errorf("%w: bucket key is not a leading physical primary-key prefix", ErrInvalidSchema)
	}
	count := 0
	for _, name := range k.primary {
		if _, present := values[name]; !present {
			break
		}
		count++
	}
	if count < len(k.bucket) {
		return nil, 0, fmt.Errorf("%w: prefix does not contain the complete bucket key", ErrInvalidRow)
	}
	return k.encode(schema, k.primary[:count], values, bucketCount)
}

func (k kvKeyLayout) encode(schema Schema, names []string, values map[string]any, bucketCount int32) ([]byte, int32, error) {
	if err := k.validatePartition(values); err != nil {
		return nil, 0, err
	}
	wire, err := encodeKVKeyColumns(schema, names, values)
	if err != nil {
		return nil, 0, err
	}
	bucketKey := wire
	if !slices.Equal(names, k.bucket) {
		bucketKey, err = encodeKVKeyColumns(schema, k.bucket, values)
		if err != nil {
			return nil, 0, err
		}
	}
	bucket, err := flussBucket(bucketKey, int(bucketCount))
	return wire, bucket, err
}

func encodeKVKeyColumns(schema Schema, names []string, values map[string]any) ([]byte, error) {
	key := make(PrimaryKey, len(names))
	for i, name := range names {
		key[i] = values[name]
	}
	return encodeKeyColumns(schema, names, key)
}

func rowValues(schema Schema, row Row, columns []string) map[string]any {
	if len(columns) == 0 {
		columns = schema.selectedColumns(nil)
	}
	values := make(map[string]any, len(columns))
	for index, name := range columns {
		values[name] = row[index]
	}
	return values
}
