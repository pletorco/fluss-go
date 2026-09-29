package fgo

import (
	"context"
	"encoding/binary"
)

// Test-only wrappers that decode with a fixed in-memory schema.

func decodeLookupValue(table Table, value []byte) (Row, error) {
	return decodeLookupValueWithResolver(
		context.Background(),
		fixedSchemaResolver{path: table.Path, schemaID: table.SchemaID, schema: table.Schema},
		table,
		value,
	)
}

func decodeFetchedLog(
	schema Schema,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	return decodeFetchedLogFormat(schema, bucket, fetchOffset, encoded, true)
}

func decodeFetchedLogFormat(
	schema Schema,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
	compacted bool,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	table := Table{
		SchemaID: schemaIDFromFetched(encoded),
		Path:     TablePath{Database: "_", Table: "_"},
		Schema:   schema,
	}
	return decodeFetchedLogWithResolver(
		context.Background(),
		fixedSchemaResolver{path: table.Path, schemaID: table.SchemaID, schema: schema},
		table,
		bucket,
		fetchOffset,
		encoded,
		compacted,
	)
}

func schemaIDFromFetched(encoded []byte) int32 {
	if len(encoded) < logBatchV0HeaderSize {
		return 0
	}
	size, err := fetchedBatchSize(encoded)
	if err != nil {
		return 0
	}
	_, _, schemaOffset, err := logBatchHeader(encoded[:size])
	if err != nil {
		return 0
	}
	return int32(int16(binary.LittleEndian.Uint16(encoded[schemaOffset:])))
}
