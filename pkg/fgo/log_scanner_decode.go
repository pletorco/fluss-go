package fgo

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func decodeFetchedLogWithResolver(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
	compacted bool,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	return decodeFetchedLogBatchesWithResolver(
		ctx, resolver, table, bucket, fetchOffset, encoded,
		fetchedLogDecodeOptions{compacted: compacted},
	)
}

func decodeFetchedFetchResponseWithResolver(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
	compacted bool,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	return decodeFetchedLogBatchesWithResolver(
		ctx, resolver, table, bucket, fetchOffset, encoded,
		fetchedLogDecodeOptions{compacted: compacted, allowIncompleteTail: true},
	)
}

type fetchedLogDecodeOptions struct {
	compacted           bool
	allowIncompleteTail bool
}

func decodeFetchedLogBatchesWithResolver(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	bucket int32,
	fetchOffset int64,
	encoded []byte,
	options fetchedLogDecodeOptions,
) (int64, []ScanRecord, []ScanArrowBatch, error) {
	next := fetchOffset
	var rows []ScanRecord
	var arrows []ScanArrowBatch
	for len(encoded) != 0 {
		size, complete, err := completeFetchedBatchSize(encoded)
		if err != nil {
			releaseScanArrows(arrows)
			return fetchOffset, nil, nil, err
		}
		if !complete {
			if options.allowIncompleteTail {
				// Byte-limited fetches may end inside the next batch. Refetch it from
				// the last complete offset, matching the Fluss Java scanner.
				break
			}
			releaseScanArrows(arrows)
			return fetchOffset, nil, nil, incompleteFetchedBatchError(encoded)
		}
		payload := encoded[:size]
		batchNext, batchRows, arrowBatch, err := decodeEvolvedFetchedBatch(
			ctx, resolver, table, bucket, next, payload, options.compacted,
		)
		if err != nil {
			releaseScanArrows(arrows)
			return fetchOffset, nil, nil, err
		}
		next = batchNext
		rows = append(rows, batchRows...)
		if arrowBatch != nil {
			arrows = append(arrows, *arrowBatch)
		}
		encoded = encoded[size:]
	}
	return next, rows, arrows, nil
}

func decodeEvolvedFetchedBatch(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	bucket int32,
	offset int64,
	payload []byte,
	compacted bool,
) (int64, []ScanRecord, *ScanArrowBatch, error) {
	_, _, schemaOffset, err := logBatchHeader(payload)
	if err != nil {
		return offset, nil, nil, err
	}
	schemaID := int32(int16(binary.LittleEndian.Uint16(payload[schemaOffset:])))
	writerSchema, err := resolver.resolveSchema(ctx, table.Path, schemaID)
	if err != nil {
		return offset, nil, nil, fmt.Errorf("fgo: resolve log schema %d: %w", schemaID, err)
	}
	next, rows, arrowBatch, err := decodeFetchedBatch(
		writerSchema, bucket, offset, payload, compacted,
	)
	if err != nil {
		return offset, nil, nil, err
	}
	if err := evolveScanRecords(rows, writerSchema, table.Schema); err != nil {
		return offset, nil, nil, err
	}
	if arrowBatch != nil {
		if err := evolveArrowBatch(&arrowBatch.Batch, writerSchema, table.Schema); err != nil {
			arrowBatch.Batch.Release()
			return offset, nil, nil, err
		}
	}
	return next, rows, arrowBatch, nil
}

func evolveScanRecords(records []ScanRecord, source, target Schema) error {
	for index := range records {
		row, err := evolveRow(source, target, records[index].Record.Value)
		if err != nil {
			return err
		}
		records[index].Record.Value = row
	}
	return nil
}

func evolveArrowBatch(batch *ArrowLogBatch, source, target Schema) error {
	mapping, err := schemaColumnMapping(source, target)
	if err != nil {
		return err
	}
	targetSchema, err := target.ArrowSchema()
	if err != nil {
		return err
	}
	columns := make([]arrow.Array, len(mapping))
	var temporary []arrow.Array
	for index, sourceIndex := range mapping {
		if sourceIndex < 0 {
			column := array.MakeArrayOfNull(
				memory.DefaultAllocator,
				targetSchema.Field(index).Type,
				int(batch.Record.NumRows()),
			)
			columns[index] = column
			temporary = append(temporary, column)
			continue
		}
		columns[index] = batch.Record.Column(sourceIndex)
	}
	record := array.NewRecordBatch(targetSchema, columns, batch.Record.NumRows())
	for _, column := range temporary {
		column.Release()
	}
	batch.Record.Release()
	batch.Record = record
	return nil
}

func (s *LogScanner) projectScanRows(rows []ScanRecord) []ScanRecord {
	if len(s.projection) == 0 {
		return rows
	}
	for index := range rows {
		projected := make(Row, len(s.projection))
		for columnIndex, position := range s.projection {
			projected[columnIndex] = rows[index].Record.Value[position]
		}
		rows[index].Record.Value = projected
	}
	return rows
}

func (s *LogScanner) projectScanArrows(batches []ScanArrowBatch) error {
	if len(s.projection) == 0 {
		return nil
	}
	schema, err := s.schema.ArrowSchema()
	if err != nil {
		return err
	}
	for index := range batches {
		source := batches[index].Batch.Record
		columns := make([]arrow.Array, len(s.projection))
		for columnIndex, position := range s.projection {
			if position < 0 || int(position) >= int(source.NumCols()) {
				return fmt.Errorf("%w: projected Arrow column %d is unavailable", ErrInvalidSchema, position)
			}
			columns[columnIndex] = source.Column(int(position))
		}
		record := array.NewRecordBatch(schema, columns, source.NumRows())
		source.Release()
		batches[index].Batch.Record = record
	}
	return nil
}

func fetchedBatchSize(encoded []byte) (int, error) {
	size, complete, err := completeFetchedBatchSize(encoded)
	if err != nil {
		return 0, err
	}
	if !complete {
		return 0, incompleteFetchedBatchError(encoded)
	}
	return size, nil
}

func completeFetchedBatchSize(encoded []byte) (int, bool, error) {
	if len(encoded) < 12 {
		return 0, false, nil
	}
	size := uint64(12) + uint64(binary.LittleEndian.Uint32(encoded[8:]))
	if size < uint64(logBatchV0HeaderSize) {
		return 0, false, fmt.Errorf("%w: invalid fetched batch size", ErrMalformedRecordBatch)
	}
	if size > uint64(len(encoded)) {
		return 0, false, nil
	}
	return int(size), true, nil
}

func incompleteFetchedBatchError(encoded []byte) error {
	if len(encoded) < 12 {
		return fmt.Errorf("%w: truncated fetched batch", ErrMalformedRecordBatch)
	}
	return fmt.Errorf("%w: invalid fetched batch size", ErrMalformedRecordBatch)
}

func decodeFetchedBatch(
	schema Schema,
	bucket int32,
	current int64,
	payload []byte,
	compacted bool,
) (int64, []ScanRecord, *ScanArrowBatch, error) {
	batch, rowErr := DecodeLogBatchRows(schema, payload, compacted)
	if rowErr == nil {
		start, next, err := fetchedBatchOffsets(batch.BaseOffset, int64(len(batch.Records)), current)
		if err != nil {
			return current, nil, nil, err
		}
		records := batch.Records[start:]
		rows := make([]ScanRecord, len(records))
		for index, record := range records {
			rows[index] = ScanRecord{Bucket: bucket, Record: record}
		}
		return next, rows, nil, nil
	}
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		return current, nil, nil, err
	}
	arrowBatch, err := DecodeArrowLogBatch(arrowSchema, payload, memory.DefaultAllocator)
	if err != nil {
		return current, nil, nil, errors.Join(rowErr, err)
	}
	count := arrowBatch.Record.NumRows()
	start, next, err := fetchedBatchOffsets(arrowBatch.BaseOffset, count, current)
	if err != nil {
		arrowBatch.Release()
		return current, nil, nil, err
	}
	if start == count {
		arrowBatch.Release()
		return next, nil, nil, nil
	}
	if start > 0 {
		sliced := sliceArrowLogBatch(arrowBatch, start, count)
		arrowBatch.Release()
		arrowBatch = &sliced
	}
	result := &ScanArrowBatch{Bucket: bucket, Batch: *arrowBatch}
	return next, nil, result, nil
}

func fetchedBatchOffsets(base, count, current int64) (int64, int64, error) {
	if count < 0 {
		return 0, current, fmt.Errorf("%w: negative log batch record count", ErrMalformedRecordBatch)
	}
	if count == 0 {
		return 0, current, nil
	}
	if base > math.MaxInt64-count {
		return 0, current, fmt.Errorf("%w: log batch offset overflow", ErrMalformedRecordBatch)
	}
	end := base + count
	start := int64(0)
	if current >= end {
		start = count
	} else if current > base {
		start = current - base
	}
	if end > current {
		current = end
	}
	return start, current, nil
}

func releaseScanArrows(batches []ScanArrowBatch) {
	for index := range batches {
		batches[index].Batch.Release()
	}
}
