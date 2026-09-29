package fgo

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func (s *BatchScanner) decodeCurrent(ctx context.Context, isLog bool, encoded []byte) (BatchResult, error) {
	if isLog != (s.table.Kind == LogTable) {
		return BatchResult{}, fmt.Errorf("%w: LIMIT_SCAN table kind mismatch", ErrValidation)
	}
	if len(encoded) == 0 {
		return BatchResult{}, nil
	}
	if !isLog {
		rows, err := decodeValueRecordBatchWithResolver(ctx, s.resolver, s.table, encoded)
		if err != nil {
			return BatchResult{}, err
		}
		if len(rows) > s.config.Limit {
			rows = rows[len(rows)-s.config.Limit:]
		}
		return BatchResult{Rows: s.projectRows(rows)}, nil
	}
	compacted := !strings.EqualFold(
		strings.TrimSpace(s.table.Properties["table.log.format"]),
		string(LogFormatIndexed),
	)
	_, records, arrows, err := decodeFetchedLogWithResolver(
		ctx, s.resolver, s.table, s.bucket.BucketID, 0, encoded, compacted,
	)
	if err != nil {
		return BatchResult{}, err
	}
	records, arrows = limitLatestLogRecords(records, arrows, int64(s.config.Limit))
	if err := s.projectArrowBatches(arrows); err != nil {
		releaseScanArrows(arrows)
		return BatchResult{}, err
	}
	rows := make([]Row, len(records))
	for index := range records {
		rows[index] = records[index].Record.Value
	}
	result := BatchResult{Rows: s.projectRows(rows), ArrowBatches: make([]ArrowLogBatch, len(arrows))}
	for index := range arrows {
		result.ArrowBatches[index] = arrows[index].Batch
	}
	return result, nil
}

func limitLatestLogRecords(
	rows []ScanRecord,
	arrows []ScanArrowBatch,
	limit int64,
) ([]ScanRecord, []ScanArrowBatch) {
	skip := scanResultRows(rows, arrows) - limit
	if skip <= 0 {
		return rows, arrows
	}
	limitedRows := make([]ScanRecord, 0, len(rows))
	limitedArrows := make([]ScanArrowBatch, 0, len(arrows))
	for _, segment := range orderedScanSegments(rows, arrows) {
		if segment.row >= 0 {
			if skip > 0 {
				skip--
				continue
			}
			limitedRows = append(limitedRows, rows[segment.row])
			continue
		}
		item := &arrows[segment.arrow]
		count := item.Batch.Record.NumRows()
		if skip >= count {
			skip -= count
			item.Batch.Release()
			continue
		}
		if skip > 0 {
			sliced := sliceArrowLogBatch(&item.Batch, skip, count)
			item.Batch.Release()
			item.Batch = sliced
			skip = 0
		}
		limitedArrows = append(limitedArrows, *item)
	}
	return limitedRows, limitedArrows
}

func (s *BatchScanner) projectArrowBatches(batches []ScanArrowBatch) error {
	if len(s.projection) == 0 {
		return nil
	}
	projected, err := projectSchema(s.table.Schema, s.config.Projection)
	if err != nil {
		return err
	}
	schema, err := projected.ArrowSchema()
	if err != nil {
		return err
	}
	for index := range batches {
		source := batches[index].Batch.Record
		columns := make([]arrow.Array, len(s.projection))
		for columnIndex, position := range s.projection {
			if position < 0 || position >= int(source.NumCols()) {
				return fmt.Errorf("%w: projected Arrow column %d is unavailable", ErrInvalidSchema, position)
			}
			columns[columnIndex] = source.Column(position)
		}
		record := array.NewRecordBatch(schema, columns, source.NumRows())
		source.Release()
		batches[index].Batch.Record = record
	}
	return nil
}

func decodeValueRecordBatch(table Table, encoded []byte) ([]Row, error) {
	return decodeValueRecordBatchWithResolver(
		context.Background(),
		fixedSchemaResolver{path: table.Path, schemaID: table.SchemaID, schema: table.Schema},
		table,
		encoded,
	)
}

func decodeValueRecordBatchWithResolver(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	encoded []byte,
) ([]Row, error) {
	const headerSize = 9
	if len(encoded) < headerSize ||
		int(binary.LittleEndian.Uint32(encoded)) != len(encoded)-4 ||
		encoded[4] != 0 {
		return nil, fmt.Errorf("%w: invalid value batch header", ErrMalformedRecordBatch)
	}
	count := int(binary.LittleEndian.Uint32(encoded[5:]))
	if count < 0 || count > (len(encoded)-headerSize)/6 {
		return nil, fmt.Errorf("%w: invalid value record count", ErrMalformedRecordBatch)
	}
	rows := make([]Row, 0, count)
	position := headerSize
	for range count {
		row, next, err := decodeValueRecord(ctx, resolver, table, encoded, position)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		position = next
	}
	if position != len(encoded) {
		return nil, fmt.Errorf("%w: trailing value batch bytes", ErrMalformedRecordBatch)
	}
	return rows, nil
}

func decodeValueRecord(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	encoded []byte,
	position int,
) (Row, int, error) {
	if len(encoded)-position < 6 {
		return nil, position, fmt.Errorf("%w: truncated value record", ErrMalformedRecordBatch)
	}
	size := int(binary.LittleEndian.Uint32(encoded[position:]))
	position += 4
	if size < 2 || size > len(encoded)-position {
		return nil, position, fmt.Errorf("%w: invalid value record length", ErrMalformedRecordBatch)
	}
	schemaID := int32(int16(binary.LittleEndian.Uint16(encoded[position:])))
	schema, err := resolver.resolveSchema(ctx, table.Path, schemaID)
	if err != nil {
		return nil, position, fmt.Errorf("fgo: resolve value schema %d: %w", schemaID, err)
	}
	row, err := DecodeCompactedRow(schema, encoded[position+2:position+size])
	if err != nil {
		return nil, position, err
	}
	row, err = evolveRow(schema, table.Schema, row)
	return row, position + size, err
}

func (s *BatchScanner) projectRows(rows []Row) []Row {
	if len(s.projection) == 0 {
		return rows
	}
	projected := make([]Row, len(rows))
	for rowIndex, row := range rows {
		projected[rowIndex] = make(Row, len(s.projection))
		for columnIndex, position := range s.projection {
			projected[rowIndex][columnIndex] = row[position]
		}
	}
	return projected
}
