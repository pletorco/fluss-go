package fgo

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const maxRowBytes = 16 << 20

// ErrMalformedRow reports an invalid compacted or indexed row encoding.
var ErrMalformedRow = errors.New("fgo: malformed row encoding")

type rowEncoding uint8

const (
	compactedEncoding rowEncoding = iota
	indexedEncoding
)

// EncodeCompactedRow encodes Fluss's compacted binary row format. The returned buffer is owned by
// the caller and remains valid after the input row is reused.
func EncodeCompactedRow(schema Schema, row Row) ([]byte, error) {
	return encodeRow(schema, row, compactedEncoding)
}

// DecodeCompactedRow decodes a complete compacted Fluss row.
func DecodeCompactedRow(schema Schema, encoded []byte) (Row, error) {
	return decodeRow(schema, encoded, compactedEncoding)
}

// EncodeIndexedRow encodes Fluss's indexed binary row format.
func EncodeIndexedRow(schema Schema, row Row) ([]byte, error) {
	return encodeRow(schema, row, indexedEncoding)
}

// DecodeIndexedRow decodes a complete indexed Fluss row.
func DecodeIndexedRow(schema Schema, encoded []byte) (Row, error) {
	return decodeRow(schema, encoded, indexedEncoding)
}

// EncodeCompactedProjectedRow encodes values for the named columns in the given order. It is used
// by Fluss partial updates, where omitted columns must not be serialized as nulls.
func EncodeCompactedProjectedRow(schema Schema, columns []string, row Row) ([]byte, error) {
	projected, err := projectSchema(schema, columns)
	if err != nil {
		return nil, err
	}
	return encodeRow(projected, row, compactedEncoding)
}

// DecodeCompactedProjectedRow decodes compacted values for columns.
func DecodeCompactedProjectedRow(schema Schema, columns []string, encoded []byte) (Row, error) {
	projected, err := projectSchema(schema, columns)
	if err != nil {
		return nil, err
	}
	return decodeRow(projected, encoded, compactedEncoding)
}

// EncodeIndexedProjectedRow encodes indexed values for columns.
func EncodeIndexedProjectedRow(schema Schema, columns []string, row Row) ([]byte, error) {
	projected, err := projectSchema(schema, columns)
	if err != nil {
		return nil, err
	}
	return encodeRow(projected, row, indexedEncoding)
}

// DecodeIndexedProjectedRow decodes indexed values for columns.
func DecodeIndexedProjectedRow(schema Schema, columns []string, encoded []byte) (Row, error) {
	projected, err := projectSchema(schema, columns)
	if err != nil {
		return nil, err
	}
	return decodeRow(projected, encoded, indexedEncoding)
}

// EncodePrimaryKey writes the compacted key representation used by Lookup v0 and v1. Primary-key
// columns have no null bitmap and must all be non-null.
func EncodePrimaryKey(schema Schema, key PrimaryKey) ([]byte, error) {
	if err := schema.ValidatePrimaryKey(key); err != nil {
		return nil, err
	}
	return encodeKeyColumns(schema, schema.PrimaryKey, key)
}

// EncodeLookupKey selects the key contract negotiated for Lookup v0 or v1. Fluss 0.9.1 uses the
// same compacted key bytes in both versions; keeping the version explicit prevents silent use of a
// future incompatible layout.
func EncodeLookupKey(schema Schema, key PrimaryKey, version int16) ([]byte, error) {
	if version != 0 && version != 1 {
		return nil, fmt.Errorf("%w: unsupported Lookup version %d", ErrInvalidConfig, version)
	}
	return EncodePrimaryKey(schema, key)
}

// EncodePrefixKey writes a non-empty leading subset of the primary key for PrefixLookup v0 and v1.
func EncodePrefixKey(schema Schema, prefix PrimaryKey) ([]byte, error) {
	if len(prefix) == 0 || len(prefix) > len(schema.PrimaryKey) {
		return nil, fmt.Errorf("%w: prefix has %d values for %d primary-key columns", ErrInvalidRow, len(prefix), len(schema.PrimaryKey))
	}
	return encodeKeyColumns(schema, schema.PrimaryKey[:len(prefix)], prefix)
}

// EncodePrefixLookupKey selects the negotiated PrefixLookup key contract.
func EncodePrefixLookupKey(schema Schema, prefix PrimaryKey, version int16) ([]byte, error) {
	if version != 0 && version != 1 {
		return nil, fmt.Errorf("%w: unsupported PrefixLookup version %d", ErrInvalidConfig, version)
	}
	return EncodePrefixKey(schema, prefix)
}

func encodeKeyColumns(schema Schema, names []string, values PrimaryKey) ([]byte, error) {
	if err := schema.ValidateRow(Row(values), names); err != nil {
		return nil, err
	}
	byName := schema.columnsByName()
	encoded := make([]byte, 0, 32)
	for i, name := range names {
		column := byName[name]
		if values[i] == nil {
			return nil, fmt.Errorf("%w: key column %q is null", ErrInvalidRow, name)
		}
		var err error
		encoded, err = appendCompactedValue(encoded, logicalTypeForColumn(column), values[i])
		if err != nil {
			return nil, fmt.Errorf("fgo: key column %q: %w", name, err)
		}
	}
	if len(encoded) > maxRowBytes {
		return nil, fmt.Errorf("%w: key exceeds %d bytes", ErrMalformedRow, maxRowBytes)
	}
	return encoded, nil
}

func encodeRow(schema Schema, row Row, encoding rowEncoding) ([]byte, error) {
	if err := schema.ValidateRow(row, nil); err != nil {
		return nil, err
	}
	nullBytes := (len(schema.Columns) + 7) / 8
	headerBytes := rowHeaderBytes(schema, encoding)
	encoded := make([]byte, headerBytes)
	lengthPosition := nullBytes
	for i, column := range schema.Columns {
		if row[i] == nil {
			encoded[i/8] |= 1 << (i % 8)
			continue
		}
		value, err := encodeRowValue(logicalTypeForColumn(column), row[i], encoding)
		if err != nil {
			return nil, fmt.Errorf("fgo: column %q: %w", column.Name, err)
		}
		if encoding == indexedEncoding && !indexedFixed(logicalTypeForColumn(column)) {
			binary.LittleEndian.PutUint32(encoded[lengthPosition:], uint32(len(value)))
			lengthPosition += 4
		}
		if len(value) > maxRowBytes-len(encoded) {
			return nil, fmt.Errorf("%w: row exceeds %d bytes", ErrMalformedRow, maxRowBytes)
		}
		encoded = append(encoded, value...)
	}
	return encoded, nil
}

func rowHeaderBytes(schema Schema, encoding rowEncoding) int {
	headerBytes := (len(schema.Columns) + 7) / 8
	if encoding != indexedEncoding {
		return headerBytes
	}
	for _, column := range schema.Columns {
		if !indexedFixed(logicalTypeForColumn(column)) {
			headerBytes += 4
		}
	}
	return headerBytes
}

func encodeRowValue(logicalType LogicalType, value any, encoding rowEncoding) ([]byte, error) {
	if encoding == compactedEncoding {
		return appendCompactedValue(nil, logicalType, value)
	}
	return appendIndexedValue(nil, logicalType, value)
}

func decodeRow(schema Schema, encoded []byte, encoding rowEncoding) (Row, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	nullBytes := (len(schema.Columns) + 7) / 8
	headerBytes := rowHeaderBytes(schema, encoding)
	if len(encoded) > maxRowBytes || len(encoded) < headerBytes {
		return nil, fmt.Errorf("%w: invalid row length", ErrMalformedRow)
	}
	row := make(Row, len(schema.Columns))
	position, lengthPosition := headerBytes, nullBytes
	for i, column := range schema.Columns {
		logicalType := logicalTypeForColumn(column)
		isNull := encoded[i/8]&(1<<(i%8)) != 0
		length, nextLengthPosition := indexedValueLength(encoded, lengthPosition, logicalType, encoding, isNull)
		lengthPosition = nextLengthPosition
		if isNull {
			if !column.Nullable {
				return nil, fmt.Errorf("%w: non-nullable column %q is null", ErrMalformedRow, column.Name)
			}
			continue
		}
		value, next, err := decodeRowValue(encoded, position, logicalType, encoding, length)
		if err != nil {
			return nil, fmt.Errorf("%w: column %q: %v", ErrMalformedRow, column.Name, err)
		}
		row[i], position = value, next
	}
	if position != len(encoded) {
		return nil, fmt.Errorf("%w: trailing bytes", ErrMalformedRow)
	}
	return row, nil
}

func indexedValueLength(
	encoded []byte,
	position int,
	logicalType LogicalType,
	encoding rowEncoding,
	isNull bool,
) (int, int) {
	if encoding != indexedEncoding || indexedFixed(logicalType) || isNull {
		return -1, position
	}
	return int(binary.LittleEndian.Uint32(encoded[position:])), position + 4
}

func decodeRowValue(
	encoded []byte,
	position int,
	logicalType LogicalType,
	encoding rowEncoding,
	length int,
) (any, int, error) {
	if encoding == compactedEncoding {
		return readCompactedValue(encoded, position, logicalType)
	}
	return readIndexedValue(encoded, position, logicalType, length)
}

func nestedSchema(logicalType LogicalType) Schema {
	columns := make([]Column, len(logicalType.Fields))
	for i, field := range logicalType.Fields {
		fieldType := field.Type
		columns[i] = Column{
			Name:        field.Name,
			Type:        dataTypeForLogicalType(fieldType),
			Nullable:    fieldType.Nullable,
			LogicalType: &fieldType,
			Description: field.Description,
			ID:          field.ID,
		}
	}
	return Schema{Columns: columns}
}

func projectSchema(schema Schema, names []string) (Schema, error) {
	if err := schema.Validate(); err != nil {
		return Schema{}, err
	}
	if len(names) == 0 {
		return Schema{}, fmt.Errorf("%w: projection has no columns", ErrInvalidSchema)
	}
	byName := schema.columnsByName()
	seen := make(map[string]struct{}, len(names))
	columns := make([]Column, len(names))
	for i, name := range names {
		column, ok := byName[name]
		if !ok {
			return Schema{}, fmt.Errorf("%w: projected column %q does not exist", ErrInvalidSchema, name)
		}
		if _, exists := seen[name]; exists {
			return Schema{}, fmt.Errorf("%w: duplicate projected column %q", ErrInvalidSchema, name)
		}
		seen[name] = struct{}{}
		columns[i] = column
	}
	return Schema{Columns: columns}, nil
}

func readFixed(encoded []byte, position, length int) ([]byte, int, error) {
	if position < 0 || length < 0 || position > len(encoded) || len(encoded)-position < length {
		return nil, 0, errors.New("truncated value")
	}
	return encoded[position : position+length], position + length, nil
}

func appendLittle(dst []byte, value uint64, width int) []byte {
	var bytes [8]byte
	binary.LittleEndian.PutUint64(bytes[:], value)
	return append(dst, bytes[:width]...)
}

func appendVar32(dst []byte, value uint32) []byte { return appendVar64(dst, uint64(value)) }

func appendVar64(dst []byte, value uint64) []byte {
	for value > 0x7f {
		dst = append(dst, byte(value)|0x80)
		value >>= 7
	}
	return append(dst, byte(value))
}

func readVar(encoded []byte, position, maximum int) (uint64, int, error) {
	var value uint64
	for i := 0; i < maximum; i++ {
		if position >= len(encoded) {
			return 0, 0, errors.New("truncated varint")
		}
		part := encoded[position]
		position++
		value |= uint64(part&0x7f) << (7 * i)
		if part&0x80 == 0 {
			return value, position, nil
		}
	}
	return 0, 0, errors.New("overlong varint")
}

func appendLengthBytes(dst, value []byte) ([]byte, error) {
	if len(value) > maxRowBytes {
		return nil, fmt.Errorf("value exceeds %d bytes", maxRowBytes)
	}
	dst = appendVar32(dst, uint32(len(value)))
	return append(dst, value...), nil
}

func readLengthBytes(encoded []byte, position int) ([]byte, int, error) {
	length, next, err := readVar(encoded, position, 5)
	if err != nil || length > maxRowBytes || next > len(encoded) || length > uint64(len(encoded)-next) {
		return nil, 0, errors.New("invalid byte length")
	}
	value := append([]byte(nil), encoded[next:next+int(length)]...)
	return value, next + int(length), nil
}
