package fgo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const maxRowBytes = 16 << 20

var ErrMalformedRow = errors.New("fgo: malformed row encoding")

// EncodeCompactedRow encodes Fluss's compacted binary row format. The returned buffer is owned by
// the caller and remains valid after the input row is reused.
func EncodeCompactedRow(schema Schema, row Row) ([]byte, error) {
	if err := schema.ValidateRow(row, nil); err != nil {
		return nil, err
	}
	encoded := make([]byte, (len(schema.Columns)+7)/8)
	for index, column := range schema.Columns {
		if row[index] == nil {
			encoded[index/8] |= 1 << (index % 8)
			continue
		}
		var err error
		encoded, err = appendCompactedValue(encoded, column.Type, row[index])
		if err != nil {
			return nil, err
		}
		if len(encoded) > maxRowBytes {
			return nil, fmt.Errorf("%w: row exceeds %d bytes", ErrMalformedRow, maxRowBytes)
		}
	}
	return encoded, nil
}

// DecodeCompactedRow decodes a compacted Fluss row. It rejects truncated, overlong, and trailing
// data instead of accepting an ambiguous prefix.
func DecodeCompactedRow(schema Schema, encoded []byte) (Row, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	if len(encoded) > maxRowBytes || len(encoded) < (len(schema.Columns)+7)/8 {
		return nil, fmt.Errorf("%w: invalid compacted row length", ErrMalformedRow)
	}
	position := (len(schema.Columns) + 7) / 8
	row := make(Row, len(schema.Columns))
	for index, column := range schema.Columns {
		if encoded[index/8]&(1<<(index%8)) != 0 {
			if !column.Nullable {
				return nil, fmt.Errorf("%w: non-nullable column %q is null", ErrMalformedRow, column.Name)
			}
			continue
		}
		value, next, err := readCompactedValue(encoded, position, column.Type)
		if err != nil {
			return nil, fmt.Errorf("%w: column %q: %v", ErrMalformedRow, column.Name, err)
		}
		row[index], position = value, next
	}
	if position != len(encoded) {
		return nil, fmt.Errorf("%w: trailing bytes", ErrMalformedRow)
	}
	return row, nil
}

// EncodePrimaryKey writes the compacted key representation used by Fluss Lookup and PrefixLookup.
// Primary-key columns have no null bitmap and must all be non-null.
func EncodePrimaryKey(schema Schema, key PrimaryKey) ([]byte, error) {
	if err := schema.ValidatePrimaryKey(key); err != nil {
		return nil, err
	}
	byName := schema.columnsByName()
	encoded := make([]byte, 0, 32)
	for index, name := range schema.PrimaryKey {
		var err error
		encoded, err = appendCompactedValue(encoded, byName[name].Type, key[index])
		if err != nil {
			return nil, err
		}
	}
	return encoded, nil
}

// EncodeIndexedRow encodes Fluss's indexed row format. Its header stores a null bitmap followed
// by little-endian lengths for non-null variable-width fields.
func EncodeIndexedRow(schema Schema, row Row) ([]byte, error) {
	if err := schema.ValidateRow(row, nil); err != nil {
		return nil, err
	}
	nullBytes := (len(schema.Columns) + 7) / 8
	variableBytes := 0
	for _, column := range schema.Columns {
		if isVariableWidth(column.Type) {
			variableBytes += 4
		}
	}
	encoded := make([]byte, nullBytes+variableBytes)
	lengthPosition := nullBytes
	for index, column := range schema.Columns {
		if row[index] == nil {
			encoded[index/8] |= 1 << (index % 8)
			continue
		}
		if isVariableWidth(column.Type) {
			length, err := compactedValueLength(column.Type, row[index])
			if err != nil {
				return nil, err
			}
			binary.LittleEndian.PutUint32(encoded[lengthPosition:], uint32(length))
			lengthPosition += 4
		}
		var err error
		encoded, err = appendIndexedValue(encoded, column.Type, row[index])
		if err != nil {
			return nil, err
		}
		if len(encoded) > maxRowBytes {
			return nil, fmt.Errorf("%w: row exceeds %d bytes", ErrMalformedRow, maxRowBytes)
		}
	}
	return encoded, nil
}

// DecodeIndexedRow validates its variable-width offsets while decoding the sequential payload.
func DecodeIndexedRow(schema Schema, encoded []byte) (Row, error) {
	if err := schema.Validate(); err != nil {
		return nil, err
	}
	nullBytes := (len(schema.Columns) + 7) / 8
	variableBytes := 0
	for _, column := range schema.Columns {
		if isVariableWidth(column.Type) {
			variableBytes += 4
		}
	}
	if len(encoded) > maxRowBytes || len(encoded) < nullBytes+variableBytes {
		return nil, fmt.Errorf("%w: invalid indexed row length", ErrMalformedRow)
	}
	row := make(Row, len(schema.Columns))
	position, lengthPosition := nullBytes+variableBytes, nullBytes
	for index, column := range schema.Columns {
		isNull := encoded[index/8]&(1<<(index%8)) != 0
		var length int
		if isVariableWidth(column.Type) && !isNull {
			length = int(binary.LittleEndian.Uint32(encoded[lengthPosition:]))
			lengthPosition += 4
		}
		if isNull {
			if !column.Nullable {
				return nil, fmt.Errorf("%w: non-nullable column %q is null", ErrMalformedRow, column.Name)
			}
			continue
		}
		value, next, err := readIndexedValue(encoded, position, column.Type, length)
		if err != nil {
			return nil, fmt.Errorf("%w: column %q: %v", ErrMalformedRow, column.Name, err)
		}
		row[index], position = value, next
	}
	if position != len(encoded) {
		return nil, fmt.Errorf("%w: trailing bytes", ErrMalformedRow)
	}
	return row, nil
}

func appendCompactedValue(dst []byte, kind DataType, value any) ([]byte, error) {
	switch kind {
	case BoolType:
		if value.(bool) {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	case TinyIntType:
		return append(dst, byte(value.(int8))), nil
	case SmallIntType:
		return appendLittle(dst, uint64(uint16(value.(int16))), 2), nil
	case IntType:
		return appendVar32(dst, uint32(value.(int32))), nil
	case BigIntType:
		return appendVar64(dst, uint64(value.(int64))), nil
	case FloatType:
		return appendLittle(dst, uint64(math.Float32bits(value.(float32))), 4), nil
	case DoubleType:
		return appendLittle(dst, math.Float64bits(value.(float64)), 8), nil
	case StringType, CharType:
		return appendLengthBytes(dst, []byte(value.(string)))
	case BytesType, BinaryType:
		return appendLengthBytes(dst, value.([]byte))
	default:
		return nil, fmt.Errorf("%w: compacted codec does not support %s", ErrInvalidSchema, kind)
	}
}

func readCompactedValue(encoded []byte, position int, kind DataType) (any, int, error) {
	switch kind {
	case BoolType:
		value, next, err := readFixed(encoded, position, 1)
		if err != nil || value[0] > 1 {
			return nil, 0, errors.New("invalid boolean")
		}
		return value[0] == 1, next, nil
	case TinyIntType:
		value, next, err := readFixed(encoded, position, 1)
		if err != nil {
			return nil, 0, err
		}
		return int8(value[0]), next, err
	case SmallIntType:
		value, next, err := readFixed(encoded, position, 2)
		if err != nil {
			return nil, 0, err
		}
		return int16(binary.LittleEndian.Uint16(value)), next, err
	case IntType:
		value, next, err := readVar(encoded, position, 5)
		return int32(value), next, err
	case BigIntType:
		value, next, err := readVar(encoded, position, 10)
		return int64(value), next, err
	case FloatType:
		value, next, err := readFixed(encoded, position, 4)
		if err != nil {
			return nil, 0, err
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(value)), next, err
	case DoubleType:
		value, next, err := readFixed(encoded, position, 8)
		if err != nil {
			return nil, 0, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(value)), next, err
	case StringType, CharType:
		value, next, err := readLengthBytes(encoded, position)
		return string(value), next, err
	case BytesType, BinaryType:
		return readLengthBytes(encoded, position)
	default:
		return nil, 0, fmt.Errorf("unsupported type %s", kind)
	}
}

func appendLengthBytes(dst, value []byte) ([]byte, error) {
	if len(value) > maxRowBytes {
		return nil, fmt.Errorf("%w: value exceeds %d bytes", ErrMalformedRow, maxRowBytes)
	}
	dst = appendVar32(dst, uint32(len(value)))
	return append(dst, value...), nil
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

func readFixed(encoded []byte, position, length int) ([]byte, int, error) {
	if position < 0 || length < 0 || len(encoded)-position < length {
		return nil, 0, errors.New("truncated value")
	}
	return encoded[position : position+length], position + length, nil
}

func readVar(encoded []byte, position, maximum int) (uint64, int, error) {
	var value uint64
	for index := 0; index < maximum; index++ {
		if position >= len(encoded) {
			return 0, 0, errors.New("truncated varint")
		}
		part := encoded[position]
		position++
		value |= uint64(part&0x7f) << (7 * index)
		if part&0x80 == 0 {
			return value, position, nil
		}
	}
	return 0, 0, errors.New("overlong varint")
}

func readLengthBytes(encoded []byte, position int) ([]byte, int, error) {
	length, next, err := readVar(encoded, position, 5)
	if err != nil || length > maxRowBytes || length > uint64(len(encoded)-next) {
		return nil, 0, errors.New("invalid byte length")
	}
	value := append([]byte(nil), encoded[next:next+int(length)]...)
	return value, next + int(length), nil
}

func appendIndexedValue(dst []byte, kind DataType, value any) ([]byte, error) {
	switch kind {
	case IntType:
		return appendLittle(dst, uint64(uint32(value.(int32))), 4), nil
	case BigIntType:
		return appendLittle(dst, uint64(value.(int64)), 8), nil
	case StringType, CharType:
		return append(dst, value.(string)...), nil
	case BytesType:
		return append(dst, value.([]byte)...), nil
	default:
		return appendCompactedValue(dst, kind, value)
	}
}

func readIndexedValue(encoded []byte, position int, kind DataType, length int) (any, int, error) {
	if isVariableWidth(kind) {
		value, next, err := readFixed(encoded, position, length)
		if err != nil {
			return nil, 0, err
		}
		if kind == StringType || kind == CharType {
			return string(value), next, nil
		}
		return append([]byte(nil), value...), next, nil
	}
	switch kind {
	case IntType:
		value, next, err := readFixed(encoded, position, 4)
		if err != nil {
			return nil, 0, err
		}
		return int32(binary.LittleEndian.Uint32(value)), next, err
	case BigIntType:
		value, next, err := readFixed(encoded, position, 8)
		if err != nil {
			return nil, 0, err
		}
		return int64(binary.LittleEndian.Uint64(value)), next, err
	default:
		return readCompactedValue(encoded, position, kind)
	}
}

func compactedValueLength(kind DataType, value any) (int, error) {
	switch kind {
	case StringType, CharType:
		return len(value.(string)), nil
	case BytesType:
		return len(value.([]byte)), nil
	default:
		return 0, fmt.Errorf("%w: indexed codec does not support %s", ErrInvalidSchema, kind)
	}
}

func isVariableWidth(kind DataType) bool {
	return kind == StringType || kind == CharType || kind == BytesType
}
