package fgo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"
)

func encodeBinaryArray(elementType LogicalType, values []any, encoding rowEncoding) ([]byte, error) {
	if len(values) > maxRowBytes/8 {
		return nil, errors.New("array element count exceeds allocation limit")
	}
	// A binary array is [uint32 count][null bitmap][fixed slots][padding]
	// followed by variable-width values. The bitmap is rounded to 32-bit words,
	// while the complete fixed region is rounded to an 8-byte boundary. Slot
	// widths are determined solely by the element type, so nulls still consume
	// their slot. maxRowBytes bounds every allocation and offset.
	//
	// This layout is pinned to Apache Fluss 0.9.1 commit
	// 6bf969f71af8d6f9cc37383ab89ae46a58b0e227 and covered by
	// TestRowsMatchJava091Fixtures and the binary-array malformed-input tests.
	slotSize := arraySlotSize(elementType)
	nullBytes := ((len(values) + 31) / 32) * 4
	fixedBytes := align8(4 + nullBytes + slotSize*len(values))
	if fixedBytes > maxRowBytes {
		return nil, errors.New("array fixed region exceeds allocation limit")
	}
	encoded := make([]byte, fixedBytes)
	binary.LittleEndian.PutUint32(encoded, uint32(len(values)))
	for i, value := range values {
		slot := 4 + nullBytes + slotSize*i
		if value == nil {
			if !elementType.Nullable {
				return nil, fmt.Errorf("array element %d is not nullable", i)
			}
			encoded[4+i/8] |= 1 << (i % 8)
			continue
		}
		if !validLogicalValue(elementType, value) {
			return nil, fmt.Errorf("array element %d has invalid %s value", i, elementType.Root)
		}
		var err error
		encoded, err = writeArrayElement(encoded, slot, elementType, value, encoding)
		if err != nil {
			return nil, fmt.Errorf("array element %d: %w", i, err)
		}
		if len(encoded) > maxRowBytes {
			return nil, errors.New("array exceeds allocation limit")
		}
	}
	return encoded, nil
}

func decodeBinaryArray(elementType LogicalType, encoded []byte, encoding rowEncoding) ([]any, error) {
	if len(encoded) < 4 || len(encoded) > maxRowBytes {
		return nil, errors.New("invalid binary array length")
	}
	count := int(binary.LittleEndian.Uint32(encoded))
	if count > maxRowBytes/8 {
		return nil, errors.New("array element count exceeds allocation limit")
	}
	nullBytes := ((count + 31) / 32) * 4
	slotSize := arraySlotSize(elementType)
	fixedBytes := align8(4 + nullBytes + slotSize*count)
	if fixedBytes > len(encoded) {
		return nil, errors.New("truncated binary array")
	}
	values := make([]any, count)
	for i := range values {
		if encoded[4+i/8]&(1<<(i%8)) != 0 {
			if !elementType.Nullable {
				return nil, fmt.Errorf("non-nullable array element %d is null", i)
			}
			continue
		}
		value, err := readArrayElement(encoded, 4+nullBytes+slotSize*i, elementType, encoding)
		if err != nil {
			return nil, fmt.Errorf("array element %d: %w", i, err)
		}
		values[i] = value
	}
	return values, nil
}

func encodeBinaryMap(logicalType LogicalType, value any, encoding rowEncoding) ([]byte, error) {
	entries, ok := mapEntries(value)
	if !ok {
		return nil, errors.New("invalid map value")
	}
	// Maps are [uint32 key-array byte length][key array][value array]. Encoding
	// both sides as binary arrays preserves element counts and null semantics;
	// decoders additionally require equal key and value counts.
	keys, values := make([]any, len(entries)), make([]any, len(entries))
	for i, entry := range entries {
		keys[i], values[i] = entry.Key, entry.Value
	}
	keyBytes, err := encodeBinaryArray(*logicalType.Key, keys, encoding)
	if err != nil {
		return nil, err
	}
	valueBytes, err := encodeBinaryArray(*logicalType.Value, values, encoding)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 4, 4+len(keyBytes)+len(valueBytes))
	binary.LittleEndian.PutUint32(encoded, uint32(len(keyBytes)))
	encoded = append(encoded, keyBytes...)
	return append(encoded, valueBytes...), nil
}

func decodeBinaryMap(logicalType LogicalType, encoded []byte, encoding rowEncoding) (Map, error) {
	if len(encoded) < 4 {
		return nil, errors.New("truncated binary map")
	}
	keyLength := int(binary.LittleEndian.Uint32(encoded))
	if keyLength > len(encoded)-4 {
		return nil, errors.New("invalid binary map key length")
	}
	keys, err := decodeBinaryArray(*logicalType.Key, encoded[4:4+keyLength], encoding)
	if err != nil {
		return nil, err
	}
	values, err := decodeBinaryArray(*logicalType.Value, encoded[4+keyLength:], encoding)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, errors.New("binary map key/value count mismatch")
	}
	entries := make(Map, len(keys))
	for i := range keys {
		entries[i] = MapEntry{Key: keys[i], Value: values[i]}
	}
	return entries, nil
}

func writeArrayElement(encoded []byte, slot int, logicalType LogicalType, value any, encoding rowEncoding) ([]byte, error) {
	kind := dataTypeForLogicalType(logicalType)
	switch kind {
	case BoolType:
		if value.(bool) {
			encoded[slot] = 1
		}
	case TinyIntType:
		encoded[slot] = byte(value.(int8))
	case SmallIntType:
		binary.LittleEndian.PutUint16(encoded[slot:], uint16(value.(int16)))
	case IntType:
		binary.LittleEndian.PutUint32(encoded[slot:], uint32(value.(int32)))
	case DateType, TimeType:
		binary.LittleEndian.PutUint32(encoded[slot:], uint32(temporalInt(kind, value.(time.Time))))
	case FloatType:
		binary.LittleEndian.PutUint32(encoded[slot:], math.Float32bits(value.(float32)))
	case BigIntType:
		binary.LittleEndian.PutUint64(encoded[slot:], uint64(value.(int64)))
	case DoubleType:
		binary.LittleEndian.PutUint64(encoded[slot:], math.Float64bits(value.(float64)))
	case DecimalType:
		unscaled, err := decimalUnscaled(value.(*big.Rat), logicalType)
		if err != nil {
			return nil, err
		}
		if logicalType.Precision <= 18 {
			binary.LittleEndian.PutUint64(encoded[slot:], uint64(unscaled.Int64()))
			break
		}
		variable := signedBigEndian(unscaled)
		return appendArrayVariable(encoded, slot, variable, 16)
	case TimestampType, TimestampLTZType:
		millis, nanos := timestampParts(kind, value.(time.Time))
		if logicalType.Precision <= 3 {
			binary.LittleEndian.PutUint64(encoded[slot:], uint64(millis))
			break
		}
		// High-precision timestamps reuse the variable slot shape, but its low
		// 32 bits carry sub-millisecond nanos rather than a payload size. The
		// upper 32 bits point to the 8-byte millisecond value.
		offset := len(encoded)
		encoded = appendLittle(encoded, uint64(millis), 8)
		putOffsetSize(encoded, slot, offset, int(nanos))
	case StringType, CharType:
		return appendPackedArrayBytes(encoded, slot, []byte(value.(string)))
	case BytesType, BinaryType:
		return appendPackedArrayBytes(encoded, slot, value.([]byte))
	case ArrayType:
		variable, err := encodeBinaryArray(*logicalType.Element, value.([]any), encoding)
		if err != nil {
			return nil, err
		}
		return appendArrayVariable(encoded, slot, variable, align8(len(variable)))
	case MapType:
		variable, err := encodeBinaryMap(logicalType, value, encoding)
		if err != nil {
			return nil, err
		}
		return appendArrayVariable(encoded, slot, variable, align8(len(variable)))
	case RowType:
		variable, err := encodeRow(nestedSchema(logicalType), value.(Row), encoding)
		if err != nil {
			return nil, err
		}
		return appendArrayVariable(encoded, slot, variable, align8(len(variable)))
	default:
		return nil, fmt.Errorf("unsupported array element type %s", logicalType.Root)
	}
	return encoded, nil
}

func readArrayElement(encoded []byte, slot int, logicalType LogicalType, encoding rowEncoding) (any, error) {
	kind := dataTypeForLogicalType(logicalType)
	if slot < 0 || arraySlotSize(logicalType) > len(encoded)-slot {
		return nil, errors.New("truncated array slot")
	}
	if arrayPrimitive(kind) {
		return readArrayPrimitive(encoded, slot, kind)
	}
	switch kind {
	case DateType, TimeType:
		return temporalTime(kind, int32(binary.LittleEndian.Uint32(encoded[slot:]))), nil
	case DecimalType:
		return readArrayDecimal(encoded, slot, logicalType)
	case TimestampType, TimestampLTZType:
		return readArrayTimestamp(encoded, slot, logicalType)
	case StringType, CharType:
		value, err := unpackedArrayBytes(encoded, slot)
		return string(value), err
	case BytesType, BinaryType:
		return unpackedArrayBytes(encoded, slot)
	case ArrayType, MapType, RowType:
		return readArrayNested(encoded, slot, logicalType, encoding)
	default:
		return nil, fmt.Errorf("unsupported array element type %s", logicalType.Root)
	}
}

func arrayPrimitive(kind DataType) bool {
	switch kind {
	case BoolType, TinyIntType, SmallIntType, IntType, FloatType, BigIntType, DoubleType:
		return true
	default:
		return false
	}
}

func readArrayPrimitive(encoded []byte, slot int, kind DataType) (any, error) {
	switch kind {
	case BoolType:
		if encoded[slot] > 1 {
			return nil, errors.New("invalid boolean")
		}
		return encoded[slot] == 1, nil
	case TinyIntType:
		return int8(encoded[slot]), nil
	case SmallIntType:
		return int16(binary.LittleEndian.Uint16(encoded[slot:])), nil
	case IntType:
		return int32(binary.LittleEndian.Uint32(encoded[slot:])), nil
	case FloatType:
		return math.Float32frombits(binary.LittleEndian.Uint32(encoded[slot:])), nil
	case BigIntType:
		return int64(binary.LittleEndian.Uint64(encoded[slot:])), nil
	case DoubleType:
		return math.Float64frombits(binary.LittleEndian.Uint64(encoded[slot:])), nil
	default:
		return nil, fmt.Errorf("unsupported array primitive %s", kind)
	}
}

func readArrayDecimal(encoded []byte, slot int, logicalType LogicalType) (any, error) {
	if logicalType.Precision <= 18 {
		return scaledRat(big.NewInt(int64(binary.LittleEndian.Uint64(encoded[slot:]))), logicalType.Scale), nil
	}
	value, err := arrayVariable(encoded, slot)
	if err != nil {
		return nil, err
	}
	return scaledRat(signedBigInt(value), logicalType.Scale), nil
}

func readArrayTimestamp(encoded []byte, slot int, logicalType LogicalType) (any, error) {
	kind := dataTypeForLogicalType(logicalType)
	if logicalType.Precision <= 3 {
		return timestampTime(kind, int64(binary.LittleEndian.Uint64(encoded[slot:])), 0), nil
	}
	offset, nanos := offsetSize(encoded, slot)
	value, _, err := readFixed(encoded, offset, 8)
	if err != nil || nanos > 999999 {
		return nil, errors.New("invalid timestamp array value")
	}
	return timestampTime(kind, int64(binary.LittleEndian.Uint64(value)), int32(nanos)), nil
}

func readArrayNested(encoded []byte, slot int, logicalType LogicalType, encoding rowEncoding) (any, error) {
	value, err := arrayVariable(encoded, slot)
	if err != nil {
		return nil, err
	}
	switch dataTypeForLogicalType(logicalType) {
	case ArrayType:
		return decodeBinaryArray(*logicalType.Element, value, encoding)
	case MapType:
		return decodeBinaryMap(logicalType, value, encoding)
	case RowType:
		return decodeRow(nestedSchema(logicalType), value, encoding)
	default:
		return nil, fmt.Errorf("unsupported nested array element type %s", logicalType.Root)
	}
}

func arraySlotSize(logicalType LogicalType) int {
	switch dataTypeForLogicalType(logicalType) {
	case BoolType, TinyIntType:
		return 1
	case SmallIntType:
		return 2
	case IntType, FloatType, DateType, TimeType:
		return 4
	default:
		return 8
	}
}

func appendPackedArrayBytes(encoded []byte, slot int, value []byte) ([]byte, error) {
	if len(value) <= 7 {
		// String and binary values up to seven bytes live directly in the slot.
		// Bit 63 marks the inline form, bits 56..62 hold the length, and the
		// low 56 bits hold bytes in little-endian order.
		var packed uint64 = uint64(len(value)|0x80) << 56
		for i, part := range value {
			packed |= uint64(part) << (8 * i)
		}
		binary.LittleEndian.PutUint64(encoded[slot:], packed)
		return encoded, nil
	}
	return appendArrayVariable(encoded, slot, value, align8(len(value)))
}

func unpackedArrayBytes(encoded []byte, slot int) ([]byte, error) {
	packed := binary.LittleEndian.Uint64(encoded[slot:])
	if packed>>63 != 0 {
		length := int(byte(packed >> 56 & 0x7f))
		if length > 7 {
			return nil, errors.New("invalid packed array length")
		}
		value := make([]byte, length)
		for i := range value {
			value[i] = byte(packed >> (8 * i))
		}
		return value, nil
	}
	return arrayVariable(encoded, slot)
}

func appendArrayVariable(encoded []byte, slot int, value []byte, reserved int) ([]byte, error) {
	if reserved < len(value) || reserved > maxRowBytes-len(encoded) {
		return nil, errors.New("invalid array variable length")
	}
	offset := len(encoded)
	encoded = append(encoded, make([]byte, reserved)...)
	copy(encoded[offset:], value)
	putOffsetSize(encoded, slot, offset, len(value))
	return encoded, nil
}

func arrayVariable(encoded []byte, slot int) ([]byte, error) {
	offset, size := offsetSize(encoded, slot)
	value, _, err := readFixed(encoded, offset, size)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), value...), nil
}

func putOffsetSize(encoded []byte, slot, offset, size int) {
	// Variable slots logically pack offset in the upper uint32 and byte size in
	// the lower uint32. Both components must remain within maxRowBytes, which
	// also keeps their conversion to uint32 lossless.
	binary.LittleEndian.PutUint64(encoded[slot:], uint64(uint32(offset))<<32|uint64(uint32(size)))
}

func offsetSize(encoded []byte, slot int) (int, int) {
	value := binary.LittleEndian.Uint64(encoded[slot:])
	return int(value >> 32), int(uint32(value))
}

func align8(value int) int {
	return (value + 7) &^ 7
}
