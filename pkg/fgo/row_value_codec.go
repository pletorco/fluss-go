package fgo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"
)

const millisPerDay = int64(24 * time.Hour / time.Millisecond)

func appendCompactedValue(dst []byte, logicalType LogicalType, value any) ([]byte, error) {
	switch dataTypeForLogicalType(logicalType) {
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
	case DateType, TimeType:
		return appendVar32(dst, uint32(temporalInt(dataTypeForLogicalType(logicalType), value.(time.Time)))), nil
	case TimestampType, TimestampLTZType:
		millis, nanos := timestampParts(dataTypeForLogicalType(logicalType), value.(time.Time))
		dst = appendVar64(dst, uint64(millis))
		if logicalType.Precision > 3 {
			dst = appendVar32(dst, uint32(nanos))
		}
		return dst, nil
	case DecimalType:
		unscaled, err := decimalUnscaled(value.(*big.Rat), logicalType)
		if err != nil {
			return nil, err
		}
		if logicalType.Precision <= 18 {
			return appendVar64(dst, uint64(unscaled.Int64())), nil
		}
		return appendLengthBytes(dst, signedBigEndian(unscaled))
	case ArrayType:
		value, err := encodeBinaryArray(*logicalType.Element, value.([]any), compactedEncoding)
		if err != nil {
			return nil, err
		}
		return appendLengthBytes(dst, value)
	case MapType:
		value, err := encodeBinaryMap(logicalType, value, compactedEncoding)
		if err != nil {
			return nil, err
		}
		return appendLengthBytes(dst, value)
	case RowType:
		value, err := encodeRow(nestedSchema(logicalType), value.(Row), compactedEncoding)
		if err != nil {
			return nil, err
		}
		return appendLengthBytes(dst, value)
	default:
		return nil, fmt.Errorf("unsupported compacted type %s", logicalType.Root)
	}
}

func readCompactedValue(encoded []byte, position int, logicalType LogicalType) (any, int, error) {
	switch dataTypeForLogicalType(logicalType) {
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
		return int8(value[0]), next, nil
	case SmallIntType:
		value, next, err := readFixed(encoded, position, 2)
		if err != nil {
			return nil, 0, err
		}
		return int16(binary.LittleEndian.Uint16(value)), next, nil
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
		return math.Float32frombits(binary.LittleEndian.Uint32(value)), next, nil
	case DoubleType:
		value, next, err := readFixed(encoded, position, 8)
		if err != nil {
			return nil, 0, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(value)), next, nil
	case StringType, CharType:
		value, next, err := readLengthBytes(encoded, position)
		return string(value), next, err
	case BytesType, BinaryType:
		return readLengthBytes(encoded, position)
	case DateType, TimeType:
		value, next, err := readVar(encoded, position, 5)
		if err != nil {
			return nil, 0, err
		}
		return temporalTime(dataTypeForLogicalType(logicalType), int32(value)), next, nil
	case TimestampType, TimestampLTZType:
		millis, next, err := readVar(encoded, position, 10)
		if err != nil {
			return nil, 0, err
		}
		nanos := uint64(0)
		if logicalType.Precision > 3 {
			nanos, next, err = readVar(encoded, next, 5)
			if err != nil || nanos > 999999 {
				return nil, 0, errors.New("invalid timestamp nanos")
			}
		}
		return timestampTime(dataTypeForLogicalType(logicalType), int64(millis), int32(nanos)), next, nil
	case DecimalType:
		var unscaled *big.Int
		var next int
		if logicalType.Precision <= 18 {
			value, position, err := readVar(encoded, position, 10)
			if err != nil {
				return nil, 0, err
			}
			unscaled, next = big.NewInt(int64(value)), position
		} else {
			value, position, err := readLengthBytes(encoded, position)
			if err != nil {
				return nil, 0, err
			}
			unscaled, next = signedBigInt(value), position
		}
		return scaledRat(unscaled, logicalType.Scale), next, nil
	case ArrayType:
		value, next, err := readLengthBytes(encoded, position)
		if err != nil {
			return nil, 0, err
		}
		array, err := decodeBinaryArray(*logicalType.Element, value, compactedEncoding)
		return array, next, err
	case MapType:
		value, next, err := readLengthBytes(encoded, position)
		if err != nil {
			return nil, 0, err
		}
		mapped, err := decodeBinaryMap(logicalType, value, compactedEncoding)
		return mapped, next, err
	case RowType:
		value, next, err := readLengthBytes(encoded, position)
		if err != nil {
			return nil, 0, err
		}
		row, err := decodeRow(nestedSchema(logicalType), value, compactedEncoding)
		return row, next, err
	default:
		return nil, 0, fmt.Errorf("unsupported compacted type %s", logicalType.Root)
	}
}

func appendIndexedValue(dst []byte, logicalType LogicalType, value any) ([]byte, error) {
	kind := dataTypeForLogicalType(logicalType)
	switch kind {
	case IntType:
		return appendLittle(dst, uint64(uint32(value.(int32))), 4), nil
	case BigIntType:
		return appendLittle(dst, uint64(value.(int64)), 8), nil
	case DateType, TimeType:
		return appendLittle(dst, uint64(uint32(temporalInt(kind, value.(time.Time)))), 4), nil
	case TimestampType, TimestampLTZType:
		millis, nanos := timestampParts(kind, value.(time.Time))
		dst = appendLittle(dst, uint64(millis), 8)
		if logicalType.Precision > 3 {
			dst = appendLittle(dst, uint64(uint32(nanos)), 4)
		}
		return dst, nil
	case StringType:
		return append(dst, value.(string)...), nil
	case CharType:
		if logicalType.Length == 0 {
			return append(dst, value.(string)...), nil
		}
		return appendFixed(dst, []byte(value.(string)), logicalType.Length)
	case BytesType:
		return append(dst, value.([]byte)...), nil
	case BinaryType:
		if logicalType.Length == 0 {
			return append(dst, value.([]byte)...), nil
		}
		return appendFixed(dst, value.([]byte), logicalType.Length)
	case DecimalType:
		unscaled, err := decimalUnscaled(value.(*big.Rat), logicalType)
		if err != nil {
			return nil, err
		}
		if logicalType.Precision <= 18 {
			return appendLittle(dst, uint64(unscaled.Int64()), 8), nil
		}
		return append(dst, signedBigEndian(unscaled)...), nil
	case ArrayType:
		value, err := encodeBinaryArray(*logicalType.Element, value.([]any), indexedEncoding)
		return append(dst, value...), err
	case MapType:
		value, err := encodeBinaryMap(logicalType, value, indexedEncoding)
		return append(dst, value...), err
	case RowType:
		value, err := encodeRow(nestedSchema(logicalType), value.(Row), indexedEncoding)
		return append(dst, value...), err
	default:
		return appendCompactedValue(dst, logicalType, value)
	}
}

func readIndexedValue(encoded []byte, position int, logicalType LogicalType, length int) (any, int, error) {
	kind := dataTypeForLogicalType(logicalType)
	if !indexedFixed(logicalType) {
		value, next, err := readFixed(encoded, position, length)
		if err != nil {
			return nil, 0, err
		}
		switch kind {
		case StringType, CharType:
			return string(value), next, nil
		case BytesType, BinaryType:
			return append([]byte(nil), value...), next, nil
		case DecimalType:
			return scaledRat(signedBigInt(value), logicalType.Scale), next, nil
		case ArrayType:
			decoded, err := decodeBinaryArray(*logicalType.Element, value, indexedEncoding)
			return decoded, next, err
		case MapType:
			decoded, err := decodeBinaryMap(logicalType, value, indexedEncoding)
			return decoded, next, err
		case RowType:
			decoded, err := decodeRow(nestedSchema(logicalType), value, indexedEncoding)
			return decoded, next, err
		}
	}
	switch kind {
	case IntType:
		value, next, err := readFixed(encoded, position, 4)
		if err != nil {
			return nil, 0, err
		}
		return int32(binary.LittleEndian.Uint32(value)), next, nil
	case BigIntType:
		value, next, err := readFixed(encoded, position, 8)
		if err != nil {
			return nil, 0, err
		}
		return int64(binary.LittleEndian.Uint64(value)), next, nil
	case DateType, TimeType:
		value, next, err := readFixed(encoded, position, 4)
		if err != nil {
			return nil, 0, err
		}
		return temporalTime(kind, int32(binary.LittleEndian.Uint32(value))), next, nil
	case TimestampType, TimestampLTZType:
		width := indexedLength(logicalType)
		value, next, err := readFixed(encoded, position, width)
		if err != nil {
			return nil, 0, err
		}
		nanos := int32(0)
		if width == 12 {
			nanos = int32(binary.LittleEndian.Uint32(value[8:]))
			if nanos > 999999 {
				return nil, 0, errors.New("invalid timestamp nanos")
			}
		}
		return timestampTime(kind, int64(binary.LittleEndian.Uint64(value)), nanos), next, nil
	case CharType:
		value, next, err := readFixed(encoded, position, logicalType.Length)
		return string(trimZero(value)), next, err
	case BinaryType:
		value, next, err := readFixed(encoded, position, logicalType.Length)
		return append([]byte(nil), trimZero(value)...), next, err
	case DecimalType:
		value, next, err := readFixed(encoded, position, 8)
		if err != nil {
			return nil, 0, err
		}
		return scaledRat(big.NewInt(int64(binary.LittleEndian.Uint64(value))), logicalType.Scale), next, nil
	default:
		return readCompactedValue(encoded, position, logicalType)
	}
}

func indexedFixed(logicalType LogicalType) bool {
	switch dataTypeForLogicalType(logicalType) {
	case BoolType, TinyIntType, SmallIntType, IntType, BigIntType, FloatType, DoubleType,
		DateType, TimeType, CharType, BinaryType, TimestampType, TimestampLTZType:
		return logicalType.Root != "" && (logicalType.Length > 0 || dataTypeForLogicalType(logicalType) != CharType && dataTypeForLogicalType(logicalType) != BinaryType)
	case DecimalType:
		return logicalType.Precision > 0 && logicalType.Precision <= 18
	default:
		return false
	}
}

func indexedLength(logicalType LogicalType) int {
	switch dataTypeForLogicalType(logicalType) {
	case BoolType, TinyIntType:
		return 1
	case SmallIntType:
		return 2
	case IntType, FloatType, DateType, TimeType:
		return 4
	case BigIntType, DoubleType, DecimalType:
		return 8
	case TimestampType, TimestampLTZType:
		if logicalType.Precision > 3 {
			return 12
		}
		return 8
	case CharType, BinaryType:
		return logicalType.Length
	default:
		return -1
	}
}

func appendFixed(dst, value []byte, length int) ([]byte, error) {
	if length < 1 || len(value) > length {
		return nil, errors.New("fixed-width value exceeds declared length")
	}
	start := len(dst)
	dst = append(dst, make([]byte, length)...)
	copy(dst[start:], value)
	return dst, nil
}

func trimZero(value []byte) []byte {
	for len(value) > 0 && value[len(value)-1] == 0 {
		value = value[:len(value)-1]
	}
	return value
}

func temporalInt(kind DataType, value time.Time) int32 {
	if kind == DateType {
		year, month, day := value.Date()
		midnight := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		return int32(midnight.Unix() / 86400)
	}
	return int32((int64(value.Hour())*int64(time.Hour) + int64(value.Minute())*int64(time.Minute) +
		int64(value.Second())*int64(time.Second) + int64(value.Nanosecond())) / int64(time.Millisecond))
}

func temporalTime(kind DataType, value int32) time.Time {
	if kind == DateType {
		return time.Unix(int64(value)*86400, 0).UTC()
	}
	return time.Unix(0, int64(value)*int64(time.Millisecond)).UTC()
}

func timestampParts(kind DataType, value time.Time) (int64, int32) {
	if kind == TimestampLTZType {
		return value.UnixMilli(), int32(value.Nanosecond() % int(time.Millisecond))
	}
	year, month, day := value.Date()
	hour, minute, second := value.Clock()
	epochDay := time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix() / 86400
	nanoOfDay := int64(hour)*int64(time.Hour) + int64(minute)*int64(time.Minute) +
		int64(second)*int64(time.Second) + int64(value.Nanosecond())
	return epochDay*millisPerDay + nanoOfDay/int64(time.Millisecond), int32(nanoOfDay % int64(time.Millisecond))
}

func timestampTime(kind DataType, millis int64, nanos int32) time.Time {
	if kind == TimestampLTZType {
		return time.UnixMilli(millis).Add(time.Duration(nanos)).UTC()
	}
	days := millis / millisPerDay
	withinDay := millis % millisPerDay
	if withinDay < 0 {
		days--
		withinDay += millisPerDay
	}
	return time.Unix(days*86400, withinDay*int64(time.Millisecond)+int64(nanos)).UTC()
}

func decimalUnscaled(value *big.Rat, logicalType LogicalType) (*big.Int, error) {
	if logicalType.Precision < 1 || logicalType.Scale < 0 {
		return nil, fmt.Errorf("%w: decimal precision and scale are required", ErrInvalidSchema)
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(logicalType.Scale)), nil)
	scaled := new(big.Rat).Mul(value, new(big.Rat).SetInt(factor))
	if !scaled.IsInt() {
		return nil, errors.New("decimal has more fractional digits than its scale")
	}
	unscaled := new(big.Int).Set(scaled.Num())
	if len(new(big.Int).Abs(new(big.Int).Set(unscaled)).String()) > logicalType.Precision {
		return nil, errors.New("decimal exceeds declared precision")
	}
	return unscaled, nil
}

func scaledRat(unscaled *big.Int, scale int) *big.Rat {
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return new(big.Rat).SetFrac(unscaled, factor)
}

func signedBigEndian(value *big.Int) []byte {
	if value.Sign() >= 0 {
		bytes := value.Bytes()
		if len(bytes) == 0 {
			return []byte{0}
		}
		if bytes[0]&0x80 != 0 {
			return append([]byte{0}, bytes...)
		}
		return bytes
	}
	width := (value.BitLen() + 8) / 8
	modulus := new(big.Int).Lsh(big.NewInt(1), uint(width*8))
	bytes := new(big.Int).Add(modulus, value).Bytes()
	if len(bytes) < width {
		bytes = append(make([]byte, width-len(bytes)), bytes...)
	}
	if bytes[0]&0x80 == 0 {
		width++
		modulus.Lsh(big.NewInt(1), uint(width*8))
		bytes = new(big.Int).Add(modulus, value).Bytes()
	}
	return bytes
}

func signedBigInt(value []byte) *big.Int {
	if len(value) == 0 {
		return new(big.Int)
	}
	decoded := new(big.Int).SetBytes(value)
	if value[0]&0x80 != 0 {
		decoded.Sub(decoded, new(big.Int).Lsh(big.NewInt(1), uint(len(value)*8)))
	}
	return decoded
}
