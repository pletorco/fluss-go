package fgo

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

type kvPartitionColumn struct {
	name  string
	kind  DataType
	value any
}

func kvPartitionColumns(schema Schema, name string) ([]kvPartitionColumn, error) {
	if len(schema.PartitionKey) == 0 {
		return nil, nil
	}
	spec, err := PartitionSpecFromName(schema.PartitionKey, name)
	if err != nil {
		return nil, err
	}
	columns := schema.columnsByName()
	result := make([]kvPartitionColumn, 0, len(spec))
	for _, key := range schema.PartitionKey {
		if !slices.Contains(schema.PrimaryKey, key) {
			return nil, fmt.Errorf("%w: partition key %q is not part of the logical primary key", ErrInvalidSchema, key)
		}
		kind := dataTypeForLogicalType(logicalTypeForColumn(columns[key]))
		value, err := parseKVPartitionValue(kind, spec[key])
		if err != nil {
			return nil, fmt.Errorf("%w: selected partition has an invalid %s value for column %q", ErrInvalidConfig, kind, key)
		}
		result = append(result, kvPartitionColumn{name: key, kind: kind, value: value})
	}
	return result, nil
}

// Fluss names partitions using PartitionNameConverters. Parse once rather than
// reproducing Java's numeric text formatting on every write or lookup.
func parseKVPartitionValue(kind DataType, value string) (any, error) {
	switch kind {
	case StringType, CharType:
		return value, nil
	case BoolType:
		switch strings.ToLower(value) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return nil, ErrInvalidConfig
		}
	case BinaryType, BytesType:
		return hex.DecodeString(value)
	case TinyIntType:
		number, err := strconv.ParseInt(value, 10, 8)
		return int8(number), err
	case SmallIntType:
		number, err := strconv.ParseInt(value, 10, 16)
		return int16(number), err
	case IntType:
		number, err := strconv.ParseInt(value, 10, 32)
		return int32(number), err
	case BigIntType:
		return strconv.ParseInt(value, 10, 64)
	case FloatType:
		number, err := strconv.ParseFloat(strings.ReplaceAll(value, "_", "."), 32)
		return float32(number), err
	case DoubleType:
		return strconv.ParseFloat(strings.ReplaceAll(value, "_", "."), 64)
	default:
		return parseKVPartitionTime(kind, value)
	}
}

func parseKVPartitionTime(kind DataType, value string) (time.Time, error) {
	switch kind {
	case DateType:
		return time.Parse(time.DateOnly, value)
	case TimeType:
		return time.Parse("15-04-05.000", strings.Replace(value, "_", ".", 1))
	case TimestampType, TimestampLTZType:
		const layout = "2006-01-02-15-04-05"
		clock, fraction, hasFraction := strings.Cut(value, "_")
		width := len(clock)
		if hasFraction && width != len(layout) {
			return time.Time{}, ErrInvalidConfig
		}
		if fraction != "" {
			if len(fraction) > 9 || strings.Trim(fraction, "0123456789") != "" {
				return time.Time{}, ErrInvalidConfig
			}
			return time.Parse(layout+".999999999", clock+"."+fraction)
		}
		switch width {
		case len(time.DateOnly), len("2006-01-02-15"), len("2006-01-02-15-04"), len(layout):
			return time.Parse(layout[:width], clock)
		default:
			return time.Time{}, ErrInvalidConfig
		}
	default:
		return time.Time{}, ErrInvalidConfig
	}
}

func (p kvPartitionColumn) matches(value any) bool {
	switch expected := p.value.(type) {
	case []byte:
		actual, ok := value.([]byte)
		return ok && bytes.Equal(actual, expected)
	case float32:
		actual, ok := value.(float32)
		return ok && (math.Float32bits(actual) == math.Float32bits(expected) ||
			math.IsNaN(float64(actual)) && math.IsNaN(float64(expected)))
	case float64:
		actual, ok := value.(float64)
		return ok && (math.Float64bits(actual) == math.Float64bits(expected) ||
			math.IsNaN(actual) && math.IsNaN(expected))
	case time.Time:
		actual, ok := value.(time.Time)
		return ok && kvPartitionTimeMatches(p.kind, actual, expected)
	default:
		return value == expected
	}
}

func kvPartitionTimeMatches(kind DataType, actual, expected time.Time) bool {
	switch kind {
	case DateType:
		return temporalInt(kind, actual) == temporalInt(kind, expected)
	case TimeType:
		encoded, err := rowTemporalInt(kind, actual)
		return err == nil && encoded == temporalInt(kind, expected)
	default:
		millis, nanos := timestampParts(kind, actual)
		wantMillis, wantNanos := timestampParts(kind, expected)
		return millis == wantMillis && nanos == wantNanos
	}
}
