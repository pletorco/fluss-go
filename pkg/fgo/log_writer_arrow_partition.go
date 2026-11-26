package fgo

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func validateArrowPartition(batch arrow.RecordBatch, partitions []physicalPartitionColumn) error {
	for _, partition := range partitions {
		column := batch.Column(partition.position)
		for row := 0; row < column.Len(); row++ {
			if column.IsNull(row) || !matchesArrowPartitionValue(partition, column, row) {
				return fmt.Errorf("%w: Arrow row %d column %q differs from the selected partition", ErrInvalidRow, row, partition.name)
			}
		}
	}
	return nil
}

func matchesArrowPartitionValue(partition physicalPartitionColumn, column arrow.Array, row int) bool {
	if values, ok := column.(*array.Timestamp); ok {
		// Compare instants directly instead of converting to milliseconds,
		// which can overflow for out-of-range Arrow timestamps.
		expected, ok := partition.value.(time.Time)
		return ok && values.Value(row).ToTime(values.DataType().(*arrow.TimestampType).Unit).Equal(expected)
	}
	return partition.matches(arrowPartitionValue(column, row))
}

// Convert Arrow's physical scalar to the same Go value used by row appends.
func arrowPartitionValue(column arrow.Array, row int) any {
	switch values := column.(type) {
	case *array.Boolean:
		return values.Value(row)
	case *array.Int8:
		return values.Value(row)
	case *array.Int16:
		return values.Value(row)
	case *array.Int32:
		return values.Value(row)
	case *array.Int64:
		return values.Value(row)
	case *array.Float32:
		return values.Value(row)
	case *array.Float64:
		return values.Value(row)
	case *array.String:
		return values.Value(row)
	case *array.Binary:
		return values.Value(row)
	case *array.FixedSizeBinary:
		return values.Value(row)
	case *array.Date32:
		return values.Value(row).ToTime()
	case *array.Time32:
		value, unit := values.Value(row), values.DataType().(*arrow.Time32Type).Unit
		if validArrowPartitionClock(int64(value), unit) {
			return value.ToTime(unit)
		}
	case *array.Time64:
		value, unit := values.Value(row), values.DataType().(*arrow.Time64Type).Unit
		if validArrowPartitionClock(int64(value), unit) {
			return value.ToTime(unit)
		}
	}
	return nil
}

func validArrowPartitionClock(value int64, unit arrow.TimeUnit) bool {
	// ToTime/clock comparison would otherwise wrap invalid times into a day.
	return value >= 0 && value < int64(24*time.Hour/unit.Multiplier())
}
