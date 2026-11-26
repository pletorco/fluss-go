package fgo

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func validateArrowBatchNullability(schema *arrow.Schema, batch arrow.RecordBatch) error {
	for i, field := range schema.Fields() {
		if err := validateArrowField(field, batch.Column(i), 0, int(batch.NumRows())); err != nil {
			return err
		}
	}
	return nil
}

func validateArrowField(field arrow.Field, column arrow.Array, start, end int) error {
	if !field.Nullable && column.NullN() != 0 {
		for i := start; i < end; i++ {
			if column.IsNull(i) {
				return fmt.Errorf("%w: required Arrow field %q is null", ErrInvalidRow, field.Name)
			}
		}
	}
	var err error
	switch values := column.(type) {
	case *array.Struct:
		fields := field.Type.(*arrow.StructType)
		err = visitValidArrowRanges(column, start, end, func(lo, hi int) error {
			for i := range values.NumField() {
				if err := validateArrowField(fields.Field(i), values.Field(i), lo, hi); err != nil {
					return err
				}
			}
			return nil
		})
	case *array.List:
		err = validateArrowListElements(field.Type.(*arrow.ListType).ElemField(), values, start, end)
	case *array.Map:
		err = validateArrowListElements(field.Type.(*arrow.MapType).ElemField(), values.List, start, end)
	}
	if err != nil {
		return fmt.Errorf("Arrow field %q: %w", field.Name, err)
	}
	return nil
}

func validateArrowListElements(field arrow.Field, values *array.List, start, end int) error {
	return visitValidArrowRanges(values, start, end, func(lo, hi int) error {
		first, _ := values.ValueOffsets(lo)
		_, last := values.ValueOffsets(hi - 1)
		return validateArrowField(field, values.ListValues(), int(first), int(last))
	})
}

// Child data under a null parent is not observable. Visit only the ranges
// covered by valid parents, including when the record or a list is sliced.
func visitValidArrowRanges(values arrow.Array, start, end int, visit func(int, int) error) error {
	if start >= end {
		return nil
	}
	if values.NullN() == 0 {
		return visit(start, end)
	}
	for start < end {
		if values.IsNull(start) {
			start++
			continue
		}
		stop := start + 1
		for stop < end && !values.IsNull(stop) {
			stop++
		}
		if err := visit(start, stop); err != nil {
			return err
		}
		start = stop
	}
	return nil
}
