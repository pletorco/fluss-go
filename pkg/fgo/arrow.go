package fgo

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// ArrowSchema converts the supported portable Fluss schema subset to Arrow.
func (s Schema) ArrowSchema() (*arrow.Schema, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	fields := make([]arrow.Field, len(s.Columns))
	for i, column := range s.Columns {
		dataType, err := arrowType(column.Type)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{Name: column.Name, Type: dataType, Nullable: column.Nullable}
	}
	return arrow.NewSchema(fields, nil), nil
}

func SchemaFromArrow(schema *arrow.Schema) (Schema, error) {
	if schema == nil {
		return Schema{}, fmt.Errorf("%w: nil Arrow schema", ErrInvalidSchema)
	}
	columns := make([]Column, len(schema.Fields()))
	for i, field := range schema.Fields() {
		kind, err := flussType(field.Type)
		if err != nil {
			return Schema{}, err
		}
		columns[i] = Column{Name: field.Name, Type: kind, Nullable: field.Nullable}
	}
	result := Schema{Columns: columns}
	return result, result.Validate()
}

func arrowType(kind DataType) (arrow.DataType, error) {
	switch kind {
	case BoolType:
		return arrow.FixedWidthTypes.Boolean, nil
	case IntType:
		return arrow.PrimitiveTypes.Int32, nil
	case BigIntType:
		return arrow.PrimitiveTypes.Int64, nil
	case FloatType:
		return arrow.PrimitiveTypes.Float32, nil
	case DoubleType:
		return arrow.PrimitiveTypes.Float64, nil
	case StringType:
		return arrow.BinaryTypes.String, nil
	case BinaryType:
		return arrow.BinaryTypes.Binary, nil
	case DateType:
		return arrow.FixedWidthTypes.Date32, nil
	case TimestampType:
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported Arrow conversion %s", ErrInvalidSchema, kind)
	}
}

func flussType(dataType arrow.DataType) (DataType, error) {
	switch dataType.ID() {
	case arrow.BOOL:
		return BoolType, nil
	case arrow.INT32:
		return IntType, nil
	case arrow.INT64:
		return BigIntType, nil
	case arrow.FLOAT32:
		return FloatType, nil
	case arrow.FLOAT64:
		return DoubleType, nil
	case arrow.STRING:
		return StringType, nil
	case arrow.BINARY:
		return BinaryType, nil
	case arrow.DATE32:
		return DateType, nil
	case arrow.TIMESTAMP:
		return TimestampType, nil
	default:
		return "", fmt.Errorf("%w: unsupported Arrow type %s", ErrInvalidSchema, dataType)
	}
}
