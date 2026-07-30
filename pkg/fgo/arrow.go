package fgo

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// ArrowSchema converts a Fluss schema to the exact Arrow field layout used by Fluss 0.9.1.
func (s Schema) ArrowSchema() (*arrow.Schema, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	fields := make([]arrow.Field, len(s.Columns))
	for i, column := range s.Columns {
		field, err := arrowField(column.Name, logicalTypeForColumn(column))
		if err != nil {
			return nil, fmt.Errorf("%w: column %q: %v", ErrInvalidSchema, column.Name, err)
		}
		fields[i] = field
	}
	return arrow.NewSchema(fields, nil), nil
}

// SchemaFromArrow reconstructs all logical parameters represented by Arrow. CHAR is returned as
// STRING and local-zoned timestamps as TIMESTAMP because Arrow does not retain those distinctions.
func SchemaFromArrow(schema *arrow.Schema) (Schema, error) {
	if schema == nil {
		return Schema{}, fmt.Errorf("%w: nil Arrow schema", ErrInvalidSchema)
	}
	columns := make([]Column, len(schema.Fields()))
	for i, field := range schema.Fields() {
		logicalType, err := logicalTypeFromArrow(field)
		if err != nil {
			return Schema{}, err
		}
		columns[i] = Column{
			Name:        field.Name,
			Type:        dataTypeForLogicalType(logicalType),
			Nullable:    field.Nullable,
			LogicalType: &logicalType,
		}
	}
	result := Schema{Columns: columns}
	return result, result.Validate()
}

func arrowField(name string, logicalType LogicalType) (arrow.Field, error) {
	dataType, err := arrowType(logicalType)
	if err != nil {
		return arrow.Field{}, err
	}
	return arrow.Field{Name: name, Type: dataType, Nullable: logicalType.Nullable}, nil
}

func arrowType(logicalType LogicalType) (arrow.DataType, error) {
	kind := dataTypeForLogicalType(logicalType)
	if primitive, ok := arrowPrimitiveTypes()[kind]; ok {
		return primitive, nil
	}
	switch kind {
	case BinaryType:
		return arrowBinaryType(logicalType)
	case DecimalType:
		return arrowDecimalType(logicalType)
	case TimeType:
		return arrowTimeType(logicalType.Precision)
	case TimestampType, TimestampLTZType:
		return &arrow.TimestampType{Unit: arrowUnit(logicalType.Precision)}, nil
	case ArrayType:
		return arrowArrayType(logicalType)
	case MapType:
		return arrowMapType(logicalType)
	case RowType:
		return arrowRowType(logicalType)
	default:
		return nil, fmt.Errorf("unsupported Arrow conversion %s", logicalType.Root)
	}
}

func arrowBinaryType(logicalType LogicalType) (arrow.DataType, error) {
	if logicalType.Length < 1 {
		return nil, fmt.Errorf("BINARY length is required")
	}
	return &arrow.FixedSizeBinaryType{ByteWidth: logicalType.Length}, nil
}

func arrowDecimalType(logicalType LogicalType) (arrow.DataType, error) {
	if logicalType.Precision < 1 || logicalType.Precision > 38 {
		return nil, fmt.Errorf("DECIMAL precision must be in [1, 38]")
	}
	return &arrow.Decimal128Type{Precision: int32(logicalType.Precision), Scale: int32(logicalType.Scale)}, nil
}

func arrowArrayType(logicalType LogicalType) (arrow.DataType, error) {
	if logicalType.Element == nil {
		return nil, fmt.Errorf("ARRAY element type is required")
	}
	element, err := arrowField("element", *logicalType.Element)
	if err != nil {
		return nil, err
	}
	return arrow.ListOfField(element), nil
}

func arrowMapType(logicalType LogicalType) (arrow.DataType, error) {
	if logicalType.Key == nil || logicalType.Value == nil {
		return nil, fmt.Errorf("MAP key and value types are required")
	}
	key, err := arrowField("key", *logicalType.Key)
	if err != nil {
		return nil, err
	}
	key.Nullable = false
	value, err := arrowField("value", *logicalType.Value)
	if err != nil {
		return nil, err
	}
	return arrow.MapOfFields(key, value), nil
}

func arrowRowType(logicalType LogicalType) (arrow.DataType, error) {
	fields := make([]arrow.Field, len(logicalType.Fields))
	for index, field := range logicalType.Fields {
		nested, err := arrowField(field.Name, field.Type)
		if err != nil {
			return nil, fmt.Errorf("invalid ROW field %q: %w", field.Name, err)
		}
		fields[index] = nested
	}
	return arrow.StructOf(fields...), nil
}

func arrowPrimitiveTypes() map[DataType]arrow.DataType {
	return map[DataType]arrow.DataType{
		BoolType: arrow.FixedWidthTypes.Boolean, TinyIntType: arrow.PrimitiveTypes.Int8,
		SmallIntType: arrow.PrimitiveTypes.Int16, IntType: arrow.PrimitiveTypes.Int32,
		BigIntType: arrow.PrimitiveTypes.Int64, FloatType: arrow.PrimitiveTypes.Float32,
		DoubleType: arrow.PrimitiveTypes.Float64, CharType: arrow.BinaryTypes.String,
		StringType: arrow.BinaryTypes.String, BytesType: arrow.BinaryTypes.Binary,
		DateType: arrow.FixedWidthTypes.Date32,
	}
}

func logicalTypeFromArrow(field arrow.Field) (LogicalType, error) {
	logicalType := LogicalType{Nullable: field.Nullable}
	if root, ok := primitiveLogicalRoot(field.Type); ok {
		logicalType.Root = root
		return logicalType, logicalType.Validate()
	}
	switch dataType := field.Type.(type) {
	case *arrow.FixedSizeBinaryType:
		logicalType.Root, logicalType.Length = "BINARY", dataType.ByteWidth
	case *arrow.Decimal128Type:
		logicalType.Root, logicalType.Precision, logicalType.Scale = "DECIMAL", int(dataType.Precision), int(dataType.Scale)
	case *arrow.Time32Type:
		logicalType.Root, logicalType.Precision = "TIME_WITHOUT_TIME_ZONE", precisionForUnit(dataType.Unit)
	case *arrow.Time64Type:
		logicalType.Root, logicalType.Precision = "TIME_WITHOUT_TIME_ZONE", precisionForUnit(dataType.Unit)
	case *arrow.TimestampType:
		logicalType.Root, logicalType.Precision = "TIMESTAMP_WITHOUT_TIME_ZONE", precisionForUnit(dataType.Unit)
	case *arrow.ListType:
		return logicalArrayFromArrow(field.Nullable, dataType)
	case *arrow.MapType:
		return logicalMapFromArrow(field.Nullable, dataType)
	case *arrow.StructType:
		return logicalRowFromArrow(field.Nullable, dataType)
	default:
		return LogicalType{}, fmt.Errorf("%w: unsupported Arrow type %s", ErrInvalidSchema, field.Type)
	}
	return logicalType, logicalType.Validate()
}

func logicalArrayFromArrow(nullable bool, dataType *arrow.ListType) (LogicalType, error) {
	element, err := logicalTypeFromArrow(dataType.ElemField())
	if err != nil {
		return LogicalType{}, err
	}
	logicalType := LogicalType{Root: "ARRAY", Nullable: nullable, Element: &element}
	return logicalType, logicalType.Validate()
}

func logicalMapFromArrow(nullable bool, dataType *arrow.MapType) (LogicalType, error) {
	key, err := logicalTypeFromArrow(dataType.KeyField())
	if err != nil {
		return LogicalType{}, err
	}
	key.Nullable = false
	value, err := logicalTypeFromArrow(dataType.ItemField())
	if err != nil {
		return LogicalType{}, err
	}
	logicalType := LogicalType{Root: "MAP", Nullable: nullable, Key: &key, Value: &value}
	return logicalType, logicalType.Validate()
}

func logicalRowFromArrow(nullable bool, dataType *arrow.StructType) (LogicalType, error) {
	logicalType := LogicalType{Root: "ROW", Nullable: nullable, Fields: make([]LogicalField, len(dataType.Fields()))}
	for index, nested := range dataType.Fields() {
		nestedType, err := logicalTypeFromArrow(nested)
		if err != nil {
			return LogicalType{}, err
		}
		logicalType.Fields[index] = LogicalField{Name: nested.Name, Type: nestedType}
	}
	return logicalType, logicalType.Validate()
}

func primitiveLogicalRoot(dataType arrow.DataType) (string, bool) {
	switch dataType.(type) {
	case *arrow.BooleanType:
		return "BOOLEAN", true
	case *arrow.Int8Type:
		return "TINYINT", true
	case *arrow.Int16Type:
		return "SMALLINT", true
	case *arrow.Int32Type:
		return "INTEGER", true
	case *arrow.Int64Type:
		return "BIGINT", true
	case *arrow.Float32Type:
		return "FLOAT", true
	case *arrow.Float64Type:
		return "DOUBLE", true
	case *arrow.StringType:
		return "STRING", true
	case *arrow.BinaryType:
		return "BYTES", true
	case *arrow.Date32Type:
		return "DATE", true
	default:
		return "", false
	}
}

func arrowTimeType(precision int) (arrow.DataType, error) {
	switch {
	case precision == 0:
		return &arrow.Time32Type{Unit: arrow.Second}, nil
	case precision <= 3:
		return &arrow.Time32Type{Unit: arrow.Millisecond}, nil
	case precision <= 6:
		return &arrow.Time64Type{Unit: arrow.Microsecond}, nil
	case precision <= 9:
		return &arrow.Time64Type{Unit: arrow.Nanosecond}, nil
	default:
		return nil, fmt.Errorf("invalid TIME precision %d", precision)
	}
}

func arrowUnit(precision int) arrow.TimeUnit {
	switch {
	case precision == 0:
		return arrow.Second
	case precision <= 3:
		return arrow.Millisecond
	case precision <= 6:
		return arrow.Microsecond
	default:
		return arrow.Nanosecond
	}
}

func precisionForUnit(unit arrow.TimeUnit) int {
	switch unit {
	case arrow.Second:
		return 0
	case arrow.Millisecond:
		return 3
	case arrow.Microsecond:
		return 6
	default:
		return 9
	}
}
