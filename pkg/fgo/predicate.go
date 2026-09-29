package fgo

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// Wire constants pinned to Apache Fluss 1.0 PredicateMessageUtils. They are
// documented as comments in FlussApi.proto because the protocol uses int32.
const (
	predicateTypeLeaf     int32 = 0
	predicateTypeCompound int32 = 1

	leafEqual          int32 = 0
	leafNotEqual       int32 = 1
	leafLessThan       int32 = 2
	leafLessOrEqual    int32 = 3
	leafGreaterThan    int32 = 4
	leafGreaterOrEqual int32 = 5
	leafIsNull         int32 = 6
	leafIsNotNull      int32 = 7
	leafStartsWith     int32 = 8
	leafContains       int32 = 9
	leafEndsWith       int32 = 10
	leafIn             int32 = 11
	leafNotIn          int32 = 12

	compoundAnd int32 = 0
	compoundOr  int32 = 1
)

// Predicate is a server-side filter for [WithScanFilter].
//
// A Predicate only reduces what the server sends. Fluss skips whole record
// batches whose statistics cannot match, so a scan still returns a superset of
// the matching rows and callers that need exact results must filter again.
// Build predicates with [Col], [And], and [Or]. The zero Predicate is invalid.
type Predicate struct {
	compound bool
	function int32
	column   string
	literals []any
	children []Predicate
	err      error
}

// PredicateColumn names a top-level column for building a [Predicate].
type PredicateColumn struct{ name string }

// Col starts a predicate on the named top-level column.
func Col(name string) PredicateColumn { return PredicateColumn{name: name} }

// Eq matches rows whose column equals value.
func (c PredicateColumn) Eq(value any) Predicate { return c.leaf(leafEqual, value) }

// Ne matches rows whose column differs from value.
func (c PredicateColumn) Ne(value any) Predicate { return c.leaf(leafNotEqual, value) }

// Lt matches rows whose column is less than value.
func (c PredicateColumn) Lt(value any) Predicate { return c.leaf(leafLessThan, value) }

// Le matches rows whose column is less than or equal to value.
func (c PredicateColumn) Le(value any) Predicate { return c.leaf(leafLessOrEqual, value) }

// Gt matches rows whose column is greater than value.
func (c PredicateColumn) Gt(value any) Predicate { return c.leaf(leafGreaterThan, value) }

// Ge matches rows whose column is greater than or equal to value.
func (c PredicateColumn) Ge(value any) Predicate { return c.leaf(leafGreaterOrEqual, value) }

// IsNull matches rows whose column is null.
func (c PredicateColumn) IsNull() Predicate { return c.leaf(leafIsNull) }

// IsNotNull matches rows whose column is not null.
func (c PredicateColumn) IsNotNull() Predicate { return c.leaf(leafIsNotNull) }

// In matches rows whose column equals any value. A nil value matches null.
func (c PredicateColumn) In(values ...any) Predicate { return c.leaf(leafIn, values...) }

// NotIn matches rows whose column equals none of values.
func (c PredicateColumn) NotIn(values ...any) Predicate { return c.leaf(leafNotIn, values...) }

// StartsWith matches CHAR or STRING values with the given prefix.
func (c PredicateColumn) StartsWith(prefix string) Predicate { return c.leaf(leafStartsWith, prefix) }

// EndsWith matches CHAR or STRING values with the given suffix.
func (c PredicateColumn) EndsWith(suffix string) Predicate { return c.leaf(leafEndsWith, suffix) }

// Contains matches CHAR or STRING values containing substring.
func (c PredicateColumn) Contains(substring string) Predicate {
	return c.leaf(leafContains, substring)
}

func (c PredicateColumn) leaf(function int32, literals ...any) Predicate {
	return Predicate{function: function, column: c.name, literals: literals}
}

// And matches rows that satisfy every predicate. A single predicate is
// returned unchanged; no predicates is an error reported when the scanner is
// created.
func And(predicates ...Predicate) Predicate { return combine(compoundAnd, predicates) }

// Or matches rows that satisfy at least one predicate. A single predicate is
// returned unchanged; no predicates is an error reported when the scanner is
// created.
func Or(predicates ...Predicate) Predicate { return combine(compoundOr, predicates) }

func combine(function int32, predicates []Predicate) Predicate {
	if len(predicates) == 1 {
		return predicates[0]
	}
	return Predicate{
		compound: true, function: function,
		children: append([]Predicate(nil), predicates...),
	}
}

// predicateEncoder resolves a Predicate against one table schema.
type predicateEncoder struct {
	columns map[string]Column
	ids     map[int]int
}

// compilePredicate encodes predicate against schema for a Fluss fetch request.
// The server evaluates filters before projection, so schema must be the full
// table schema rather than the projected one.
func compilePredicate(predicate Predicate, schema Schema) (*fmsg.PbPredicate, error) {
	encoder := predicateEncoder{
		columns: make(map[string]Column, len(schema.Columns)),
		ids:     make(map[int]int, len(schema.Columns)),
	}
	for _, column := range schema.Columns {
		encoder.columns[column.Name] = column
		encoder.ids[column.ID]++
	}
	return encoder.encode(predicate)
}

func (e predicateEncoder) encode(predicate Predicate) (*fmsg.PbPredicate, error) {
	if predicate.err != nil {
		return nil, predicate.err
	}
	if !predicate.compound {
		return e.encodeLeaf(predicate)
	}
	if len(predicate.children) == 0 {
		return nil, predicateError("compound predicate has no children")
	}
	children := make([]*fmsg.PbPredicate, 0, len(predicate.children))
	for _, child := range predicate.children {
		encoded, err := e.encode(child)
		if err != nil {
			return nil, err
		}
		children = append(children, encoded)
	}
	return &fmsg.PbPredicate{
		Type: proto.Int32(predicateTypeCompound),
		Compound: &fmsg.PbCompoundPredicate{
			Function: proto.Int32(predicate.function), Children: children,
		},
	}, nil
}

func (e predicateEncoder) encodeLeaf(predicate Predicate) (*fmsg.PbPredicate, error) {
	if predicate.column == "" && predicate.function == 0 && predicate.literals == nil {
		return nil, predicateError("predicate is empty")
	}
	column, ok := e.columns[predicate.column]
	if !ok {
		return nil, predicateError("unknown column %q", predicate.column)
	}
	if column.ID < 0 || e.ids[column.ID] != 1 {
		return nil, predicateError("column %q has no assigned field ID", column.Name)
	}
	logical := logicalTypeForColumn(column)
	kind := dataTypeForLogicalType(logical)
	code, ok := predicateLiteralType(kind)
	if !ok {
		return nil, predicateError("column %q of type %s cannot be filtered", column.Name, kind)
	}
	if err := checkLeafShape(predicate, column.Name, kind); err != nil {
		return nil, err
	}
	literals := make([]*fmsg.PbLiteralValue, 0, len(predicate.literals))
	for _, literal := range predicate.literals {
		encoded, err := encodeLiteral(literal, code, kind, logical)
		if err != nil {
			return nil, predicateError("column %q: %v", column.Name, err)
		}
		literals = append(literals, encoded)
	}
	return &fmsg.PbPredicate{
		Type: proto.Int32(predicateTypeLeaf),
		Leaf: &fmsg.PbLeafPredicate{
			Function: proto.Int32(predicate.function),
			FieldId:  proto.Int32(int32(column.ID)),
			Literals: literals,
		},
	}, nil
}

func checkLeafShape(predicate Predicate, name string, kind DataType) error {
	count := len(predicate.literals)
	switch predicate.function {
	case leafIsNull, leafIsNotNull:
		if count != 0 {
			return predicateError("column %q: null checks take no literals", name)
		}
	case leafIn, leafNotIn:
		if count == 0 {
			return predicateError("column %q: IN requires at least one value", name)
		}
	case leafStartsWith, leafContains, leafEndsWith:
		if kind != StringType && kind != CharType {
			return predicateError("column %q of type %s does not support string matching", name, kind)
		}
		fallthrough
	default:
		if count != 1 {
			return predicateError("column %q: comparison requires exactly one value", name)
		}
		if predicate.literals[0] == nil {
			return predicateError("column %q: use IsNull to match null", name)
		}
	}
	return nil
}

func predicateError(format string, args ...any) error {
	return fmt.Errorf("%w: predicate: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
}

// predicateLiteralType returns the PbDataTypeRoot code for a filterable type.
func predicateLiteralType(kind DataType) (int32, bool) {
	switch kind {
	case BoolType:
		return 0, true
	case TinyIntType:
		return 1, true
	case SmallIntType:
		return 2, true
	case IntType:
		return 3, true
	case BigIntType:
		return 4, true
	case FloatType:
		return 5, true
	case DoubleType:
		return 6, true
	case CharType:
		return 7, true
	case StringType:
		return 8, true
	case DecimalType:
		return 9, true
	case DateType:
		return 10, true
	case TimeType:
		return 11, true
	case TimestampType:
		return 12, true
	case TimestampLTZType:
		return 13, true
	case BinaryType:
		return 14, true
	case BytesType:
		return 15, true
	default:
		return 0, false
	}
}

func encodeLiteral(value any, code int32, kind DataType, logical LogicalType) (*fmsg.PbLiteralValue, error) {
	literal := &fmsg.PbLiteralValue{LiteralType: proto.Int32(code), IsNull: proto.Bool(value == nil)}
	if value == nil {
		return literal, nil
	}
	switch kind {
	case BoolType:
		v, ok := value.(bool)
		if !ok {
			return nil, literalTypeError(value, kind)
		}
		literal.BooleanValue = proto.Bool(v)
	case TinyIntType, SmallIntType, IntType:
		v, err := integerLiteral(value, kind)
		if err != nil {
			return nil, err
		}
		literal.IntValue = proto.Int32(int32(v))
	case BigIntType:
		v, err := integerLiteral(value, kind)
		if err != nil {
			return nil, err
		}
		literal.BigintValue = proto.Int64(v)
	case FloatType:
		v, err := floatLiteral(value, 24)
		if err != nil {
			return nil, err
		}
		literal.FloatValue = proto.Float32(float32(v))
	case DoubleType:
		v, err := floatLiteral(value, 53)
		if err != nil {
			return nil, err
		}
		literal.DoubleValue = proto.Float64(v)
	case StringType, CharType:
		v, ok := value.(string)
		if !ok {
			return nil, literalTypeError(value, kind)
		}
		literal.StringValue = proto.String(v)
	case BinaryType, BytesType:
		v, ok := value.([]byte)
		if !ok {
			return nil, literalTypeError(value, kind)
		}
		literal.BinaryValue = append([]byte(nil), v...)
	case DecimalType:
		return literal, encodeDecimalLiteral(literal, value, logical)
	case DateType, TimeType, TimestampType, TimestampLTZType:
		return literal, encodeTemporalLiteral(literal, value, kind, logical)
	}
	return literal, nil
}

func literalTypeError(value any, kind DataType) error {
	return fmt.Errorf("%T literal does not match %s column", value, kind)
}

func integerLiteral(value any, kind DataType) (int64, error) {
	var v int64
	switch n := value.(type) {
	case int:
		v = int64(n)
	case int8:
		v = int64(n)
	case int16:
		v = int64(n)
	case int32:
		v = int64(n)
	case int64:
		v = n
	case uint:
		if uint64(n) > math.MaxInt64 {
			return 0, fmt.Errorf("literal %d overflows %s", n, kind)
		}
		v = int64(n)
	case uint8:
		v = int64(n)
	case uint16:
		v = int64(n)
	case uint32:
		v = int64(n)
	case uint64:
		if n > math.MaxInt64 {
			return 0, fmt.Errorf("literal %d overflows %s", n, kind)
		}
		v = int64(n)
	default:
		return 0, literalTypeError(value, kind)
	}
	low, high := int64(math.MinInt64), int64(math.MaxInt64)
	switch kind {
	case TinyIntType:
		low, high = math.MinInt8, math.MaxInt8
	case SmallIntType:
		low, high = math.MinInt16, math.MaxInt16
	case IntType:
		low, high = math.MinInt32, math.MaxInt32
	}
	if v < low || v > high {
		return 0, fmt.Errorf("literal %d overflows %s", v, kind)
	}
	return v, nil
}

// floatLiteral accepts floats and integers that convert exactly into a
// floating-point significand of the given bit width.
func floatLiteral(value any, mantissaBits uint) (float64, error) {
	var v float64
	switch n := value.(type) {
	case float32:
		v = float64(n)
	case float64:
		v = n
		if mantissaBits == 24 && !math.IsNaN(v) && !math.IsInf(v, 0) && float64(float32(v)) != v {
			return 0, fmt.Errorf("literal %v is not exactly representable as FLOAT", n)
		}
	default:
		integer, err := integerLiteral(value, BigIntType)
		if err != nil {
			return 0, fmt.Errorf("%T literal does not match floating-point column", value)
		}
		limit := int64(1) << mantissaBits
		if integer > limit || integer < -limit {
			return 0, fmt.Errorf("literal %d is not exactly representable", integer)
		}
		v = float64(integer)
	}
	return v, nil
}

func encodeDecimalLiteral(literal *fmsg.PbLiteralValue, value any, logical LogicalType) error {
	var rat *big.Rat
	switch v := value.(type) {
	case *big.Rat:
		if v == nil {
			return literalTypeError(value, DecimalType)
		}
		rat = v
	default:
		integer, err := integerLiteral(value, BigIntType)
		if err != nil {
			return literalTypeError(value, DecimalType)
		}
		rat = new(big.Rat).SetInt64(integer)
	}
	unscaled, err := decimalUnscaled(rat, logical)
	if err != nil {
		return err
	}
	if logical.Precision <= 18 {
		literal.DecimalValue = proto.Int64(unscaled.Int64())
	} else {
		literal.DecimalBytes = signedBigEndian(unscaled)
	}
	return nil
}

func encodeTemporalLiteral(literal *fmsg.PbLiteralValue, value any, kind DataType, logical LogicalType) error {
	v, ok := value.(time.Time)
	if !ok {
		return literalTypeError(value, kind)
	}
	switch kind {
	case DateType:
		hour, minute, second := v.Clock()
		if hour != 0 || minute != 0 || second != 0 || v.Nanosecond() != 0 {
			return errors.New("DATE literal must be at midnight")
		}
		literal.BigintValue = proto.Int64(int64(temporalInt(DateType, v)))
	case TimeType:
		if v.Nanosecond()%int(time.Millisecond) != 0 {
			return errors.New("TIME literal has sub-millisecond precision")
		}
		literal.IntValue = proto.Int32(temporalInt(TimeType, v))
	default:
		precision := logical.Precision
		if precision < 0 || precision > 9 {
			return fmt.Errorf("invalid %s precision %d", kind, precision)
		}
		granularity := int(time.Millisecond)
		if precision > 3 {
			granularity = 1
			for range 9 - precision {
				granularity *= 10
			}
		}
		if v.Nanosecond()%int(time.Millisecond)%granularity != 0 {
			return fmt.Errorf("%s literal exceeds column precision %d", kind, precision)
		}
		millis, nanos := timestampParts(kind, v)
		literal.TimestampMillisValue = proto.Int64(millis)
		literal.TimestampNanoOfMillisValue = proto.Int32(nanos)
	}
	return nil
}

// scanFilter is a compiled predicate and the schema it was compiled against.
type scanFilter struct {
	predicate *fmsg.PbPredicate
	schemaID  int32
}

// validateFilterLogFormat rejects tables whose log cannot evaluate batch
// filters. Fluss evaluates predicates against Arrow batch statistics.
func validateFilterLogFormat(table Table) error {
	format := strings.TrimSpace(table.Properties["table.log.format"])
	if format == "" || strings.EqualFold(format, string(LogFormatArrow)) {
		return nil
	}
	return fmt.Errorf(
		"%w: scan filters require table.log.format=ARROW, table %s uses %s",
		ErrInvalidConfig, table.Path, format,
	)
}
