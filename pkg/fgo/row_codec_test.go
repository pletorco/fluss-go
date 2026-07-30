package fgo

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/big"
	"reflect"
	"testing"
	"time"
)

type rowFixture struct {
	name   string
	hex    string
	encode func(Schema, Row) ([]byte, error)
	decode func(Schema, []byte) (Row, error)
}

func TestCompactedRowRoundTrip(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "flag", Type: BoolType},
		{Name: "small", Type: TinyIntType},
		{Name: "number", Type: IntType},
		{Name: "id", Type: BigIntType},
		{Name: "name", Type: StringType, Nullable: true},
		{Name: "payload", Type: BytesType},
	}}
	want := Row{true, int8(-2), int32(-100), int64(300), nil, []byte("payload")}
	encoded, err := EncodeCompactedRow(schema, want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCompactedRow(schema, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] || got[4] != nil || !bytes.Equal(got[5].([]byte), want[5].([]byte)) {
		t.Fatalf("decoded row = %#v", got)
	}
}

func TestCompactedPrimaryKeyMatchesJavaFixture(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "one", Type: IntType}, {Name: "two", Type: BigIntType}, {Name: "three", Type: IntType}}, PrimaryKey: []string{"one", "two", "three"}}
	key, err := EncodePrimaryKey(schema, PrimaryKey{int32(1), int64(3), int32(2)})
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{1, 3, 2}; !bytes.Equal(key, want) {
		t.Fatalf("key = %x, want %x", key, want)
	}
}

func TestRowsMatchJava091Fixtures(t *testing.T) {
	schema := java091FixtureSchema()
	want := Row{int32(42), "fluss", []any{int32(1), nil, int32(-2)}, Row{int32(7), "go"}}
	fixtures := []rowFixture{
		{
			name:   "compacted",
			hex:    "002a05666c7573731803000000020000000100000000000000feffffff0000000005000702676f",
			encode: EncodeCompactedRow,
			decode: DecodeCompactedRow,
		},
		{
			name:   "indexed",
			hex:    "0005000000180000000b0000002a000000666c75737303000000020000000100000000000000feffffff00000000000200000007000000676f",
			encode: EncodeIndexedRow,
			decode: DecodeIndexedRow,
		},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			assertRowFixture(t, schema, want, fixture)
		})
	}
}

func assertRowFixture(t *testing.T, schema Schema, want Row, fixture rowFixture) {
	t.Helper()
	javaBytes, err := hex.DecodeString(fixture.hex)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fixture.decode(schema, javaBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded Java fixture = %#v", got)
	}
	goBytes, err := fixture.encode(schema, want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(goBytes, javaBytes) {
		t.Fatalf("Go fixture = %x, Java fixture = %x", goBytes, javaBytes)
	}
}

func java091FixtureSchema() Schema {
	integer := LogicalType{Root: "INTEGER", Nullable: true}
	stringType := LogicalType{Root: "STRING", Nullable: true}
	arrayType := LogicalType{Root: "ARRAY", Nullable: true, Element: &integer}
	nestedType := LogicalType{Root: "ROW", Nullable: true, Fields: []LogicalField{
		{Name: "number", Type: integer},
		{Name: "label", Type: stringType},
	}}
	return Schema{Columns: []Column{
		logicalColumn("number", integer),
		logicalColumn("name", stringType),
		logicalColumn("values", arrayType),
		logicalColumn("nested", nestedType),
	}}
}

func TestCompactedRowRejectsMalformedInput(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: IntType}, {Name: "name", Type: StringType}}}
	for _, encoded := range [][]byte{
		nil,
		{0, 0x80},
		{0, 1, 0x80, 0x80, 0x80, 0x80, 0x80},
		{1, 1},
		{0, 1, 1, 'x', 'y'},
	} {
		if _, err := DecodeCompactedRow(schema, encoded); !errors.Is(err, ErrMalformedRow) {
			t.Fatalf("DecodeCompactedRow(%x) error = %v", encoded, err)
		}
	}
	if _, err := EncodeCompactedRow(schema, Row{int32(1), nil}); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("EncodeCompactedRow null error = %v", err)
	}
}

func TestIndexedRowRoundTrip(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: IntType}, {Name: "name", Type: StringType}, {Name: "offset", Type: BigIntType}, {Name: "payload", Type: BytesType, Nullable: true}}}
	want := Row{int32(7), "go", int64(-4), nil}
	encoded, err := EncodeIndexedRow(schema, want)
	if err != nil {
		t.Fatal(err)
	}
	// one null byte, two variable-width length slots, then little-endian field values
	if wantPrefix := []byte{0x08, 0x02, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 'g', 'o'}; !bytes.Equal(encoded[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("indexed prefix = %x, want %x", encoded, wantPrefix)
	}
	got, err := DecodeIndexedRow(schema, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != nil {
		t.Fatalf("decoded row = %#v", got)
	}
	if _, err := DecodeIndexedRow(schema, encoded[:len(encoded)-1]); !errors.Is(err, ErrMalformedRow) {
		t.Fatalf("truncated indexed row error = %v", err)
	}
}

func TestRowCodecsCoverPrimitiveWidths(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "bool", Type: BoolType}, {Name: "small", Type: SmallIntType}, {Name: "float", Type: FloatType},
		{Name: "double", Type: DoubleType}, {Name: "char", Type: CharType}, {Name: "bytes", Type: BytesType},
	}}
	want := Row{false, int16(-9), float32(1.5), float64(-2.5), "x", []byte{1, 2}}
	for _, codec := range []struct {
		encode func(Schema, Row) ([]byte, error)
		decode func(Schema, []byte) (Row, error)
	}{{EncodeCompactedRow, DecodeCompactedRow}, {EncodeIndexedRow, DecodeIndexedRow}} {
		encoded, err := codec.encode(schema, want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := codec.decode(schema, encoded)
		if err != nil || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] || got[4] != want[4] || !bytes.Equal(got[5].([]byte), want[5].([]byte)) {
			t.Fatalf("codec result = %#v, %v", got, err)
		}
	}
	unsupported := Schema{Columns: []Column{{Name: "decimal", Type: DecimalType}}}
	if _, err := EncodeCompactedRow(unsupported, Row{big.NewRat(1, 2)}); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("unsupported type error = %v", err)
	}
}

func TestLogicalScalarRowRoundTrip(t *testing.T) {
	at := time.Date(2026, 7, 30, 12, 34, 56, 123456789, time.FixedZone("KST", 9*60*60))
	schema := Schema{Columns: []Column{
		logicalColumn("date", LogicalType{Root: "DATE"}),
		logicalColumn("time", LogicalType{Root: "TIME_WITHOUT_TIME_ZONE", Precision: 3}),
		logicalColumn("timestamp", LogicalType{Root: "TIMESTAMP_WITHOUT_TIME_ZONE", Precision: 9}),
		logicalColumn("timestamp_ltz", LogicalType{Root: "TIMESTAMP_WITH_LOCAL_TIME_ZONE", Precision: 9}),
		logicalColumn("decimal", LogicalType{Root: "DECIMAL", Precision: 20, Scale: 3}),
		logicalColumn("char", LogicalType{Root: "CHAR", Length: 4}),
		logicalColumn("binary", LogicalType{Root: "BINARY", Length: 4}),
	}}
	want := Row{
		at, at, at, at, big.NewRat(-12345, 1000), "go", []byte{1, 2},
	}
	for _, codec := range rowCodecs() {
		encoded, err := codec.encode(schema, want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := codec.decode(schema, encoded)
		if err != nil {
			t.Fatal(err)
		}
		if got[0].(time.Time).Format("2006-01-02") != "2026-07-30" ||
			got[1].(time.Time).Format("15:04:05.000") != "12:34:56.123" ||
			!got[2].(time.Time).Equal(time.Date(2026, 7, 30, 12, 34, 56, 123456789, time.UTC)) ||
			!got[3].(time.Time).Equal(at.UTC()) ||
			got[4].(*big.Rat).Cmp(want[4].(*big.Rat)) != 0 ||
			got[5] != "go" || !bytes.Equal(got[6].([]byte), []byte{1, 2}) {
			t.Fatalf("decoded logical row = %#v", got)
		}
	}
}

func TestNestedRowArrayAndMapRoundTrip(t *testing.T) {
	integer := LogicalType{Root: "INTEGER"}
	nullableString := LogicalType{Root: "STRING", Nullable: true}
	nested := LogicalType{Root: "ROW", Fields: []LogicalField{
		{Name: "id", Type: integer},
		{Name: "label", Type: nullableString},
	}}
	array := LogicalType{Root: "ARRAY", Element: &nested}
	key := LogicalType{Root: "STRING"}
	value := LogicalType{Root: "ARRAY", Element: &nullableString}
	mapped := LogicalType{Root: "MAP", Key: &key, Value: &value}
	schema := Schema{Columns: []Column{
		logicalColumn("items", array),
		logicalColumn("labels", mapped),
		logicalColumn("projected", nested),
	}}
	want := Row{
		[]any{Row{int32(1), "one"}, Row{int32(2), nil}},
		Map{{Key: "a", Value: []any{"x", nil}}, {Key: "b", Value: []any{}}},
		Row{int32(3), "three"},
	}
	for _, codec := range rowCodecs() {
		encoded, err := codec.encode(schema, want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := codec.decode(schema, encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("decoded nested row = %#v, want %#v", got, want)
		}
	}
}

func TestBinaryArrayElementTypesRoundTrip(t *testing.T) {
	at := time.Date(2026, 7, 30, 1, 2, 3, 456789123, time.UTC)
	tests := []struct {
		name    string
		element LogicalType
		values  []any
	}{
		{"boolean", LogicalType{Root: "BOOLEAN", Nullable: true}, []any{true, nil, false}},
		{"tinyint", LogicalType{Root: "TINYINT"}, []any{int8(-1), int8(2)}},
		{"smallint", LogicalType{Root: "SMALLINT"}, []any{int16(-2), int16(3)}},
		{"integer", LogicalType{Root: "INTEGER"}, []any{int32(-3), int32(4)}},
		{"bigint", LogicalType{Root: "BIGINT"}, []any{int64(-4), int64(5)}},
		{"float", LogicalType{Root: "FLOAT"}, []any{float32(1.25), float32(-2.5)}},
		{"double", LogicalType{Root: "DOUBLE"}, []any{float64(3.25), float64(-4.5)}},
		{"date", LogicalType{Root: "DATE"}, []any{time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)}},
		{"time", LogicalType{Root: "TIME_WITHOUT_TIME_ZONE", Precision: 3}, []any{time.Date(1970, 1, 1, 1, 2, 3, 456000000, time.UTC)}},
		{"timestamp", LogicalType{Root: "TIMESTAMP_WITHOUT_TIME_ZONE", Precision: 9}, []any{at}},
		{"timestamp_ltz", LogicalType{Root: "TIMESTAMP_WITH_LOCAL_TIME_ZONE", Precision: 9}, []any{at}},
		{"decimal_compact", LogicalType{Root: "DECIMAL", Precision: 8, Scale: 2}, []any{big.NewRat(-123, 100)}},
		{"decimal_wide", LogicalType{Root: "DECIMAL", Precision: 20, Scale: 2}, []any{big.NewRat(1234567890123456789, 100)}},
		{"short_string", LogicalType{Root: "STRING"}, []any{"go"}},
		{"long_string", LogicalType{Root: "STRING"}, []any{"longer than seven bytes"}},
		{"char", LogicalType{Root: "CHAR", Length: 8}, []any{"char"}},
		{"short_bytes", LogicalType{Root: "BYTES"}, []any{[]byte{1, 2}}},
		{"long_bytes", LogicalType{Root: "BYTES"}, []any{[]byte("longer than seven bytes")}},
		{"binary", LogicalType{Root: "BINARY", Length: 8}, []any{[]byte{1, 2, 3}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			arrayType := LogicalType{Root: "ARRAY", Element: &test.element}
			schema := Schema{Columns: []Column{logicalColumn("value", arrayType)}}
			for _, codec := range rowCodecs() {
				encoded, err := codec.encode(schema, Row{test.values})
				if err != nil {
					t.Fatal(err)
				}
				got, err := codec.decode(schema, encoded)
				if err != nil {
					t.Fatal(err)
				}
				assertArrayValues(t, got[0].([]any), test.values)
			}
		})
	}
}

func TestBinaryArrayRejectsMalformedData(t *testing.T) {
	integer := LogicalType{Root: "INTEGER"}
	for _, encoded := range [][]byte{
		nil,
		{1, 0, 0, 0},
		{0xff, 0xff, 0xff, 0x7f},
	} {
		if _, err := decodeBinaryArray(integer, encoded, compactedEncoding); err == nil {
			t.Fatalf("decodeBinaryArray(%x) succeeded", encoded)
		}
	}
	stringType := LogicalType{Root: "STRING"}
	invalidOffset := make([]byte, 16)
	binary.LittleEndian.PutUint32(invalidOffset, 1)
	binary.LittleEndian.PutUint64(invalidOffset[8:], uint64(20)<<32|1)
	if _, err := decodeBinaryArray(stringType, invalidOffset, indexedEncoding); err == nil {
		t.Fatal("expected invalid variable offset error")
	}
	invalidPacked := make([]byte, 16)
	binary.LittleEndian.PutUint32(invalidPacked, 1)
	binary.LittleEndian.PutUint64(invalidPacked[8:], uint64(0xff)<<56)
	if _, err := decodeBinaryArray(stringType, invalidPacked, indexedEncoding); err == nil {
		t.Fatal("expected invalid packed length error")
	}
	mapType := LogicalType{Root: "MAP", Key: &stringType, Value: &integer}
	if _, err := decodeBinaryMap(mapType, []byte{9, 0, 0, 0}, indexedEncoding); err == nil {
		t.Fatal("expected invalid map key length error")
	}
}

func TestPrefixKeyAndIndexedNullLengthSlots(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "region", Type: StringType},
		{Name: "tenant", Type: IntType},
		{Name: "suffix", Type: StringType, Nullable: true},
	}, PrimaryKey: []string{"region", "tenant"}}
	prefix, err := EncodePrefixKey(schema, PrimaryKey{"ap"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(prefix, []byte{2, 'a', 'p'}) {
		t.Fatalf("prefix = %x", prefix)
	}
	for _, version := range []int16{0, 1} {
		lookup, err := EncodeLookupKey(schema, PrimaryKey{"ap", int32(1)}, version)
		if err != nil || !bytes.Equal(lookup, []byte{2, 'a', 'p', 1}) {
			t.Fatalf("Lookup v%d key = %x, %v", version, lookup, err)
		}
		gotPrefix, err := EncodePrefixLookupKey(schema, PrimaryKey{"ap"}, version)
		if err != nil || !bytes.Equal(gotPrefix, prefix) {
			t.Fatalf("PrefixLookup v%d key = %x, %v", version, gotPrefix, err)
		}
	}
	if _, err := EncodeLookupKey(schema, PrimaryKey{"ap", int32(1)}, 2); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported Lookup version error = %v", err)
	}
	if _, err := EncodePrefixLookupKey(schema, PrimaryKey{"ap"}, -1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported PrefixLookup version error = %v", err)
	}
	if _, err := EncodePrefixKey(schema, nil); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("empty prefix error = %v", err)
	}
	row := Row{"ap", int32(1), nil}
	encoded, err := EncodeIndexedRow(schema, row)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeIndexedRow(schema, encoded)
	if err != nil || !reflect.DeepEqual(got, row) {
		t.Fatalf("indexed nullable variables = %#v, %v", got, err)
	}
}

func TestProjectedRowsPreserveOmittedColumns(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "id", Type: IntType},
		{Name: "name", Type: StringType},
		{Name: "score", Type: BigIntType},
	}}
	columns := []string{"score", "name"}
	want := Row{int64(9), "updated"}
	for _, codec := range []struct {
		encode func(Schema, []string, Row) ([]byte, error)
		decode func(Schema, []string, []byte) (Row, error)
	}{
		{EncodeCompactedProjectedRow, DecodeCompactedProjectedRow},
		{EncodeIndexedProjectedRow, DecodeIndexedProjectedRow},
	} {
		encoded, err := codec.encode(schema, columns, want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := codec.decode(schema, columns, encoded)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("projected row = %#v, %v", got, err)
		}
	}
	for _, columns := range [][]string{nil, {"missing"}, {"name", "name"}} {
		if _, err := EncodeCompactedProjectedRow(schema, columns, nil); !errors.Is(err, ErrInvalidSchema) {
			t.Fatalf("projection %v error = %v", columns, err)
		}
	}
}

func TestRowCodecsRejectInvalidLogicalValues(t *testing.T) {
	decimal := logicalColumn("decimal", LogicalType{Root: "DECIMAL", Precision: 3, Scale: 2})
	if _, err := EncodeCompactedRow(Schema{Columns: []Column{decimal}}, Row{big.NewRat(1, 3)}); err == nil {
		t.Fatal("expected inexact decimal error")
	}
	binary := logicalColumn("binary", LogicalType{Root: "BINARY", Length: 2})
	if _, err := EncodeIndexedRow(Schema{Columns: []Column{binary}}, Row{[]byte{1, 2, 3}}); err == nil {
		t.Fatal("expected fixed binary overflow")
	}
	arrayType := LogicalType{Root: "ARRAY", Element: &LogicalType{Root: "INTEGER"}}
	array := logicalColumn("array", arrayType)
	if _, err := EncodeCompactedRow(Schema{Columns: []Column{array}}, Row{[]any{nil}}); err == nil {
		t.Fatal("expected non-nullable array element error")
	}
}

func TestRowValueHelpersRejectTruncatedAndInvalidValues(t *testing.T) {
	for _, kind := range []DataType{TinyIntType, SmallIntType, FloatType, DoubleType} {
		if _, _, err := readCompactedFixed(nil, 0, kind); err == nil {
			t.Fatalf("readCompactedFixed(%s) error = nil", kind)
		}
	}
	if _, _, err := readCompactedFixed([]byte{2}, 0, BoolType); err == nil {
		t.Fatal("invalid compacted boolean error = nil")
	}
	if _, _, err := readCompactedFixed(nil, 0, StringType); err == nil {
		t.Fatal("unsupported compacted fixed type error = nil")
	}

	timestamp := appendVar64(nil, 0)
	timestamp = appendVar32(timestamp, 1_000_000)
	if _, _, err := readCompactedTimestamp(timestamp, 0, LogicalType{Root: "TIMESTAMP", Precision: 6}); err == nil {
		t.Fatal("invalid compacted timestamp nanos error = nil")
	}
	if _, _, err := readCompactedTimestamp([]byte{0x80}, 0, LogicalType{Root: "TIMESTAMP"}); err == nil {
		t.Fatal("truncated compacted timestamp error = nil")
	}
	if _, _, err := readCompactedDecimal([]byte{0x80}, 0, LogicalType{Root: "DECIMAL", Precision: 18, Scale: 2}); err == nil {
		t.Fatal("truncated small decimal error = nil")
	}
	if _, _, err := readCompactedDecimal([]byte{0x80}, 0, LogicalType{Root: "DECIMAL", Precision: 19, Scale: 2}); err == nil {
		t.Fatal("truncated large decimal error = nil")
	}
}

func TestIndexedLengthsAndSignedIntegersCoverBoundaries(t *testing.T) {
	tests := []struct {
		logical LogicalType
		want    int
	}{
		{LogicalType{Root: "BOOLEAN"}, 1},
		{LogicalType{Root: "SMALLINT"}, 2},
		{LogicalType{Root: "INTEGER"}, 4},
		{LogicalType{Root: "BIGINT"}, 8},
		{LogicalType{Root: "TIMESTAMP", Precision: 6}, 12},
		{LogicalType{Root: "TIMESTAMP", Precision: 3}, 8},
		{LogicalType{Root: "CHAR", Length: 7}, 7},
		{LogicalType{Root: "STRING"}, -1},
	}
	for _, test := range tests {
		if got := indexedLength(test.logical); got != test.want {
			t.Fatalf("indexedLength(%#v) = %d, want %d", test.logical, got, test.want)
		}
	}
	for _, value := range []*big.Int{
		big.NewInt(0), big.NewInt(127), big.NewInt(128),
		big.NewInt(-1), big.NewInt(-128), big.NewInt(-129),
	} {
		encoded := signedBigEndian(value)
		if got := signedBigInt(encoded); got.Cmp(value) != 0 {
			t.Fatalf("signed round trip %s = %x -> %s", value, encoded, got)
		}
	}
	negative := timestampTime(TimestampType, -1, 0)
	if negative.UnixNano() != -int64(time.Millisecond) {
		t.Fatalf("negative timestamp = %v", negative)
	}
}

func TestBinaryArrayReadersRejectInvalidNestedAndTimestampSlots(t *testing.T) {
	truncated := make([]byte, 8)
	putOffsetSize(truncated, 0, 100, 1)
	if _, err := readArrayNested(
		truncated, 0, LogicalType{Root: "ARRAY", Element: &LogicalType{Root: "INTEGER"}},
		compactedEncoding,
	); err == nil {
		t.Fatal("truncated nested array error = nil")
	}

	unsupported := make([]byte, 8)
	var err error
	unsupported, err = appendArrayVariable(unsupported, 0, []byte{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readArrayNested(
		unsupported, 0, LogicalType{Root: "STRING"}, compactedEncoding,
	); err == nil {
		t.Fatal("unsupported nested array type error = nil")
	}

	timestamp := make([]byte, 16)
	putOffsetSize(timestamp, 0, 8, 1_000_000)
	if _, err := readArrayTimestamp(
		timestamp, 0, LogicalType{Root: "TIMESTAMP", Precision: 6},
	); err == nil {
		t.Fatal("invalid array timestamp nanos error = nil")
	}
	putOffsetSize(timestamp, 0, 100, 1)
	if _, err := readArrayTimestamp(
		timestamp, 0, LogicalType{Root: "TIMESTAMP", Precision: 6},
	); err == nil {
		t.Fatal("truncated array timestamp error = nil")
	}
}

func logicalColumn(name string, logicalType LogicalType) Column {
	return Column{Name: name, Type: dataTypeForLogicalType(logicalType), Nullable: logicalType.Nullable, LogicalType: &logicalType}
}

func rowCodecs() []struct {
	encode func(Schema, Row) ([]byte, error)
	decode func(Schema, []byte) (Row, error)
} {
	return []struct {
		encode func(Schema, Row) ([]byte, error)
		decode func(Schema, []byte) (Row, error)
	}{{EncodeCompactedRow, DecodeCompactedRow}, {EncodeIndexedRow, DecodeIndexedRow}}
}

func assertArrayValues(t *testing.T, got, want []any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("array length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !arrayValueEqual(got[i], want[i]) {
			t.Fatalf("array[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func arrayValueEqual(got, want any) bool {
	switch want := want.(type) {
	case []byte:
		value, ok := got.([]byte)
		return ok && bytes.Equal(value, want)
	case *big.Rat:
		value, ok := got.(*big.Rat)
		return ok && value.Cmp(want) == 0
	case time.Time:
		value, ok := got.(time.Time)
		return ok && value.Equal(want)
	default:
		return reflect.DeepEqual(got, want)
	}
}

func FuzzDecodeCompactedRow(f *testing.F) {
	schema := Schema{Columns: []Column{{Name: "id", Type: IntType}, {Name: "payload", Type: BytesType, Nullable: true}}}
	f.Add([]byte{0, 1, 3, 'a', 'b', 'c'})
	f.Add([]byte{2, 1})
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > maxRowBytes+1 {
			t.Skip()
		}
		_, _ = DecodeCompactedRow(schema, encoded)
	})
}

func FuzzDecodeIndexedRow(f *testing.F) {
	schema := Schema{Columns: []Column{{Name: "id", Type: IntType}, {Name: "payload", Type: BytesType, Nullable: true}}}
	f.Add([]byte{0, 3, 0, 0, 0, 1, 0, 0, 0, 'a', 'b', 'c'})
	f.Add([]byte{2, 1, 0, 0, 0})
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > maxRowBytes+1 {
			t.Skip()
		}
		_, _ = DecodeIndexedRow(schema, encoded)
	})
}

func FuzzLookupKeyEncoding(f *testing.F) {
	schema := Schema{
		Columns: []Column{
			{Name: "name", Type: StringType},
			{Name: "sequence", Type: BigIntType},
		},
		PrimaryKey: []string{"name", "sequence"},
	}
	f.Add("alice", int64(1), true)
	f.Add("", int64(-1), false)
	f.Fuzz(func(t *testing.T, name string, sequence int64, prefixOnly bool) {
		if len(name) > maxRowBytes {
			t.Skip()
		}
		key := PrimaryKey{name, sequence}
		if prefixOnly {
			key = key[:1]
		}
		for _, version := range []int16{0, 1} {
			var first, second []byte
			var firstErr, secondErr error
			if prefixOnly {
				first, firstErr = EncodePrefixLookupKey(schema, key, version)
				second, secondErr = EncodePrefixLookupKey(schema, key, version)
			} else {
				first, firstErr = EncodeLookupKey(schema, key, version)
				second, secondErr = EncodeLookupKey(schema, key, version)
			}
			if !errors.Is(firstErr, secondErr) || !bytes.Equal(first, second) {
				t.Fatalf("key encoding is not deterministic: %x/%v != %x/%v", first, firstErr, second, secondErr)
			}
		}
	})
}
