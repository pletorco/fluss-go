package fgo

import (
	"bytes"
	"errors"
	"math/big"
	"testing"
)

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
