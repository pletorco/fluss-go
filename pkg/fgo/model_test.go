package fgo

import (
	"errors"
	"testing"
)

func TestSchemaJSONRoundTrip(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: BigIntType}, {Name: "value", Type: StringType, Nullable: true}}, PrimaryKey: []string{"id"}}
	data, err := schema.JSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseSchemaJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Columns[1].Name != "value" || got.PrimaryKey[0] != "id" {
		t.Fatalf("round trip = %#v", got)
	}
}

func TestValidateRow(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: BigIntType}, {Name: "value", Type: StringType, Nullable: true}}}
	if err := schema.ValidateRow(Row{int64(1), "ok"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateRow(Row{int64(1), nil}, nil); err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateRow(Row{"wrong", "ok"}, nil); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("error = %v", err)
	}
}

func TestTableCapabilities(t *testing.T) {
	table := Table{Path: TablePath{Database: "db", Table: "t"}, Kind: LogTable}
	if err := table.RequirePrimaryKey(); !errors.Is(err, ErrTableKind) {
		t.Fatalf("error = %v", err)
	}
	if err := table.RequireLog(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaValidationFailures(t *testing.T) {
	cases := []Schema{
		{},
		{Columns: []Column{{Name: " ", Type: StringType}}},
		{Columns: []Column{{Name: "id", Type: StringType}, {Name: "id", Type: StringType}}},
		{Columns: []Column{{Name: "id", Type: DataType("UNKNOWN")}}},
		{Columns: []Column{{Name: "id", Type: StringType}}, PrimaryKey: []string{"missing"}},
	}
	for _, schema := range cases {
		if err := schema.Validate(); !errors.Is(err, ErrInvalidSchema) {
			t.Fatalf("Validate(%#v) error = %v", schema, err)
		}
	}
	if _, err := ParseSchemaJSON([]byte("{")); !errors.Is(err, ErrInvalidSchema) {
		t.Fatalf("ParseSchemaJSON malformed error = %v", err)
	}
}

func TestValidateRowAllTypes(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "bool", Type: BoolType}, {Name: "int", Type: IntType}, {Name: "big", Type: BigIntType},
		{Name: "float", Type: FloatType}, {Name: "double", Type: DoubleType}, {Name: "string", Type: StringType},
		{Name: "binary", Type: BinaryType}, {Name: "date", Type: DateType}, {Name: "timestamp", Type: TimestampType},
	}}
	row := Row{true, int32(1), int64(2), float32(3), float64(4), "five", []byte("six"), "date", "timestamp"}
	if err := schema.ValidateRow(row, nil); err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateRow(row[:1], []string{"missing"}); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("unknown column error = %v", err)
	}
	if err := schema.ValidateRow(Row{nil}, []string{"bool"}); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("nil required error = %v", err)
	}
}
