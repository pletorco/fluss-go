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
}
