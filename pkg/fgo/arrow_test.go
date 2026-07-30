package fgo

import "testing"

func TestArrowSchemaRoundTrip(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "id", Type: BigIntType},
		{Name: "value", Type: StringType, Nullable: true},
		{Name: "created_at", Type: TimestampType},
	}}
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	got, err := SchemaFromArrow(arrowSchema)
	if err != nil {
		t.Fatal(err)
	}
	if got.Columns[1].Type != StringType || !got.Columns[1].Nullable {
		t.Fatalf("round trip = %#v", got)
	}
}

func TestArrowTypeRoundTrip(t *testing.T) {
	for _, kind := range []DataType{BoolType, IntType, BigIntType, FloatType, DoubleType, StringType, BinaryType, DateType, TimestampType} {
		arrowType, err := arrowType(kind)
		if err != nil {
			t.Fatalf("arrowType(%s) error = %v", kind, err)
		}
		got, err := flussType(arrowType)
		if err != nil {
			t.Fatalf("flussType(%s) error = %v", kind, err)
		}
		if got != kind {
			t.Fatalf("round trip = %s, want %s", got, kind)
		}
	}
}

func TestArrowSchemaRejectsInvalidSchema(t *testing.T) {
	_, err := (Schema{}).ArrowSchema()
	if err == nil {
		t.Fatal("ArrowSchema() error = nil, want invalid schema")
	}
	if _, err := SchemaFromArrow(nil); err == nil {
		t.Fatal("SchemaFromArrow(nil) error = nil")
	}
}
