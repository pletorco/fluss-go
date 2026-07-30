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
