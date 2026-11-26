package fgo

import (
	"errors"
	"testing"
)

func TestSchemaRejectsConflictingNullabilityEverywhere(t *testing.T) {
	for _, declared := range []bool{false, true} {
		schema := Schema{Columns: []Column{{
			Name: "label", Type: StringType, Nullable: declared,
			LogicalType: &LogicalType{Root: "STRING", Nullable: !declared},
		}}}
		if err := schema.Validate(); !errors.Is(err, ErrInvalidSchema) {
			t.Errorf("nullable=%t: Validate() = %v", declared, err)
		}
		if _, err := schema.JSON(); !errors.Is(err, ErrInvalidSchema) {
			t.Errorf("nullable=%t: JSON() = %v", declared, err)
		}
		if _, err := schema.ArrowSchema(); !errors.Is(err, ErrInvalidSchema) {
			t.Errorf("nullable=%t: ArrowSchema() = %v", declared, err)
		}
		for _, codec := range rowCodecs() {
			if _, err := codec.encode(schema, Row{"valid string"}); !errors.Is(err, ErrInvalidSchema) {
				t.Errorf("nullable=%t: encode() = %v", declared, err)
			}
			if _, err := codec.decode(schema, nil); !errors.Is(err, ErrInvalidSchema) {
				t.Errorf("nullable=%t: decode() = %v", declared, err)
			}
		}
	}
}

func TestCoherentSchemaNullabilityRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name     string
		nullable bool
		explicit bool
	}{
		{"simple nullable", true, false}, {"simple required", false, false},
		{"typed nullable", true, true}, {"typed required", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			column := Column{Name: "label", Type: StringType, Nullable: tc.nullable}
			if tc.explicit {
				column.LogicalType = &LogicalType{Root: "STRING", Nullable: tc.nullable}
			}
			schema := Schema{Columns: []Column{column}}
			wire, err := schema.JSON()
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := ParseSchemaJSON(wire)
			if err != nil {
				t.Fatal(err)
			}
			for _, candidate := range []Schema{schema, parsed} {
				arrow, err := candidate.ArrowSchema()
				if err != nil || arrow.Field(0).Nullable != tc.nullable {
					t.Fatalf("Arrow schema = %v, %v; want nullable=%t", arrow, err, tc.nullable)
				}
				for _, codec := range rowCodecs() {
					encoded, err := codec.encode(candidate, Row{nil})
					if tc.nullable {
						if err != nil {
							t.Fatal(err)
						}
						row, err := codec.decode(parsed, encoded)
						if err != nil || row[0] != nil {
							t.Fatalf("decoded null = %v, %v", row, err)
						}
					} else if !errors.Is(err, ErrInvalidRow) {
						t.Fatalf("encode null in required column = %v", err)
					}
				}
			}
		})
	}
}

func TestNestedNullabilityIsIndependentOfParent(t *testing.T) {
	typ := LogicalType{Root: "ROW", Fields: []LogicalField{{Name: "note", Type: LogicalType{Root: "STRING", Nullable: true}}}}
	schema := Schema{Columns: []Column{logicalColumn("parent", typ)}}
	for _, codec := range rowCodecs() {
		encoded, err := codec.encode(schema, Row{Row{nil}})
		if err != nil {
			t.Fatal(err)
		}
		row, err := codec.decode(schema, encoded)
		if err != nil || row[0].(Row)[0] != nil {
			t.Fatalf("nested null = %v, %v", row, err)
		}
		if _, err := codec.encode(schema, Row{nil}); !errors.Is(err, ErrInvalidRow) {
			t.Fatalf("required parent accepted nil: %v", err)
		}
	}
}
