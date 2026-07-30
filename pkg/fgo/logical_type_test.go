package fgo

import (
	"bytes"
	"errors"
	"testing"
)

func TestLogicalTypeValidation(t *testing.T) {
	for _, root := range []string{"BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DATE", "STRING", "BYTES"} {
		if err := (LogicalType{Root: root}).Validate(); err != nil {
			t.Fatalf("%s: %v", root, err)
		}
	}
	nested := LogicalType{Root: "ROW", Fields: []LogicalField{{Name: "items", ID: 1, Type: LogicalType{Root: "ARRAY", Element: &LogicalType{Root: "MAP", Key: &LogicalType{Root: "STRING"}, Value: &LogicalType{Root: "DECIMAL", Precision: 10, Scale: 2}}}}}}
	if err := nested.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLogicalTypeRejectsInvalidParameters(t *testing.T) {
	for _, typ := range []LogicalType{{Root: "CHAR"}, {Root: "BINARY"}, {Root: "DECIMAL", Precision: 2, Scale: 3}, {Root: "TIME_WITHOUT_TIME_ZONE", Precision: 10}, {Root: "ARRAY"}, {Root: "MAP", Key: &LogicalType{Root: "STRING"}}, {Root: "ROW"}, {Root: "UNKNOWN"}} {
		if err := typ.Validate(); !errors.Is(err, ErrInvalidSchema) {
			t.Fatalf("%#v: %v", typ, err)
		}
	}
}

func TestLogicalTypeJavaJSONRoundTrip(t *testing.T) {
	fixture := []byte(`{"type":"ROW","fields":[{"name":"id","field_type":{"type":"BIGINT"},"field_id":1},{"name":"amount","field_type":{"type":"DECIMAL","precision":10,"scale":2},"field_id":2}]}`)
	logicalType, err := ParseLogicalTypeJSON(fixture)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := logicalType.JSON()
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := ParseLogicalTypeJSON(encoded)
	if err != nil || len(roundTrip.Fields) != 2 || roundTrip.Fields[1].Type.Scale != 2 ||
		!roundTrip.Nullable || !roundTrip.Fields[0].Type.Nullable {
		t.Fatalf("round trip = %#v, %v", roundTrip, err)
	}
	if bytes.Contains(encoded, []byte(`"nullable":true`)) {
		t.Fatalf("nullable default should be omitted: %s", encoded)
	}

	nonNull, err := (LogicalType{Root: "BIGINT"}).JSON()
	if err != nil || !bytes.Contains(nonNull, []byte(`"nullable":false`)) {
		t.Fatalf("non-null JSON = %s, %v", nonNull, err)
	}
}
