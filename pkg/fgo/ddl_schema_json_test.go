package fgo

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestLogicalTypeJSONWritesRequiredZeroParameters(t *testing.T) {
	for _, tc := range []struct {
		logical LogicalType
		want    string
	}{
		{LogicalType{Root: "DECIMAL", Precision: 20}, `{"type":"DECIMAL","nullable":false,"precision":20,"scale":0}`},
		{LogicalType{Root: "TIME_WITHOUT_TIME_ZONE"}, `{"type":"TIME_WITHOUT_TIME_ZONE","nullable":false,"precision":0}`},
		{LogicalType{Root: "TIMESTAMP_WITHOUT_TIME_ZONE"}, `{"type":"TIMESTAMP_WITHOUT_TIME_ZONE","nullable":false,"precision":0}`},
		{LogicalType{Root: "TIMESTAMP_WITH_LOCAL_TIME_ZONE"}, `{"type":"TIMESTAMP_WITH_LOCAL_TIME_ZONE","nullable":false,"precision":0}`},
		{LogicalType{Root: "INTEGER"}, `{"type":"INTEGER","nullable":false}`},
	} {
		t.Run(tc.logical.Root, func(t *testing.T) {
			got, err := tc.logical.JSON()
			if err != nil || string(got) != tc.want {
				t.Fatalf("JSON = %s, %v; want %s", got, err, tc.want)
			}
			nested := LogicalType{Root: "ARRAY", Element: &tc.logical}
			got, err = nested.JSON()
			if err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				Element json.RawMessage `json:"element_type"`
			}
			if err := json.Unmarshal(got, &decoded); err != nil || string(decoded.Element) != tc.want {
				t.Fatalf("nested JSON = %s; %v", got, err)
			}
			read, err := ParseLogicalTypeJSON(decoded.Element)
			if err != nil || read.Precision != tc.logical.Precision || read.Scale != tc.logical.Scale {
				t.Fatalf("round trip = %+v, %v", read, err)
			}
		})
	}
}

func TestSchemaJSONAssignsIDsWithoutChangingNestedInputs(t *testing.T) {
	details := LogicalType{Root: "MAP", Key: &LogicalType{Root: "STRING"}, Value: &LogicalType{
		Root: "ARRAY", Element: &LogicalType{Root: "ROW", Fields: []LogicalField{
			{Name: "amount", Type: LogicalType{Root: "DECIMAL", Precision: 20}},
		}},
	}}
	schema := Schema{Columns: []Column{{Name: "id", Type: IntType}, logicalColumn("details", details)}}
	want, err := schema.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if got := details.Value.Element.Fields[0].ID; got != 0 {
		t.Fatalf("serializing assigned ID %d in caller metadata", got)
	}
	parsed, err := ParseSchemaJSON(want)
	if err != nil {
		t.Fatal(err)
	}
	field := parsed.Columns[1].LogicalType.Value.Element.Fields[0]
	if field.ID != 2 || parsed.Columns[1].ID != 1 || parsed.HighestFieldID != field.ID {
		t.Fatalf("assigned field IDs = %+v; nested ID=%d", parsed, field.ID)
	}
	for range 12 {
		t.Run("serialize shared schema", func(t *testing.T) {
			t.Parallel()
			got, err := schema.JSON()
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("repeat JSON = %s, %v; want %s", got, err, want)
			}
		})
	}
}
