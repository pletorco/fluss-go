package fgo

import (
	"errors"
	"reflect"
	"testing"
)

func TestKVValueEncodingFromTableProperties(t *testing.T) {
	for _, tc := range []struct {
		property string
		want     rowEncoding
		invalid  bool
	}{
		{"", compactedEncoding, false}, {"COMPACTED", compactedEncoding, false},
		{" InDeXeD ", indexedEncoding, false}, {"ARROW", compactedEncoding, true},
	} {
		table := upsertWriterTable()
		table.Properties = map[string]string{"table.kv.format": tc.property, "table.log.format": "INDEXED"}
		got, err := kvValueEncoding(table)
		if got != tc.want || errors.Is(err, ErrInvalidConfig) != tc.invalid {
			t.Fatalf("KV encoding for %q = %v, %v", tc.property, got, err)
		}
	}
}

func TestIndexedKVValueDecoders(t *testing.T) {
	table := upsertWriterTable()
	table.Properties = map[string]string{"table.kv.format": "INDEXED"}
	want := Row{int32(7), "indexed", int64(4)}
	value, err := EncodeIndexedRow(table.Schema, want)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := decodeValueRecordBatch(table, encodedSchemaValueBatch([]schemaValue{{id: int16(table.SchemaID), row: value}}))
	if err != nil || !reflect.DeepEqual(rows, []Row{want}) {
		t.Fatalf("decode batch = %v, %v", rows, err)
	}
	header := appendLittle(nil, uint64(table.SchemaID), 2)
	got, err := decodeLookupValue(table, append(header, value...))
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("decode lookup value = %v, %v", got, err)
	}
}
