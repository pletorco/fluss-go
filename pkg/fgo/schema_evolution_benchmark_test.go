package fgo

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
)

func BenchmarkReadBatchSchemaEvolution(b *testing.B) {
	const batchRows = 128
	ctx := context.Background()
	table := Table{
		Path: TablePath{Database: "db", Table: "wide"}, SchemaID: 2,
		Schema: Schema{Columns: make([]Column, 32), HighestFieldID: 32},
	}
	row := make(Row, len(table.Schema.Columns))
	for i := range table.Schema.Columns {
		table.Schema.Columns[i] = Column{
			Name: fmt.Sprintf("col%d", i), Type: IntType, ID: i + 1, Nullable: i == len(row)-1,
		}
		row[i] = int32(i)
	}
	old := table
	old.SchemaID = 1
	old.Schema.Columns = old.Schema.Columns[:len(row)-1]
	resolver := &mapSchemaResolver{schemas: map[int32]Schema{old.SchemaID: old.Schema, table.SchemaID: table.Schema}}
	log := LogBatch{Magic: 0, SchemaID: int16(table.SchemaID), AppendOnly: true}
	for range batchRows {
		log.Records = append(log.Records, Record{Value: row, Change: Append})
	}
	logBytes, err := log.EncodeRows(table.Schema, true)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("log", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, rows, _, err := decodeFetchedLogWithResolver(ctx, resolver, table, 0, 0, logBytes, true)
			if err != nil || len(rows) != batchRows {
				b.Fatalf("decoded log rows=%d err=%v", len(rows), err)
			}
		}
	})
	kv := mixedSchemaValueBatch(b, old, table, row, batchRows)
	b.Run("mixedKV", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, err := decodeValueRecordBatchWithResolver(ctx, resolver, table, kv, compactedEncoding)
			if err != nil || len(rows) != batchRows {
				b.Fatalf("decoded KV rows=%d err=%v", len(rows), err)
			}
		}
	})
}

func mixedSchemaValueBatch(t testing.TB, old, current Table, row Row, count int) []byte {
	t.Helper()
	oldRecord := encodedValueBatch(t, old, row[:len(old.Schema.Columns)])[9:]
	currentRecord := encodedValueBatch(t, current, row)[9:]
	encoded := make([]byte, 9)
	for i := range count {
		if i%2 == 0 {
			encoded = append(encoded, oldRecord...)
		} else {
			encoded = append(encoded, currentRecord...)
		}
	}
	binary.LittleEndian.PutUint32(encoded, uint32(len(encoded)-4))
	binary.LittleEndian.PutUint32(encoded[5:], uint32(count))
	return encoded
}
