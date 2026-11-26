package fgo

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestFetchSkipsEmptyWALWithoutSkippingNextRow(t *testing.T) {
	schema := appendWriterTable().Schema
	for _, magic := range []byte{0, 1} {
		empty, err := (LogBatch{Magic: magic, BaseOffset: 6, SchemaID: 3}).EncodeRows(schema, true)
		if err != nil {
			t.Fatal(err)
		}
		data := append(encodedRows(t, schema, 5, 1), empty...)
		data = append(data, encodedRows(t, schema, 7, 2)...)
		next, rows, arrows, err := decodeFetchedLog(schema, 0, 5, data)
		if next != 8 || err != nil || len(arrows) != 0 {
			t.Fatalf("magic %d: next=%d, arrows=%v, error=%v", magic, next, arrows, err)
		}
		requireRecordOffsets(t, rows, 5, 7)
		next, rows, _, err = decodeFetchedLog(schema, 0, 9, empty)
		if next != 9 || err != nil || len(rows) != 0 {
			t.Fatalf("stale empty marker moved offset backward: %d / %v / %v", next, rows, err)
		}
	}
}

func TestFetchRejectsMalformedEmptyWALRange(t *testing.T) {
	schema := appendWriterTable().Schema
	for _, tc := range []struct {
		base  int64
		delta uint32
	}{
		{6, 1}, {6, math.MaxUint32}, {-1, 0}, {math.MaxInt64, 0},
	} {
		empty, err := (LogBatch{Magic: 1, BaseOffset: tc.base, SchemaID: 3}).EncodeRows(schema, true)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint32(empty[29+3:], tc.delta)
		rewriteArrowCRC(empty, 25, 29)
		if next, _, _, err := decodeFetchedLog(schema, 0, 5, empty); next != 5 || !errors.Is(err, ErrMalformedRecordBatch) {
			t.Fatalf("invalid empty WAL (%d, %d): next=%d, error=%v", tc.base, tc.delta, next, err)
		}
	}
}
