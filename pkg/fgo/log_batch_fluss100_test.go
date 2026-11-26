package fgo

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestArrowLogBatchDecodesFluss100JavaFixtures(t *testing.T) {
	schema, err := (Schema{Columns: []Column{{Name: "id", Type: IntType}, {Name: "name", Type: StringType}}}).ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		magic byte
	}{
		{"no_statistics", 0},
		{"statistics", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, err := os.ReadFile("testdata/fluss100_arrow_" + tc.name + ".hex")
			if err != nil {
				t.Fatal(err)
			}
			data, err := hex.DecodeString(string(bytes.TrimSpace(fixture)))
			if err != nil {
				t.Fatal(err)
			}
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			batch, err := DecodeArrowLogBatch(schema, data, alloc)
			if err != nil {
				t.Fatal(err)
			}
			if batch.Magic != tc.magic || batch.SchemaID != 1 || batch.Record == nil ||
				batch.Record.NumRows() != 1 || batch.Record.Column(0).(*array.Int32).Value(0) != 73 ||
				batch.Record.Column(1).(*array.String).Value(0) != "java" {
				t.Errorf("unexpected Java batch metadata or row: %+v", batch)
			}
			batch.Release()
			alloc.AssertSize(t, 0)
			if tc.magic == 0 {
				return
			}
			if got := binary.LittleEndian.Uint32(data[48:]); got != 49 {
				t.Fatalf("official statistics length = %d, want 49", got)
			}
			t.Run("statistics outside batch with valid CRC", func(t *testing.T) {
				bad := bytes.Clone(data)
				binary.LittleEndian.PutUint32(bad[48:], ^uint32(0))
				binary.LittleEndian.PutUint32(bad[21:], crc32.Checksum(bad[25:], crc32.MakeTable(crc32.Castagnoli)))
				if _, err := DecodeArrowLogBatch(schema, bad, alloc); !errors.Is(err, ErrMalformedRecordBatch) {
					t.Fatalf("invalid statistics length: %v", err)
				}
			})
			t.Run("statistics covered by CRC", func(t *testing.T) {
				bad := bytes.Clone(data)
				bad[52] ^= 1
				if _, err := DecodeArrowLogBatch(schema, bad, alloc); !errors.Is(err, ErrMalformedRecordBatch) {
					t.Fatalf("invalid statistics CRC: %v", err)
				}
			})
		})
	}
}
