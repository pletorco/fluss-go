package fgo

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"testing"
)

func TestWideDecimalKeyMatchesJavaFixture(t *testing.T) {
	schema := Schema{
		Columns:    []Column{logicalColumn("amount", LogicalType{Root: "DECIMAL", Precision: 38, Scale: 2})},
		PrimaryKey: []string{"amount"},
	}
	// The payload bytes come from Java BigInteger.toByteArray(); the Go
	// compacted key prepends the payload's Fluss varint byte length.
	for _, test := range []struct {
		unscaled string
		payload  string
	}{
		{"-32769", "ff7fff"}, {"-32768", "8000"}, {"-32767", "8001"},
		{"-129", "ff7f"}, {"-128", "80"}, {"-127", "81"},
		{"-1", "ff"}, {"0", "00"}, {"1", "01"},
		{"127", "7f"}, {"128", "0080"}, {"129", "0081"},
		{"32767", "7fff"}, {"32768", "008000"}, {"32769", "008001"},
		{"-664613997892457936451903530140172288", "800000000000000000000000000000"},
		{"664613997892457936451903530140172288", "00800000000000000000000000000000"},
	} {
		t.Run(test.unscaled, func(t *testing.T) {
			unscaled, ok := new(big.Int).SetString(test.unscaled, 10)
			if !ok {
				t.Fatal("invalid decimal fixture")
			}
			amount := new(big.Rat).SetFrac(unscaled, big.NewInt(100))
			payload, err := hex.DecodeString(test.payload)
			if err != nil {
				t.Fatal(err)
			}
			want := append([]byte{byte(len(payload))}, payload...)
			got, err := EncodePrimaryKey(schema, PrimaryKey{amount})
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("key = %x, %v; want %x", got, err, want)
			}
			indexed, err := appendIndexedValue(nil, logicalTypeForColumn(schema.Columns[0]), amount)
			if err != nil || !bytes.Equal(indexed, payload) {
				t.Fatalf("indexed value = %x, %v; want %x", indexed, err, payload)
			}
		})
	}
}

func TestWideDecimalCanonicalBucketRouting(t *testing.T) {
	schema := Schema{
		Columns:    []Column{logicalColumn("amount", LogicalType{Root: "DECIMAL", Precision: 20, Scale: 2})},
		PrimaryKey: []string{"amount"},
	}
	layout, err := newKVKeyLayout(schema, "")
	if err != nil {
		t.Fatal(err)
	}
	key, bucket, err := layout.encodePoint(schema, map[string]any{"amount": big.NewRat(-128, 100)}, 17)
	canonical := []byte{0x01, 0x80}
	newBucket, _ := flussBucket(canonical, 17)
	oldBucket, _ := flussBucket([]byte{0x02, 0xff, 0x80}, 17)
	if err != nil || !bytes.Equal(key, canonical) || bucket != newBucket || bucket == oldBucket {
		t.Fatalf("key %x routed to bucket %d, err %v; want canonical bucket %d, legacy %d", key, bucket, err, newBucket, oldBucket)
	}
}

func TestWideDecimalLegacyValueDecoding(t *testing.T) {
	kind := LogicalType{Root: "DECIMAL", Precision: 20, Scale: 2}
	want := big.NewRat(-128, 100)
	// Redundant ff sign bytes were previously emitted for this value.
	compacted := []byte{0x02, 0xff, 0x80}
	indexed := compacted[1:]
	decoded, next, err := readCompactedDecimal(compacted, 0, kind)
	if err != nil || next != len(compacted) || decoded.(*big.Rat).Cmp(want) != 0 {
		t.Fatalf("compacted legacy value = %v, %d, %v", decoded, next, err)
	}
	decoded, next, err = readIndexedValue(indexed, 0, kind, len(indexed))
	if err != nil || next != len(indexed) || decoded.(*big.Rat).Cmp(want) != 0 {
		t.Fatalf("indexed legacy value = %v, %d, %v", decoded, next, err)
	}
}
