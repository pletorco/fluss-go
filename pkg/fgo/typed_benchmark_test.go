package fgo

import "testing"

func BenchmarkTypedCodec(b *testing.B) {
	value := typedRow{Values: Row{int32(7), "seven", int64(70)}}
	row := value.Values
	codec := typedRowCodec()

	b.Run("row", func(b *testing.B) {
		for range b.N {
			if row[0] != int32(7) {
				b.Fatal("unexpected row")
			}
		}
	})
	b.Run("encode", func(b *testing.B) {
		for range b.N {
			encoded, err := codec.Encode(value)
			if err != nil || len(encoded) != 3 {
				b.Fatal("unexpected encoded value")
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		for range b.N {
			decoded, err := codec.Decode(row)
			if err != nil || len(decoded.Values) != 3 {
				b.Fatal("unexpected decoded value")
			}
		}
	})
}
