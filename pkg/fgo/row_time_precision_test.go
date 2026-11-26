package fgo

import (
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestRowAndKeyTimePrecision(t *testing.T) {
	kind := LogicalType{Root: "TIME_WITHOUT_TIME_ZONE", Precision: 9}
	schema := Schema{Columns: []Column{logicalColumn("clock", kind)}, PrimaryKey: []string{"clock"}}
	arraySchema := Schema{Columns: []Column{logicalColumn("clocks", LogicalType{Root: "ARRAY", Element: &kind})}}
	unrepresentable := time.Date(1970, 1, 1, 12, 34, 56, 123456789, time.UTC)
	representable := unrepresentable.Truncate(time.Millisecond)
	for _, tc := range []struct {
		name   string
		encode func(Schema, Row) ([]byte, error)
		decode func(Schema, []byte) (Row, error)
	}{
		{"compacted", EncodeCompactedRow, DecodeCompactedRow},
		{"indexed", EncodeIndexedRow, DecodeIndexedRow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.encode(schema, Row{unrepresentable}); !errors.Is(err, ErrInvalidRow) {
				t.Fatalf("TIME(9) row error = %v", err)
			}
			if _, err := tc.encode(arraySchema, Row{[]any{unrepresentable}}); !errors.Is(err, ErrInvalidRow) {
				t.Fatalf("TIME(9) nested row error = %v", err)
			}
			encoded, err := tc.encode(schema, Row{representable})
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := tc.decode(schema, encoded)
			if err != nil || !decoded[0].(time.Time).Equal(representable) {
				t.Fatalf("millisecond row = %v, %v", decoded, err)
			}
		})
	}
	if _, err := EncodePrimaryKey(schema, PrimaryKey{unrepresentable}); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("TIME(9) key error = %v", err)
	}
	if kvPartitionTimeMatches(TimeType, unrepresentable, representable) {
		t.Fatal("sub-millisecond TIME(9) matched a millisecond partition")
	}
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := arrowSchema.Field(0).Type.(*arrow.Time64Type); !ok || got.Unit != arrow.Nanosecond {
		t.Fatalf("Arrow TIME(9) must keep nanosecond representation: %v", got)
	}
}
