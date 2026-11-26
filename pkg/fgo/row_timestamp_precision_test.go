package fgo

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestRowAndKeyTimestampPrecision(t *testing.T) {
	base := time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC)
	for _, root := range []string{"TIMESTAMP_WITHOUT_TIME_ZONE", "TIMESTAMP_WITH_LOCAL_TIME_ZONE"} {
		for _, tc := range []struct {
			precision int
			step      time.Duration
		}{
			{0, time.Second}, {2, 10 * time.Millisecond}, {3, time.Millisecond},
			{6, time.Microsecond}, {9, time.Nanosecond},
		} {
			t.Run(fmt.Sprintf("%s(%d)", root, tc.precision), func(t *testing.T) {
				stamp := LogicalType{Root: root, Precision: tc.precision}
				schema := Schema{Columns: []Column{logicalColumn("ts", stamp)}, PrimaryKey: []string{"ts"}}
				good := base.Add(time.Second - tc.step)
				bad := good.Add(time.Nanosecond)
				k1, err := EncodePrimaryKey(schema, PrimaryKey{good})
				if err != nil {
					t.Fatal(err)
				}
				k2, err := EncodePrimaryKey(schema, PrimaryKey{good.Add(tc.step)})
				if err != nil || bytes.Equal(k1, k2) {
					t.Fatalf("distinct representable keys collided: %x / %x; %v", k1, k2, err)
				}
				if tc.precision != 9 {
					if _, err := EncodePrimaryKey(schema, PrimaryKey{bad}); !errors.Is(err, ErrInvalidRow) {
						t.Fatalf("unrepresentable key = %v", err)
					}
				}
				for index, codec := range rowCodecs() {
					name := []string{"compacted", "indexed"}[index]
					t.Run(name, func(t *testing.T) {
						assertTimestampCodec(t, stamp, good, bad, codec.encode, codec.decode)
					})
				}
			})
		}
	}
}

func assertTimestampCodec(t *testing.T, stamp LogicalType, good, bad time.Time,
	encode func(Schema, Row) ([]byte, error), decode func(Schema, []byte) (Row, error),
) {
	t.Helper()
	for _, shape := range []struct {
		typ  LogicalType
		wrap func(time.Time) any
	}{
		{stamp, func(value time.Time) any { return value }},
		{LogicalType{Root: "ARRAY", Element: &stamp}, func(value time.Time) any { return []any{value} }},
		{LogicalType{Root: "MAP", Key: &LogicalType{Root: "STRING"}, Value: &stamp}, func(value time.Time) any {
			return Map{{Key: "key", Value: value}}
		}},
		{LogicalType{Root: "ROW", Fields: []LogicalField{{Name: "ts", Type: stamp}}}, func(value time.Time) any {
			return Row{value}
		}},
	} {
		schema := Schema{Columns: []Column{logicalColumn("value", shape.typ)}}
		if stamp.Precision < 9 {
			if _, err := encode(schema, Row{shape.wrap(bad)}); !errors.Is(err, ErrInvalidRow) {
				t.Fatalf("%s accepted an unrepresentable value: %v", shape.typ.Root, err)
			}
		}
		want := Row{shape.wrap(good)}
		encoded, err := encode(schema, want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decode(schema, encoded)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s round trip = %v, %v; want %v", shape.typ.Root, got, err, want)
		}
	}
}
