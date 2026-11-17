package fgo

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"
)

func decimalValidationCases() []struct {
	name    string
	column  Column
	invalid any
	valid   any
} {
	decimal := LogicalType{Root: "DECIMAL", Precision: 20, Scale: 2, Nullable: true}
	var nilDecimal *big.Rat
	return []struct {
		name    string
		column  Column
		invalid any
		valid   any
	}{
		{"required", logicalColumn("amount", LogicalType{Root: "DECIMAL", Precision: 8, Scale: 2}), nilDecimal, new(big.Rat)},
		{"nullable", logicalColumn("amount", decimal), nilDecimal, nil},
		{"array", logicalColumn("amount", LogicalType{Root: "ARRAY", Element: &decimal}), []any{nilDecimal}, []any{nil, big.NewRat(123, 100)}},
		{"map key", logicalColumn("amount", LogicalType{Root: "MAP", Key: &decimal, Value: &decimal}), Map{{Key: nilDecimal, Value: big.NewRat(1, 1)}}, Map{{Key: big.NewRat(1, 1), Value: nil}}},
		{"map value", logicalColumn("amount", LogicalType{Root: "MAP", Key: &decimal, Value: &decimal}), Map{{Key: big.NewRat(1, 1), Value: nilDecimal}}, Map{{Key: big.NewRat(1, 1), Value: big.NewRat(2, 1)}}},
		{"row", logicalColumn("amount", LogicalType{Root: "ROW", Fields: []LogicalField{{Name: "nested", Type: decimal}}}), Row{nilDecimal}, Row{nil}},
	}
}

func TestDecimalValidationRejectsTypedNil(t *testing.T) {
	for _, test := range decimalValidationCases() {
		t.Run(test.name, func(t *testing.T) {
			schema := Schema{Columns: []Column{test.column}}
			if err := schema.ValidateRow(Row{test.invalid}, nil); !errors.Is(err, ErrInvalidRow) {
				t.Fatalf("ValidateRow(typed nil) = %v; want ErrInvalidRow", err)
			}
			for index, codec := range rowCodecs() {
				if _, err := codec.encode(schema, Row{test.invalid}); !errors.Is(err, ErrInvalidRow) {
					t.Fatalf("codec %d encode(typed nil) = %v; want ErrInvalidRow", index, err)
				}
				encoded, err := codec.encode(schema, Row{test.valid})
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := codec.decode(schema, encoded)
				if err != nil {
					t.Fatal(err)
				}
				if !arrayValueEqual(decoded[0], test.valid) {
					t.Fatalf("codec %d roundtrip = %#v; want %#v", index, decoded[0], test.valid)
				}
			}
		})
	}
	// Schemas without logical metadata share the same decimal type check.
	schema := Schema{Columns: []Column{{Name: "amount", Type: DecimalType, Nullable: true}}}
	if err := schema.ValidateRow(Row{(*big.Rat)(nil)}, nil); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("untyped schema validation = %v; want ErrInvalidRow", err)
	}
}

func TestDecimalNilAndZeroRemainDistinct(t *testing.T) {
	for _, precision := range []int{8, 20, 38} {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("precision%d/nullable%t", precision, nullable), func(t *testing.T) {
				schema := Schema{Columns: []Column{logicalColumn("amount", LogicalType{
					Root: "DECIMAL", Precision: precision, Scale: 2, Nullable: nullable,
				})}}
				limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(precision)), nil)
				maximum := new(big.Rat).SetFrac(new(big.Int).Sub(limit, big.NewInt(1)), big.NewInt(100))
				for index, codec := range rowCodecs() {
					_, err := codec.encode(schema, Row{nil})
					if nullable && err != nil || !nullable && !errors.Is(err, ErrInvalidRow) {
						t.Fatalf("codec %d encode(NULL) = %v; nullable %t", index, err, nullable)
					}
					for _, value := range []*big.Rat{new(big.Rat), big.NewRat(123, 100), big.NewRat(-123, 100), maximum, new(big.Rat).Neg(maximum)} {
						encoded, err := codec.encode(schema, Row{value})
						if err != nil {
							t.Fatal(err)
						}
						decoded, err := codec.decode(schema, encoded)
						if err != nil || decoded[0].(*big.Rat).Cmp(value) != 0 {
							t.Fatalf("codec %d decimal roundtrip = %v, %v; want %v", index, decoded, err, value)
						}
					}
					for _, invalid := range []*big.Rat{new(big.Rat).SetFrac(limit, big.NewInt(100)), big.NewRat(1, 1000)} {
						if _, err := codec.encode(schema, Row{invalid}); err == nil {
							t.Fatalf("codec %d accepted precision/scale overflow %v", index, invalid)
						}
					}
				}
			})
		}
	}
}

func TestAppendRejectsTypedNilDecimalBeforeAdmission(t *testing.T) {
	for _, format := range []LogFormat{LogFormatCompacted, LogFormatIndexed} {
		for _, test := range decimalValidationCases() {
			t.Run(string(format)+"/"+test.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				table := appendWriterTable()
				table.Schema.Columns[1] = test.column
				backend := logBackend(0)
				writer, err := newAppendWriter(ctx, backend, table, WithAppendLogFormat(format), WithAppendBuffer(1))
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close(ctx)
				assertDecimalRejectedFuture(t, writer.Append(ctx, Row{int32(1), test.invalid}))
				if len(writer.slots) != 0 || len(writer.commands) != 0 || len(backend.produced()) != 0 {
					t.Fatal("invalid append consumed admission capacity or reached the backend")
				}
				future := writer.Append(ctx, Row{int32(1), test.valid})
				if err := writer.Close(ctx); err != nil {
					t.Fatal(err)
				}
				if result := future.Await(ctx); result.Err != nil || len(writer.slots) != 0 || len(backend.produced()) != 1 {
					t.Fatalf("valid append after rejection = %+v", result)
				}
			})
		}
	}
}

func TestUpsertRejectsTypedNilDecimalBeforeAdmission(t *testing.T) {
	for _, test := range decimalValidationCases() {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			table := upsertWriterTable()
			table.Schema.Columns[1] = test.column
			backend := kvBackend(0)
			writer, err := newUpsertWriter(ctx, backend, table, WithUpsertBuffer(1))
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close(ctx)
			assertDecimalRejectedFuture(t, writer.Upsert(ctx, Row{int32(1), test.invalid, nil}))
			assertDecimalRejectedFuture(t, writer.PartialUpsert(ctx, []string{"id", "amount"}, Row{int32(1), test.invalid}))
			if len(writer.slots) != 0 || len(writer.commands) != 0 || len(backend.putCalls()) != 0 {
				t.Fatal("invalid upsert consumed admission capacity or reached the backend")
			}
			future := writer.Upsert(ctx, Row{int32(1), test.valid, nil})
			if err := writer.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if result := future.Await(ctx); result.Err != nil || len(writer.slots) != 0 || len(backend.putCalls()) != 1 {
				t.Fatalf("valid upsert after rejection = %+v", result)
			}
		})
	}
}

func TestDecimalKeyRejectsTypedNil(t *testing.T) {
	table := upsertWriterTable()
	table.Schema.Columns[0] = logicalColumn("id", LogicalType{Root: "DECIMAL", Precision: 20, Scale: 2})
	key := PrimaryKey{(*big.Rat)(nil)}
	for _, encode := range []func(Schema, PrimaryKey) ([]byte, error){EncodePrimaryKey, EncodePrefixKey} {
		if _, err := encode(table.Schema, key); !errors.Is(err, ErrInvalidRow) {
			t.Fatalf("encode typed-nil key = %v; want ErrInvalidRow", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backend := kvBackend(0)
	writer, err := newUpsertWriter(ctx, backend, table, WithUpsertBuffer(1))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	assertDecimalRejectedFuture(t, writer.Delete(ctx, key))
	assertDecimalRejectedFuture(t, writer.Upsert(ctx, Row{key[0], nil, nil}))
	assertDecimalRejectedFuture(t, writer.PartialUpsert(ctx, []string{"id"}, Row{key[0]}))
	if len(writer.slots) != 0 || len(writer.commands) != 0 || len(backend.putCalls()) != 0 {
		t.Fatal("invalid decimal key consumed capacity or reached the backend")
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertDecimalRejectedFuture(t *testing.T, future *WriteFuture) {
	t.Helper()
	select {
	case <-future.done:
	default:
		t.Fatal("typed-nil decimal did not complete its future synchronously")
	}
	if result := future.Await(context.Background()); !errors.Is(result.Err, ErrInvalidRow) {
		t.Fatalf("typed-nil decimal result = %+v; want ErrInvalidRow", result)
	}
}
