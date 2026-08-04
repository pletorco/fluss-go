package fgo

import (
	"errors"
	"testing"
	"time"
)

func TestSharedRecordModels(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: BigIntType}, {Name: "value", Type: StringType, Nullable: true}}, PrimaryKey: []string{"id"}}
	row, err := schema.RowFromNamed(NamedRow{"id": int64(1), "value": "ok"})
	if err != nil || row[1] != "ok" {
		t.Fatalf("named row = %#v, %v", row, err)
	}
	record := Record{Key: PrimaryKey{int64(1)}, Value: row, Change: Upsert, Offset: -1}
	if err := record.Validate(schema); err != nil {
		t.Fatal(err)
	}
	if err := (TableBucket{TableID: 1, PartitionID: -1, BucketID: 0, Leader: ServerNode{Address: "tablet:9123", ServerType: TabletServer}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "t"}, Partition: "day=2026-07-30"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if got := (PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "t"}}).String(); got != "db.t" {
		t.Fatalf("table path = %q", got)
	}
	if got := (PhysicalTablePath{
		TablePath: TablePath{Database: "db", Table: "t"}, Partition: "day=2026-07-30",
	}).String(); got != "db.t(p=day=2026-07-30)" {
		t.Fatalf("partition path = %q", got)
	}
}

func TestSharedRecordModelFailures(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: BigIntType}}, PrimaryKey: []string{"id"}}
	if _, err := schema.RowFromNamed(NamedRow{"other": int64(1)}); !errors.Is(err, ErrInvalidRow) {
		t.Fatal(err)
	}
	if err := (Record{Change: ChangeType(99)}).Validate(schema); !errors.Is(err, ErrInvalidRow) {
		t.Fatal(err)
	}
	if err := (OffsetSpec{Offset: 1, Timestamp: time.Now()}).Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if err := (TableBucket{}).Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if err := (PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "t"}, Partition: " "}).Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := schema.RowFromNamed(NamedRow{"id": int64(1), "extra": true}); !errors.Is(err, ErrInvalidRow) {
		t.Fatal(err)
	}
	if err := (Record{Key: PrimaryKey{int64(1)}, Change: Delete, Offset: 0}).Validate(schema); err != nil {
		t.Fatal(err)
	}
	if err := (Record{Key: PrimaryKey{int64(1)}, Change: Append, Offset: -2}).Validate(schema); !errors.Is(err, ErrInvalidRow) {
		t.Fatal(err)
	}
	if err := (OffsetSpec{Offset: -2}).Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
}
