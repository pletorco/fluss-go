package fgo

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
)

func TestKVBatchRoundTrip(t *testing.T) {
	want := KVBatch{SchemaID: 7, WriterID: 99, BatchSequence: 4, Records: []KVRecord{{Key: []byte("first"), Value: []byte("value")}, {Key: []byte("deleted")}}}
	encoded, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeKVBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.SchemaID != want.SchemaID || got.WriterID != want.WriterID || got.BatchSequence != want.BatchSequence || len(got.Records) != 2 || !bytes.Equal(got.Records[0].Key, want.Records[0].Key) || !bytes.Equal(got.Records[0].Value, want.Records[0].Value) || got.Records[1].Value != nil {
		t.Fatalf("decoded batch = %#v", got)
	}
}

func TestKVAndLogBatchesMatchJava091Fixtures(t *testing.T) {
	schema := java091FixtureSchema()
	row := Row{int32(42), "fluss", []any{int32(1), nil, int32(-2)}, Row{int32(7), "go"}}
	rowBytes, err := EncodeCompactedRow(schema, row)
	if err != nil {
		t.Fatal(err)
	}
	javaKV, err := hex.DecodeString("4b000000004989f86709000007000000000000000d0000000200000029000000012a002a05666c7573731803000000020000000100000000000000feffffff0000000005000702676f02000000012b")
	if err != nil {
		t.Fatal(err)
	}
	kv := KVBatch{SchemaID: 9, WriterID: 7, BatchSequence: 13, Records: []KVRecord{
		{Key: []byte{42}, Value: rowBytes},
		{Key: []byte{43}},
	}}
	goKV, err := kv.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(goKV, javaKV) {
		t.Fatalf("Go KV = %x, Java KV = %x", goKV, javaKV)
	}
	decodedKV, err := DecodeKVBatch(javaKV)
	if err != nil || !reflect.DeepEqual(decodedKV, kv) {
		t.Fatalf("decoded Java KV = %#v, %v", decodedKV, err)
	}

	javaLog, err := hex.DecodeString("000000000000000050000000000000000000000000709a7bb70900000000000007000000000000000d000000010000002800000003002a05666c7573731803000000020000000100000000000000feffffff0000000005000702676f")
	if err != nil {
		t.Fatal(err)
	}
	logBatch := LogBatch{Magic: 0, SchemaID: 9, WriterID: 7, BatchSequence: 13, Records: []Record{{Change: UpdateAfter, Value: row}}}
	goLog, err := logBatch.EncodeRows(schema, true)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(goLog, javaLog) {
		t.Fatalf("Go log = %x, Java log = %x", goLog, javaLog)
	}
	decodedLog, err := DecodeLogBatchRows(schema, javaLog, true)
	if err != nil || decodedLog.Records[0].Change != UpdateAfter || !reflect.DeepEqual(decodedLog.Records[0].Value, row) {
		t.Fatalf("decoded Java log = %#v, %v", decodedLog, err)
	}
}

func TestKVBatchRejectsCorruption(t *testing.T) {
	encoded, err := (KVBatch{Records: []KVRecord{{Key: []byte("key")}}}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range [][]byte{encoded[:4], append([]byte(nil), encoded...)} {
		if len(corrupt) == len(encoded) {
			corrupt[len(corrupt)-1] ^= 1
		}
		if _, err := DecodeKVBatch(corrupt); !errors.Is(err, ErrMalformedRecordBatch) {
			t.Fatalf("DecodeKVBatch(%x) error = %v", corrupt, err)
		}
	}
}

func TestLogBatchEncodesV0AndV1Headers(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: IntType}}}
	for _, magic := range []byte{0, 1} {
		encoded, err := (LogBatch{Magic: magic, BaseOffset: -1, LeaderEpoch: 5, SchemaID: 2, WriterID: 3, BatchSequence: 4, Records: []Record{{Value: Row{int32(1)}, Change: Append}}}).EncodeRows(schema, true)
		if err != nil {
			t.Fatal(err)
		}
		if encoded[12] != magic || len(encoded) != map[byte]int{0: logBatchV0HeaderSize + 7, 1: logBatchV1HeaderSize + 7}[magic] {
			t.Fatalf("magic %d batch = %x", magic, encoded)
		}
		decoded, err := DecodeLogBatchRows(schema, encoded, true)
		if err != nil || decoded.Magic != magic || decoded.Records[0].Value[0] != int32(1) {
			t.Fatalf("DecodeLogBatchRows() = %#v, %v", decoded, err)
		}
	}
}

func TestLogBatchRejectsInvalidChecksumAndCount(t *testing.T) {
	schema := Schema{Columns: []Column{{Name: "id", Type: IntType}}}
	encoded, err := (LogBatch{Magic: 0, Records: []Record{{Value: Row{int32(1)}, Change: Append}}}).EncodeRows(schema, false)
	if err != nil {
		t.Fatal(err)
	}
	encoded[21] ^= 1
	if _, err := DecodeLogBatchRows(schema, encoded, false); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("checksum error = %v", err)
	}
}

func FuzzDecodeKVBatch(f *testing.F) {
	f.Add([]byte{24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > maxRowBytes+1 {
			t.Skip()
		}
		_, _ = DecodeKVBatch(encoded)
	})
}

func FuzzDecodeLogBatchRows(f *testing.F) {
	schema := Schema{Columns: []Column{{Name: "id", Type: IntType}}}
	seed, err := (LogBatch{Magic: 0, Records: []Record{{Value: Row{int32(1)}, Change: Append}}}).EncodeRows(schema, true)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > maxRowBytes+1 {
			t.Skip()
		}
		_, _ = DecodeLogBatchRows(schema, encoded, true)
	})
}
