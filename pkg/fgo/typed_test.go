package fgo

import (
	"context"
	"errors"
	"testing"
)

type typedRow struct{ Values Row }
type typedKey struct{ Values PrimaryKey }

var errTypedCodec = errors.New("typed codec failure")

func typedRowCodec() Codec[typedRow] {
	return CodecFuncs[typedRow]{
		EncodeFunc: func(value typedRow) (Row, error) {
			if value.Values == nil {
				return nil, errTypedCodec
			}
			return append(Row(nil), value.Values...), nil
		},
		DecodeFunc: func(row Row) (typedRow, error) {
			if row == nil {
				return typedRow{}, errTypedCodec
			}
			return typedRow{Values: append(Row(nil), row...)}, nil
		},
	}
}

func typedKeyCodec() KeyCodec[typedKey] {
	return KeyCodecFunc[typedKey](func(key typedKey) (PrimaryKey, error) {
		if key.Values == nil {
			return nil, errTypedCodec
		}
		return append(PrimaryKey(nil), key.Values...), nil
	})
}

func TestTypedCodecFunctionsValidateCallbacks(t *testing.T) {
	var codec CodecFuncs[typedRow]
	if _, err := codec.Encode(typedRow{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil encode error = %v", err)
	}
	if _, err := codec.Decode(Row{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil decode error = %v", err)
	}
	var keyCodec KeyCodecFunc[typedKey]
	if _, err := keyCodec.EncodeKey(typedKey{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil key error = %v", err)
	}
	if err := validateTypedCodec[typedRow](nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil codec error = %v", err)
	}
	if err := validateTypedKeyCodec[typedKey](nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil key codec error = %v", err)
	}
}

func TestTypedLogWriterDelegatesEncodingAndLifecycle(t *testing.T) {
	table := logWriterTable()
	backend := logBackend(0)
	base, err := newLogWriter(context.Background(), backend, table, WithLogLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := wrapTypedLogWriter(base, typedRowCodec())
	if err != nil {
		t.Fatal(err)
	}
	result := writer.Append(context.Background(), typedRow{Values: Row{int32(1), "one"}}).
		Await(context.Background())
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if result := writer.Append(context.Background(), typedRow{}).Await(context.Background()); !errors.Is(result.Err, errTypedCodec) {
		t.Fatalf("codec error = %v", result.Err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTypedKVWriterDelegatesUpsertDeleteAndLifecycle(t *testing.T) {
	table := kvWriterTable()
	base, err := newKVWriter(context.Background(), kvBackend(0), table, WithKVLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := wrapTypedKVWriter(base, typedRowCodec(), typedKeyCodec())
	if err != nil {
		t.Fatal(err)
	}
	upsert := writer.Upsert(context.Background(), typedRow{
		Values: Row{int32(1), "one", int64(10)},
	}).Await(context.Background())
	if upsert.Err != nil {
		t.Fatal(upsert.Err)
	}
	deleted := writer.Delete(context.Background(), typedKey{
		Values: PrimaryKey{int32(1)},
	}).Await(context.Background())
	if deleted.Err != nil {
		t.Fatal(deleted.Err)
	}
	if result := writer.Upsert(context.Background(), typedRow{}).Await(context.Background()); !errors.Is(result.Err, errTypedCodec) {
		t.Fatalf("upsert codec error = %v", result.Err)
	}
	if result := writer.Delete(context.Background(), typedKey{}).Await(context.Background()); !errors.Is(result.Err, errTypedCodec) {
		t.Fatalf("delete codec error = %v", result.Err)
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTypedLookupConvertsPointAndPrefixResults(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	firstKey := PrimaryKey{"a", int32(1)}
	secondKey := PrimaryKey{"a"}
	putLookupValue(t, backend, table, firstKey, Row{"a", int32(1), "one"})
	encodedPrefix, err := EncodePrefixLookupKey(table.Schema, secondKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	backend.prefixes[string(encodedPrefix)] = [][]byte{
		encodeLookupValue(t, table, Row{"a", int32(1), "one"}),
		encodeLookupValue(t, table, Row{"a", int32(2), "two"}),
	}
	base, err := newLookupClient(context.Background(), backend, table)
	if err != nil {
		t.Fatal(err)
	}
	client, err := wrapTypedLookupClient(base, typedRowCodec(), typedKeyCodec())
	if err != nil {
		t.Fatal(err)
	}
	points := client.Lookup(
		context.Background(),
		typedKey{Values: firstKey},
		typedKey{},
		typedKey{Values: PrimaryKey{"missing", int32(2)}},
	)
	if len(points) != 3 || !points[0].Found || points[0].Value.Values[2] != "one" ||
		!errors.Is(points[1].Err, errTypedCodec) || !errors.Is(points[2].Err, ErrNotFound) {
		t.Fatalf("typed points = %#v", points)
	}
	prefixes := client.PrefixLookup(
		context.Background(),
		typedKey{Values: secondKey},
		typedKey{},
	)
	if len(prefixes) != 2 || len(prefixes[0].Values) != 2 ||
		prefixes[0].Values[1].Values[2] != "two" || !errors.Is(prefixes[1].Err, errTypedCodec) {
		t.Fatalf("typed prefixes = %#v", prefixes)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedLookupReportsDecodeFailure(t *testing.T) {
	table := lookupTable()
	backend := lookupBackendFor(table, 0, 1)
	key := PrimaryKey{"a", int32(1)}
	putLookupValue(t, backend, table, key, Row{"a", int32(1), "one"})
	base, err := newLookupClient(context.Background(), backend, table)
	if err != nil {
		t.Fatal(err)
	}
	codec := CodecFuncs[typedRow]{
		EncodeFunc: typedRowCodec().Encode,
		DecodeFunc: func(Row) (typedRow, error) { return typedRow{}, errTypedCodec },
	}
	client, err := wrapTypedLookupClient(base, codec, typedKeyCodec())
	if err != nil {
		t.Fatal(err)
	}
	result := client.Lookup(context.Background(), typedKey{Values: key})
	if !errors.Is(result[0].Err, errTypedCodec) {
		t.Fatalf("decode error = %v", result[0].Err)
	}
	_ = client.Close()
}

func TestTypedLogScannerConvertsRows(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	backend.fetches[0] = scannerFetch{
		records:       encodedRows(t, table.Schema, 5, 1, 2),
		highWatermark: 8,
	}
	base, err := newLogScanner(context.Background(), backend, table, AtOffset(5))
	if err != nil {
		t.Fatal(err)
	}
	scanner, err := wrapTypedLogScanner(base, typedRowCodec())
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Records) != 2 ||
		result.Records[0].Value.Values[0] != int32(1) ||
		result.HighWatermark[0] != 8 {
		t.Fatalf("typed scan = %#v, %v", result, err)
	}
	scanner.Wakeup()
	if scanner.Done() {
		t.Fatal("unbounded scanner unexpectedly done")
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedBatchScannerConvertsRows(t *testing.T) {
	table := kvWriterTable()
	encoded := encodedValueBatch(t, table, Row{int32(1), "one", int64(10)})
	base, err := newBatchScanner(
		context.Background(),
		batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
			return false, encoded, nil
		}),
		nil, table, testTableBucket(table),
	)
	if err != nil {
		t.Fatal(err)
	}
	scanner, err := WrapTypedBatchScanner(base, typedRowCodec())
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || len(result.Values) != 1 ||
		result.Values[0].Values[1] != "one" || !scanner.Done() {
		t.Fatalf("typed batch = %#v, %v", result, err)
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedBatchScannerConstructors(t *testing.T) {
	table := kvWriterTable()
	bucket := testTableBucket(table)
	codec := typedRowCodec()

	current, err := NewTypedBatchScanner(
		context.Background(), &Client{}, table, bucket, codec,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}

	reader := &fakeSnapshotBatchReader{batches: [][]Row{{
		{int32(1), "one", int64(10)},
	}}}
	client := &Client{snapshotProvider: SnapshotBatchProviderFunc(func(
		context.Context,
		SnapshotBatchRequest,
	) (SnapshotBatchReader, error) {
		return reader, nil
	})}
	snapshot, err := NewTypedSnapshotBatchScanner(
		context.Background(), client, table, bucket, 7, codec,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := snapshot.Poll(context.Background())
	if err != nil || len(result.Values) != 1 || result.Values[0].Values[1] != "one" {
		t.Fatalf("snapshot Poll() = %#v, %v", result, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedConstructorsValidateNilValues(t *testing.T) {
	codec, keyCodec := typedRowCodec(), typedKeyCodec()
	if _, err := NewTypedLogWriter(context.Background(), nil, Table{}, codec); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("log constructor error = %v", err)
	}
	if _, err := NewTypedKVWriter(context.Background(), nil, Table{}, codec, keyCodec); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("KV constructor error = %v", err)
	}
	if _, err := NewTypedLookupClient(context.Background(), nil, Table{}, codec, keyCodec); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("lookup constructor error = %v", err)
	}
	if _, err := NewTypedLogScanner(context.Background(), nil, Table{}, AtOffset(0), codec); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("scanner constructor error = %v", err)
	}
	if _, err := NewTypedBatchScanner(
		context.Background(), nil, Table{}, TableBucket{}, codec,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("batch constructor error = %v", err)
	}
	if _, err := NewTypedSnapshotBatchScanner(
		context.Background(), nil, Table{}, TableBucket{}, 0, codec,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("snapshot constructor error = %v", err)
	}
}

func TestTypedWrappersValidateNilValues(t *testing.T) {
	codec, keyCodec := typedRowCodec(), typedKeyCodec()
	for name, err := range map[string]error{
		"log wrapper": func() error { _, err := wrapTypedLogWriter[typedRow](nil, codec); return err }(),
		"KV wrapper": func() error {
			_, err := wrapTypedKVWriter[typedRow, typedKey](nil, codec, keyCodec)
			return err
		}(),
		"lookup wrapper": func() error {
			_, err := wrapTypedLookupClient[typedRow, typedKey](nil, codec, keyCodec)
			return err
		}(),
		"scan wrapper":  func() error { _, err := wrapTypedLogScanner[typedRow](nil, codec); return err }(),
		"batch wrapper": func() error { _, err := WrapTypedBatchScanner[typedRow](nil, codec); return err }(),
	} {
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s error = %v", name, err)
		}
	}
	if _, err := wrapTypedLogWriter[typedRow](&LogWriter{}, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil log codec error = %v", err)
	}
	if _, err := wrapTypedKVWriter[typedRow, typedKey](
		&KVWriter{}, nil, keyCodec,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil KV codec error = %v", err)
	}
	if _, err := wrapTypedKVWriter[typedRow, typedKey](
		&KVWriter{}, codec, nil,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil KV key codec error = %v", err)
	}
	if _, err := wrapTypedLookupClient[typedRow, typedKey](
		&LookupClient{}, codec, nil,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil lookup key codec error = %v", err)
	}
}

func TestTypedConstructorsRejectInvalidCodecsAndTables(t *testing.T) {
	codec, keyCodec := typedRowCodec(), typedKeyCodec()
	client := &Client{}
	if _, err := NewTypedLogWriter[typedRow](
		context.Background(), client, Table{}, nil,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil log codec error = %v", err)
	}
	if _, err := NewTypedKVWriter[typedRow, typedKey](
		context.Background(), client, Table{}, codec, nil,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil KV key codec error = %v", err)
	}
	if _, err := NewTypedLookupClient[typedRow, typedKey](
		context.Background(), client, Table{}, codec, nil,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil lookup key codec error = %v", err)
	}
	if _, err := NewTypedLogScanner[typedRow](
		context.Background(), client, Table{}, AtOffset(0), nil,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil scanner codec error = %v", err)
	}
	if _, err := NewTypedBatchScanner[typedRow](
		context.Background(), client, Table{}, TableBucket{}, nil,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil batch codec error = %v", err)
	}
	if _, err := NewTypedSnapshotBatchScanner[typedRow](
		context.Background(), client, Table{}, TableBucket{}, 0, nil,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil snapshot codec error = %v", err)
	}
	for name, open := range map[string]func() error{
		"log writer": func() error {
			_, err := NewTypedLogWriter(context.Background(), client, Table{}, codec)
			return err
		},
		"KV writer": func() error {
			_, err := NewTypedKVWriter(context.Background(), client, Table{}, codec, keyCodec)
			return err
		},
		"lookup": func() error {
			_, err := NewTypedLookupClient(context.Background(), client, Table{}, codec, keyCodec)
			return err
		},
		"log scanner": func() error {
			_, err := NewTypedLogScanner(context.Background(), client, Table{}, AtOffset(0), codec)
			return err
		},
	} {
		if err := open(); err == nil {
			t.Fatalf("%s with invalid table succeeded", name)
		}
	}
}

func TestTypedNilWriterReceivers(t *testing.T) {
	var logWriter *TypedLogWriter[typedRow]
	if result := logWriter.Append(context.Background(), typedRow{}).Await(context.Background()); !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("nil log append error = %v", result.Err)
	}
	if err := logWriter.Flush(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil log flush error = %v", err)
	}
	if err := logWriter.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	var kvWriter *TypedKVWriter[typedRow, typedKey]
	if result := kvWriter.Upsert(context.Background(), typedRow{}).Await(context.Background()); !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("nil KV upsert error = %v", result.Err)
	}
	if result := kvWriter.Delete(context.Background(), typedKey{}).Await(context.Background()); !errors.Is(result.Err, ErrInvalidConfig) {
		t.Fatalf("nil KV delete error = %v", result.Err)
	}
	if err := kvWriter.Flush(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil KV flush error = %v", err)
	}
	if err := kvWriter.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTypedNilReaderReceivers(t *testing.T) {
	var lookup *TypedLookupClient[typedRow, typedKey]
	if result := lookup.Lookup(context.Background(), typedKey{}); !errors.Is(result[0].Err, ErrInvalidConfig) {
		t.Fatalf("nil lookup error = %v", result[0].Err)
	}
	if result := lookup.PrefixLookup(context.Background(), typedKey{}); !errors.Is(result[0].Err, ErrInvalidConfig) {
		t.Fatalf("nil prefix error = %v", result[0].Err)
	}
	if err := lookup.Close(); err != nil {
		t.Fatal(err)
	}
	var scanner *TypedLogScanner[typedRow]
	if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil scan error = %v", err)
	}
	scanner.Wakeup()
	if !scanner.Done() {
		t.Fatal("nil scanner must be done")
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	var batch *TypedBatchScanner[typedRow]
	if _, err := batch.Poll(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil batch error = %v", err)
	}
	if !batch.Done() {
		t.Fatal("nil batch must be done")
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
}
