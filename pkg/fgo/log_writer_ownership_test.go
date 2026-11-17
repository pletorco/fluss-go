package fgo

import (
	"bytes"
	"context"
	"math/big"
	"sync"
	"testing"
	"time"
)

func TestAppendWriterOwnsMutableInput(t *testing.T) {
	for _, format := range []LogFormat{LogFormatCompacted, LogFormatIndexed} {
		for _, bucketKey := range []string{"", "payload", "items", "labels"} {
			t.Run(string(format)+"/key="+bucketKey, func(t *testing.T) {
				testAppendWriterOwnsMutableInput(t, format, bucketKey)
			})
		}
	}
}

func testAppendWriterOwnsMutableInput(t *testing.T, format LogFormat, bucketKey string) {
	t.Helper()
	schema, row, mutate := mutableAppendInput()
	table := appendWriterTable()
	table.Schema, table.BucketCount = schema, 4
	if bucketKey != "" {
		table.Schema.BucketKey = []string{bucketKey}
	}
	backend := logBackend(0, 1, 2, 3)
	writer, release := pausedOwnershipWriter(t, backend, table, format)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wantBucket, sequence := int32(1), int32(0)
	if bucketKey != "" {
		values, err := writer.bucketKeyValues(row)
		if err != nil {
			t.Fatal(err)
		}
		key, err := encodeKeyColumns(table.Schema, table.Schema.BucketKey, values)
		if err != nil {
			t.Fatal(err)
		}
		wantBucket, err = flussBucket(key, 4)
		if err != nil {
			t.Fatal(err)
		}
		sequence = 1 // The warm-up record used the same immutable key.
	}
	want, err := (LogBatch{
		BaseOffset: -1, SchemaID: int16(table.SchemaID), AppendOnly: true,
		WriterID: 42, BatchSequence: sequence, Records: []Record{{Value: row, Change: Append}},
	}).EncodeRows(table.Schema, format != LogFormatIndexed)
	if err != nil {
		t.Fatal(err)
	}
	future := writer.Append(ctx, row)
	mutate()
	release()
	result := future.Await(ctx)
	if result.Err != nil || result.Bucket != wantBucket {
		t.Fatalf("Append result = %#v, want bucket %d", result, wantBucket)
	}
	calls := backend.produced()
	if len(calls) != 2 || calls[1].bucket != wantBucket || !bytes.Equal(calls[1].records, want) {
		t.Fatalf("queued payload/routing changed after input reuse: calls=%d, want bucket=%d bytes=%x", len(calls), wantBucket, want)
	}
}

// Pause the scheduler after a completed warm-up request so the tested record
// cannot be routed or encoded asynchronously before the caller reuses it.
func pausedOwnershipWriter(t *testing.T, backend *fakeAppendWriterBackend, table Table, format LogFormat) (*AppendWriter, func()) {
	t.Helper()
	writer, err := newAppendWriter(context.Background(), backend, table,
		WithAppendLogFormat(format), WithAppendBatchTimeout(0), WithAppendNoKeyAssigner(NoKeyAssignerRoundRobin))
	if err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var pauseOnce, resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(func() {
		release()
		if err := writer.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	writer.observer = MetricsObserverFunc(func(event MetricEvent) {
		if event.Kind == MetricWriteBatch {
			pauseOnce.Do(func() {
				close(entered)
				<-resume
			})
		}
	})
	_, warmup, _ := mutableAppendInput()
	first := writer.Append(context.Background(), warmup)
	select {
	case <-entered:
	case <-first.done:
		t.Fatalf("warm-up append failed: %v", first.data.Err)
	}
	return writer, release
}

func mutableAppendInput() (Schema, Row, func()) {
	binary := LogicalType{Root: "BYTES"}
	nested := LogicalType{Root: "ROW", Fields: []LogicalField{{Name: "value", Type: binary}}}
	array := LogicalType{Root: "ARRAY", Element: &binary}
	schema := Schema{Columns: []Column{
		{Name: "id", Type: IntType},
		{Name: "payload", Type: BytesType},
		logicalColumn("amount", LogicalType{Root: "DECIMAL", Precision: 20, Scale: 2}),
		logicalColumn("items", LogicalType{Root: "ARRAY", Element: &nested}),
		logicalColumn("labels", LogicalType{Root: "MAP", Key: &binary, Value: &array}),
		logicalColumn("attributes", LogicalType{Root: "MAP", Key: &LogicalType{Root: "STRING"}, Value: &binary}),
		logicalColumn("nested", nested),
	}}
	payload, arrayBytes, mapKey := []byte("old"), []byte("array"), []byte("key")
	mapBytes, goMapBytes, rowBytes := []byte("map"), []byte("go-map"), []byte("row")
	amount := big.NewRat(123, 100)
	items := []any{Row{arrayBytes}}
	labels := Map{{Key: mapKey, Value: []any{mapBytes}}}
	attributes := map[string]any{"key": goMapBytes}
	nestedRow := Row{rowBytes}
	row := Row{int32(1), payload, amount, items, labels, attributes, nestedRow}
	mutate := func() {
		row[0] = int32(99)
		for _, value := range [][]byte{payload, arrayBytes, mapKey, mapBytes, goMapBytes, rowBytes} {
			value[0] = 'X'
		}
		amount.SetInt64(9)
		items[0] = Row{[]byte("replacement")}
		labels[0] = MapEntry{Key: []byte("new"), Value: []any{[]byte("replacement")}}
		delete(attributes, "key")
		attributes["new"] = []byte("replacement")
		nestedRow[0] = []byte("replacement")
	}
	return schema, row, mutate
}

func TestAppendWriterAccountsForIndexedRowBytes(t *testing.T) {
	table := appendWriterTable()
	table.Schema.Columns[1].Type = BytesType
	row := Row{int32(1), bytes.Repeat([]byte{'x'}, 24)}
	encoded, err := EncodeIndexedRow(table.Schema, row)
	if err != nil {
		t.Fatal(err)
	}
	backend := logBackend(0)
	writer, err := newAppendWriter(context.Background(), backend, table,
		WithAppendLogFormat(LogFormatIndexed), WithAppendBatchTimeout(time.Hour),
		WithAppendBatchLimits(2*(len(encoded)+5)-1, 100))
	if err != nil {
		t.Fatal(err)
	}
	first := writer.Append(context.Background(), row)
	second := writer.Append(context.Background(), row)
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, future := range []*WriteFuture{first, second} {
		if result := future.Await(context.Background()); result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	if calls := backend.produced(); len(calls) != 2 {
		t.Fatalf("got %d batches, want two batches using indexed row sizes", len(calls))
	}
}
