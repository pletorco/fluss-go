package fgo

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func typedIDCodec(decode func(int32) error) Codec[int32] {
	return CodecFuncs[int32]{DecodeFunc: func(row Row) (int32, error) {
		id := row[0].(int32)
		return id, decode(id)
	}}
}

func TestTypedLogRetainsTerminalRowsAndMetadata(t *testing.T) {
	ctx := t.Context()
	table := appendWriterTable()
	backend := scannerBackend(0, 1)
	backend.fetchErr[0] = ErrStorage
	backend.fetches[1] = scannerFetch{records: encodedRows(t, table.Schema, 5, 1, 2), highWatermark: 8}
	base, err := newLogScanner(ctx, backend, table, AtOffset(5), WithScanRowLimit(2))
	if err != nil {
		t.Fatal(err)
	}
	var attempts []int32
	fail := true
	scanner, err := wrapTypedLogScanner(base, typedIDCodec(func(id int32) error {
		attempts = append(attempts, id)
		if id == 2 && fail {
			return errTypedCodec
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	for range 2 {
		result, err := scanner.Poll(ctx)
		if !errors.Is(err, errTypedCodec) || len(result.Records) != 0 || result.Done || scanner.Done() {
			t.Fatalf("failed typed poll = %+v, %v; done=%v", result, err, scanner.Done())
		}
	}
	fail = false
	result, err := scanner.Poll(ctx)
	if err != nil || !result.Done || !scanner.Done() || len(result.Records) != 2 ||
		result.Records[0].Value != 1 || result.Records[1].Value != 2 ||
		result.Records[0].Offset != 5 || result.Records[1].Offset != 6 || result.HighWatermark[1] != 8 ||
		len(result.BucketErrors) != 1 || result.BucketErrors[0].Bucket != 0 || !errors.Is(result.BucketErrors[0].Err, ErrStorage) {
		t.Fatalf("recovered result = %+v, %v; done=%v", result, err, scanner.Done())
	}
	if got := backend.fetchCalls(); len(got) != 2 || !slices.Equal(attempts, []int32{1, 2, 1, 2, 1, 2}) {
		t.Fatalf("fetches=%+v decode attempts=%v", got, attempts)
	}
	result, err = scanner.Poll(ctx)
	if err != nil || !result.Done || len(result.Records) != 0 {
		t.Fatalf("after delivery = %+v, %v", result, err)
	}
}

func TestTypedScansDoNotSkipUnsupportedArrow(t *testing.T) {
	ctx := t.Context()
	table := appendWriterTable()
	encoded := encodedArrowRows(t, table.Schema, 5, 1)
	codec := typedIDCodec(func(int32) error {
		t.Fatal("application codec was called for an Arrow batch")
		return nil
	})
	logBackend := scannerBackend(0)
	logBackend.fetches[0] = scannerFetch{records: encoded, highWatermark: 6}
	logScanner, err := newLogScanner(ctx, logBackend, table, AtOffset(5), WithScanRowLimit(1))
	if err != nil {
		t.Fatal(err)
	}
	typedLog, err := wrapTypedLogScanner(logScanner, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer typedLog.Close()
	batchCalls := 0
	batchBackend := batchScanBackendFunc(func(context.Context, TableBucket, int32) (bool, []byte, error) {
		batchCalls++
		return true, encoded, nil
	})
	batchScanner, err := newBatchScanner(ctx, batchBackend, nil, table, testTableBucket(table))
	if err != nil {
		t.Fatal(err)
	}
	typedBatch, err := WrapTypedBatchScanner(batchScanner, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer typedBatch.Close()
	for range 2 {
		if _, err := typedLog.Poll(ctx); !errors.Is(err, ErrUnsupportedAPI) || typedLog.Done() {
			t.Errorf("typed log after Arrow = %v; done=%v", err, typedLog.Done())
		}
		if _, err := typedBatch.Poll(ctx); !errors.Is(err, ErrUnsupportedAPI) || typedBatch.Done() {
			t.Errorf("typed batch after Arrow = %v; done=%v", err, typedBatch.Done())
		}
	}
	if len(logBackend.fetchCalls()) != 1 || batchCalls != 1 {
		t.Error("an unsupported Arrow batch was fetched again")
	}
}

func TestTypedKVRetainsNonterminalPageWithoutReplayingSequence(t *testing.T) {
	ctx := t.Context()
	table := upsertWriterTable()
	backend := &fakeKVSessionBackend{batches: []kvSessionBatch{
		{scannerID: []byte("scan"), hasMore: true, records: encodedValueBatch(t, table,
			Row{int32(1), "one", nil}, Row{int32(2), "two", nil})},
		{scannerID: []byte("scan"), records: encodedValueBatch(t, table, Row{int32(3), "three", nil})},
	}}
	base, err := newBatchScanner(ctx, backend, nil, table, testTableBucket(table))
	if err != nil {
		t.Fatal(err)
	}
	fail := true
	scanner, err := WrapTypedBatchScanner(base, typedIDCodec(func(id int32) error {
		if id == 2 && fail {
			return errTypedCodec
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	if result, err := scanner.Poll(ctx); !errors.Is(err, errTypedCodec) || len(result.Values) != 0 || scanner.Done() {
		t.Fatalf("failed typed page = %+v, %v", result, err)
	}
	fail = false
	if result, err := scanner.Poll(ctx); err != nil || result.Done || !slices.Equal(result.Values, []int32{1, 2}) {
		t.Fatalf("retried page = %+v, %v", result, err)
	}
	if len(backend.calls) != 1 {
		t.Fatalf("retry made another request: %+v", backend.calls)
	}
	if result, err := scanner.Poll(ctx); err != nil || !result.Done || !slices.Equal(result.Values, []int32{3}) || !scanner.Done() {
		t.Fatalf("next page = %+v, %v", result, err)
	}
	if len(backend.calls) != 2 || backend.calls[0].sequence != 0 || backend.calls[1].sequence != 1 ||
		string(backend.calls[1].scannerID) != "scan" {
		t.Fatalf("KV scans = %+v", backend.calls)
	}
}

func TestTypedSnapshotRetainsFinalRowsAfterCodecFailure(t *testing.T) {
	reader := &fakeSnapshotBatchReader{
		batches:     [][]Row{{{int32(1), "one", nil}, {int32(2), "two", nil}}},
		eofWithLast: true,
	}
	fail := true
	scanner := newTypedSnapshotFixture(t, reader, typedIDCodec(func(id int32) error {
		if id == 2 && fail {
			return errTypedCodec
		}
		return nil
	}))
	defer scanner.Close()
	first, err := scanner.Poll(t.Context())
	if !errors.Is(err, errTypedCodec) || len(first.Values) != 0 || first.Done || scanner.Done() {
		t.Fatalf("failed terminal snapshot = %+v, %v; done=%v", first, err, scanner.Done())
	}
	fail = false
	last, err := scanner.Poll(t.Context())
	if err != nil || !last.Done || !scanner.Done() || !slices.Equal(last.Values, []int32{1, 2}) {
		t.Fatalf("retried terminal snapshot = %+v, %v; done=%v", last, err, scanner.Done())
	}
}

func TestTypedScanCanceledConversionRetainsPage(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reader := &fakeSnapshotBatchReader{
		batches: [][]Row{{{int32(7), "row", nil}}}, eofWithLast: true,
	}
	scanner := newTypedSnapshotFixture(t, reader, typedIDCodec(func(int32) error {
		cancel()
		return nil
	}))
	defer scanner.Close()
	if result, err := scanner.Poll(ctx); !errors.Is(err, context.Canceled) || len(result.Values) != 0 || scanner.Done() {
		t.Fatalf("canceled conversion = %+v, %v", result, err)
	}
	if result, err := scanner.Poll(t.Context()); err != nil || !result.Done || !slices.Equal(result.Values, []int32{7}) {
		t.Fatalf("retry after cancellation = %+v, %v", result, err)
	}
}

func TestTypedScanQueuedPollAndCloseDuringDecode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		reader := &fakeSnapshotBatchReader{
			batches: [][]Row{{{int32(7), "row", nil}}}, eofWithLast: true,
		}
		entered, finish := make(chan struct{}), make(chan struct{})
		scanner := newTypedSnapshotFixture(t, reader, typedIDCodec(func(int32) error {
			close(entered)
			<-finish
			return nil
		}))
		active, queued := make(chan error, 1), make(chan error, 1)
		go func() { _, err := scanner.Poll(ctx); active <- err }()
		<-entered
		if scanner.Done() {
			t.Fatal("active codec must not publish terminal progress")
		}
		deadline, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		if _, err := scanner.Poll(deadline); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued poll with deadline = %v", err)
		}
		go func() { _, err := scanner.Poll(ctx); queued <- err }()
		synctest.Wait()
		if err := scanner.Close(); err != nil || reader.closed.Load() != 1 {
			t.Fatalf("close = %v; snapshot closes=%d", err, reader.closed.Load())
		}
		if err := <-queued; !errors.Is(err, ErrClosed) {
			t.Errorf("queued poll on close = %v", err)
		}
		close(finish)
		if err := <-active; !errors.Is(err, ErrClosed) {
			t.Errorf("active poll on close = %v", err)
		}
		if _, err := scanner.Poll(ctx); !errors.Is(err, ErrClosed) || scanner.Done() {
			t.Errorf("closed scanner = %v; done=%v", err, scanner.Done())
		}
		scanner.delivery.mu.Lock()
		defer scanner.delivery.mu.Unlock()
		if scanner.delivery.retained || scanner.delivery.pending.Rows != nil {
			t.Error("close did not discard the retained page")
		}
	})
}

func newTypedSnapshotFixture(t *testing.T, reader SnapshotBatchReader, codec Codec[int32]) *TypedBatchScanner[int32] {
	t.Helper()
	table := upsertWriterTable()
	base, err := newBatchScanner(t.Context(), nil, reader, table, testTableBucket(table))
	if err != nil {
		t.Fatal(err)
	}
	scanner, err := WrapTypedBatchScanner(base, codec)
	if err != nil {
		t.Fatal(err)
	}
	return scanner
}
