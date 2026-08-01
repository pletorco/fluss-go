package fgo

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type scannerFetchCall struct {
	bucket      int32
	offset      int64
	tableID     int64
	partitionID int64
	projection  []int32
	config      LogScannerConfig
}

type fakeLogScannerBackend struct {
	mu          sync.Mutex
	physicalID  int64
	locations   map[int32]Node
	metadataErr error
	listOffsets map[int32]int64
	listErr     error
	fetches     map[int32]scannerFetch
	fetchErr    map[int32]error
	block       <-chan struct{}
	entered     chan struct{}
	enterOnce   sync.Once
	calls       []scannerFetchCall
}

func (b *fakeLogScannerBackend) metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error) {
	return b.physicalID, b.locations, b.metadataErr
}

func (b *fakeLogScannerBackend) listOffset(
	_ context.Context,
	_ PhysicalTablePath,
	bucket int32,
	_ int64,
	_ int64,
	_ ScanOffset,
) (int64, error) {
	if b.listErr != nil {
		return 0, b.listErr
	}
	return b.listOffsets[bucket], nil
}

func (b *fakeLogScannerBackend) fetch(
	ctx context.Context,
	input logFetchRequest,
) (scannerFetch, error) {
	b.mu.Lock()
	b.calls = append(b.calls, scannerFetchCall{
		bucket: input.bucket, offset: input.offset, tableID: input.tableID, partitionID: input.partitionID,
		projection: append([]int32(nil), input.projection...), config: input.config,
	})
	b.mu.Unlock()
	if b.entered != nil {
		b.enterOnce.Do(func() { close(b.entered) })
	}
	if b.block != nil {
		select {
		case <-b.block:
		case <-ctx.Done():
			return scannerFetch{}, ctx.Err()
		}
	}
	return b.fetches[input.bucket], b.fetchErr[input.bucket]
}

func (b *fakeLogScannerBackend) fetchCalls() []scannerFetchCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]scannerFetchCall(nil), b.calls...)
}

func scannerBackend(bucketIDs ...int32) *fakeLogScannerBackend {
	locations := make(map[int32]Node, len(bucketIDs))
	for _, bucket := range bucketIDs {
		locations[bucket] = Node{ID: bucket + 10, Address: "tablet", Role: TabletServer}
	}
	return &fakeLogScannerBackend{
		physicalID: 9, locations: locations, listOffsets: map[int32]int64{0: 5},
		fetches: make(map[int32]scannerFetch), fetchErr: make(map[int32]error),
	}
}

func encodedRows(t *testing.T, schema Schema, base int64, values ...int32) []byte {
	t.Helper()
	batch := LogBatch{Magic: 0, BaseOffset: base, SchemaID: 3, AppendOnly: true}
	for _, value := range values {
		batch.Records = append(batch.Records, Record{Value: Row{value, "row"}, Change: Append})
	}
	encoded, err := batch.EncodeRows(schema, true)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func encodedArrowRows(t *testing.T, schema Schema, base int64, values ...int32) []byte {
	t.Helper()
	arrowSchema, err := schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	changes := make([]ChangeType, len(values))
	for index, value := range values {
		builder.Field(0).(*array.Int32Builder).Append(value)
		builder.Field(1).(*array.StringBuilder).Append("arrow")
		changes[index] = ChangeType(index%4 + 1)
	}
	record := builder.NewRecordBatch()
	builder.Release()
	encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, BaseOffset: base, SchemaID: 3, Record: record, Changes: changes,
	}, ArrowCompressionNone, memory.DefaultAllocator)
	record.Release()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func requireRecordOffsets(t *testing.T, records []ScanRecord, want ...int64) {
	t.Helper()
	if len(records) != len(want) {
		t.Fatalf("record count = %d, want %d: %#v", len(records), len(want), records)
	}
	for index := range want {
		if records[index].Record.Offset != want[index] {
			t.Fatalf("record %d offset = %d, want %d", index, records[index].Record.Offset, want[index])
		}
	}
}

func TestDecodeFetchedRowsTrimsBeforeRequestedOffset(t *testing.T) {
	table := logWriterTable()
	encoded := encodedRows(t, table.Schema, 5, 10, 11, 12)
	for _, test := range []struct {
		name    string
		current int64
		next    int64
		offsets []int64
	}{
		{name: "before", current: 4, next: 8, offsets: []int64{5, 6, 7}},
		{name: "at base", current: 5, next: 8, offsets: []int64{5, 6, 7}},
		{name: "inside", current: 6, next: 8, offsets: []int64{6, 7}},
		{name: "at end", current: 8, next: 8},
		{name: "after", current: 9, next: 9},
	} {
		t.Run(test.name, func(t *testing.T) {
			next, rows, arrows, err := decodeFetchedLog(table.Schema, 0, test.current, encoded)
			if err != nil || next != test.next || len(arrows) != 0 {
				t.Fatalf("decodeFetchedLog() = next %d, rows %#v, arrows %#v, %v", next, rows, arrows, err)
			}
			requireRecordOffsets(t, rows, test.offsets...)
		})
	}
}

func TestDecodeFetchedRowsTrimsMultipleBatchesAndPreservesOffset(t *testing.T) {
	table := logWriterTable()
	first := encodedRows(t, table.Schema, 4, 1, 2)
	second := encodedRows(t, table.Schema, 6, 3, 4, 5)
	next, rows, arrows, err := decodeFetchedLog(table.Schema, 0, 5, append(first, second...))
	if err != nil || next != 9 || len(arrows) != 0 {
		t.Fatalf("decodeFetchedLog() = next %d, rows %#v, arrows %#v, %v", next, rows, arrows, err)
	}
	requireRecordOffsets(t, rows, 5, 6, 7, 8)
	overlapping := append(
		encodedRows(t, table.Schema, 5, 1, 2),
		encodedRows(t, table.Schema, 6, 3, 4)...,
	)
	next, rows, arrows, err = decodeFetchedLog(table.Schema, 0, 5, overlapping)
	if err != nil || next != 8 || len(arrows) != 0 {
		t.Fatalf("overlapping decodeFetchedLog() = next %d, rows %#v, arrows %#v, %v", next, rows, arrows, err)
	}
	requireRecordOffsets(t, rows, 5, 6, 7)

	empty := encodedRows(t, table.Schema, 100)
	next, rows, arrows, err = decodeFetchedLog(table.Schema, 0, 9, empty)
	if err != nil || next != 9 || len(rows) != 0 || len(arrows) != 0 {
		t.Fatalf("empty decodeFetchedLog() = next %d, rows %#v, arrows %#v, %v", next, rows, arrows, err)
	}

	overflow := encodedRows(t, table.Schema, int64(^uint64(0)>>1), 1, 2)
	if _, _, _, err = decodeFetchedLog(table.Schema, 0, 0, overflow); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("overflow decodeFetchedLog() error = %v", err)
	}
	if _, _, err = fetchedBatchOffsets(0, -1, 0); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("negative count error = %v", err)
	}
}

func TestDecodeFetchedArrowTrimsBeforeRequestedOffset(t *testing.T) {
	table := logWriterTable()
	encoded := encodedArrowRows(t, table.Schema, 5, 10, 11, 12)
	for _, test := range []struct {
		name        string
		current     int64
		next        int64
		base        int64
		rows        int64
		firstValue  int32
		firstChange ChangeType
	}{
		{name: "before", current: 4, next: 8, base: 5, rows: 3, firstValue: 10, firstChange: Insert},
		{name: "at base", current: 5, next: 8, base: 5, rows: 3, firstValue: 10, firstChange: Insert},
		{name: "inside", current: 6, next: 8, base: 6, rows: 2, firstValue: 11, firstChange: UpdateBefore},
		{name: "at end", current: 8, next: 8},
		{name: "after", current: 9, next: 9},
	} {
		t.Run(test.name, func(t *testing.T) {
			next, rows, arrows, err := decodeFetchedLog(table.Schema, 0, test.current, encoded)
			if err != nil || next != test.next || len(rows) != 0 {
				t.Fatalf("decodeFetchedLog() = next %d, rows %#v, arrows %#v, %v", next, rows, arrows, err)
			}
			defer releaseScanArrows(arrows)
			if test.rows == 0 {
				if len(arrows) != 0 {
					t.Fatalf("Arrow batches = %#v, want none", arrows)
				}
				return
			}
			if len(arrows) != 1 || arrows[0].Batch.BaseOffset != test.base ||
				arrows[0].Batch.Record.NumRows() != test.rows || len(arrows[0].Batch.Changes) != int(test.rows) {
				t.Fatalf("Arrow batches = %#v", arrows)
			}
			values := arrows[0].Batch.Record.Column(0).(*array.Int32)
			if values.Value(0) != test.firstValue || arrows[0].Batch.Changes[0] != test.firstChange {
				t.Fatalf("first Arrow row = %d/%v", values.Value(0), arrows[0].Batch.Changes[0])
			}
		})
	}
}

func TestDecodeFetchedArrowTrimsMultipleBatchesAndRejectsOverflow(t *testing.T) {
	table := logWriterTable()
	first := encodedArrowRows(t, table.Schema, 4, 10, 11)
	second := encodedArrowRows(t, table.Schema, 6, 12, 13, 14)
	next, rows, arrows, err := decodeFetchedLog(table.Schema, 0, 5, append(first, second...))
	if err != nil || next != 9 || len(rows) != 0 || len(arrows) != 2 {
		t.Fatalf("decodeFetchedLog() = next %d, rows %#v, arrows %#v, %v", next, rows, arrows, err)
	}
	defer releaseScanArrows(arrows)
	if arrows[0].Batch.BaseOffset != 5 || arrows[0].Batch.Record.NumRows() != 1 ||
		arrows[1].Batch.BaseOffset != 6 || arrows[1].Batch.Record.NumRows() != 3 {
		t.Fatalf("Arrow batches = %#v", arrows)
	}
	firstValues := arrows[0].Batch.Record.Column(0).(*array.Int32)
	if firstValues.Value(0) != 11 || arrows[0].Batch.Changes[0] != UpdateBefore {
		t.Fatalf("first retained Arrow row = %d/%v", firstValues.Value(0), arrows[0].Batch.Changes[0])
	}

	overflow := encodedArrowRows(t, table.Schema, int64(^uint64(0)>>1), 1, 2)
	if _, _, _, err = decodeFetchedLog(table.Schema, 0, 0, overflow); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("overflow decodeFetchedLog() error = %v", err)
	}
}

func TestLogScannerIgnoresIncompleteTrailingRowBatch(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	first := encodedRows(t, table.Schema, 4, 1, 2)
	second := encodedRows(t, table.Schema, 6, 3, 4, 5)
	backend.fetches[0] = scannerFetch{records: append(append([]byte(nil), first...), second[:len(second)-1]...)}

	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(4))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scanner.Close() }()

	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.BucketErrors) != 0 {
		t.Fatalf("first Poll() = %#v, %v", result, err)
	}
	requireRecordOffsets(t, result.Records, 4, 5)
	result.Release()

	backend.fetches[0] = scannerFetch{records: second}
	result, err = scanner.Poll(context.Background())
	if err != nil || len(result.BucketErrors) != 0 {
		t.Fatalf("second Poll() = %#v, %v", result, err)
	}
	requireRecordOffsets(t, result.Records, 6, 7, 8)
	result.Release()
	calls := backend.fetchCalls()
	if len(calls) != 2 || calls[1].offset != 6 {
		t.Fatalf("fetch calls = %#v", calls)
	}
}

func TestLogScannerIgnoresIncompleteTrailingArrowBatch(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	first := encodedArrowRows(t, table.Schema, 10, 1, 2)
	second := encodedArrowRows(t, table.Schema, 12, 3, 4, 5)
	backend.fetches[0] = scannerFetch{records: append(append([]byte(nil), first...), second[:len(second)-1]...)}

	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(10))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scanner.Close() }()

	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.BucketErrors) != 0 || len(result.ArrowBatches) != 1 {
		t.Fatalf("first Poll() = %#v, %v", result, err)
	}
	batch := result.ArrowBatches[0].Batch
	if batch.BaseOffset != 10 || batch.Record.NumRows() != 2 {
		t.Fatalf("first Arrow batch = %#v", batch)
	}
	result.Release()

	backend.fetches[0] = scannerFetch{records: second}
	result, err = scanner.Poll(context.Background())
	if err != nil || len(result.BucketErrors) != 0 || len(result.ArrowBatches) != 1 {
		t.Fatalf("second Poll() = %#v, %v", result, err)
	}
	batch = result.ArrowBatches[0].Batch
	if batch.BaseOffset != 12 || batch.Record.NumRows() != 3 {
		t.Fatalf("second Arrow batch = %#v", batch)
	}
	result.Release()
	calls := backend.fetchCalls()
	if len(calls) != 2 || calls[1].offset != 12 {
		t.Fatalf("fetch calls = %#v", calls)
	}
}

func TestLogScannerRetriesIncompleteFirstBatchWithoutAdvancing(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	complete := encodedRows(t, table.Schema, 20, 1, 2)
	backend.fetches[0] = scannerFetch{records: complete[:len(complete)-1]}

	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(20))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scanner.Close() }()

	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.BucketErrors) != 0 || len(result.Records) != 0 || len(result.ArrowBatches) != 0 {
		t.Fatalf("incomplete Poll() = %#v, %v", result, err)
	}
	result.Release()
	backend.fetches[0] = scannerFetch{records: complete}

	result, err = scanner.Poll(context.Background())
	if err != nil || len(result.BucketErrors) != 0 {
		t.Fatalf("retry Poll() = %#v, %v", result, err)
	}
	requireRecordOffsets(t, result.Records, 20, 21)
	result.Release()
	calls := backend.fetchCalls()
	if len(calls) != 2 || calls[0].offset != 20 || calls[1].offset != 20 {
		t.Fatalf("fetch calls = %#v", calls)
	}
}

func TestLogScannerAcceptsFirstArrowBatchLargerThanBucketFetchLimit(t *testing.T) {
	table := logWriterTable()
	arrowSchema, err := table.Schema.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	value := strings.Repeat("x", 600<<10)
	for id := int32(1); id <= 2; id++ {
		builder.Field(0).(*array.Int32Builder).Append(id)
		builder.Field(1).(*array.StringBuilder).Append(value)
	}
	record := builder.NewRecordBatch()
	builder.Release()
	encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, BaseOffset: 30, SchemaID: 3, Record: record,
		Changes: []ChangeType{Append, Append},
	}, ArrowCompressionNone, memory.DefaultAllocator)
	record.Release()
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) <= 1<<20 {
		t.Fatalf("encoded Arrow batch size = %d, want larger than 1 MiB", len(encoded))
	}

	backend := scannerBackend(0)
	backend.fetches[0] = scannerFetch{records: encoded}
	scanner, err := newLogScanner(
		context.Background(), backend, table, AtOffset(30), WithScanRowLimit(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scanner.Close() }()

	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.BucketErrors) != 0 || !result.Done || len(result.ArrowBatches) != 1 ||
		result.ArrowBatches[0].Batch.Record.NumRows() != 1 {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	result.Release()
	calls := backend.fetchCalls()
	if len(calls) != 1 || calls[0].config.MaxBucketBytes != 1<<20 {
		t.Fatalf("fetch calls = %#v", calls)
	}
}

func TestLogScannerRejectsInvalidCompleteBatchHeader(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	backend.fetches[0] = scannerFetch{records: make([]byte, 12)}
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scanner.Close() }()

	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.BucketErrors) != 1 ||
		!errors.Is(result.BucketErrors[0].Err, ErrMalformedRecordBatch) {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
}

func TestLogScannerAppliesBoundsAfterTrimmingRows(t *testing.T) {
	table := logWriterTable()
	for _, test := range []struct {
		name   string
		option LogScannerOption
	}{
		{name: "row limit", option: WithScanRowLimit(2)},
		{name: "stopping offset", option: WithScanStoppingOffsets(map[int32]int64{0: 7})},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := scannerBackend(0)
			backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 4, 1, 2, 3, 4)}
			scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(5), test.option)
			if err != nil {
				t.Fatal(err)
			}
			result, err := scanner.Poll(context.Background())
			if err != nil || !result.Done {
				t.Fatalf("Poll() = %#v, %v", result, err)
			}
			requireRecordOffsets(t, result.Records, 5, 6)
			if scanner.offset[0] != 7 || scanner.delivered != 2 {
				t.Fatalf("scanner offset/delivered = %d/%d", scanner.offset[0], scanner.delivered)
			}
			result.Release()
			_ = scanner.Close()
		})
	}
}

func TestLogScannerAppliesBoundsAfterTrimmingArrow(t *testing.T) {
	table := logWriterTable()
	for _, test := range []struct {
		name   string
		option LogScannerOption
	}{
		{name: "row limit", option: WithScanRowLimit(2)},
		{name: "stopping offset", option: WithScanStoppingOffsets(map[int32]int64{0: 13})},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := scannerBackend(0)
			backend.fetches[0] = scannerFetch{records: encodedArrowRows(t, table.Schema, 10, 1, 2, 3, 4)}
			scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(11), test.option)
			if err != nil {
				t.Fatal(err)
			}
			result, err := scanner.Poll(context.Background())
			if err != nil || !result.Done || len(result.ArrowBatches) != 1 {
				t.Fatalf("Poll() = %#v, %v", result, err)
			}
			batch := result.ArrowBatches[0].Batch
			values := batch.Record.Column(0).(*array.Int32)
			if batch.BaseOffset != 11 || batch.Record.NumRows() != 2 || len(batch.Changes) != 2 ||
				values.Value(0) != 2 || batch.Changes[0] != UpdateBefore ||
				scanner.offset[0] != 13 || scanner.delivered != 2 {
				t.Fatalf("bounded Arrow batch/scanner = %#v, %d/%d", batch, scanner.offset[0], scanner.delivered)
			}
			result.Release()
			result.Release()
			_ = scanner.Close()
		})
	}
}

func TestLogScannerTimestampTrimsPerBucketRows(t *testing.T) {
	table := logWriterTable()
	table.BucketCount = 2
	backend := scannerBackend(0, 1)
	backend.listOffsets = map[int32]int64{0: 5, 1: 8}
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 4, 1, 2, 3)}
	backend.fetches[1] = scannerFetch{records: encodedRows(t, table.Schema, 7, 4, 5, 6)}
	scanner, err := newLogScanner(context.Background(), backend, table, AtTimestamp(time.UnixMilli(1234)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 4 || result.Records[0].Bucket != 0 || result.Records[1].Bucket != 0 ||
		result.Records[2].Bucket != 1 || result.Records[3].Bucket != 1 {
		t.Fatalf("timestamp records = %#v", result.Records)
	}
	requireRecordOffsets(t, result.Records, 5, 6, 8, 9)
	calls := backend.fetchCalls()
	if len(calls) != 2 || calls[0].offset != 5 || calls[1].offset != 8 {
		t.Fatalf("timestamp fetch calls = %#v", calls)
	}
	result.Release()
	_ = scanner.Close()
}

func TestLogScannerTimestampTrimsArrowBatch(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	backend.listOffsets[0] = 12
	backend.fetches[0] = scannerFetch{records: encodedArrowRows(t, table.Schema, 10, 1, 2, 3, 4)}
	scanner, err := newLogScanner(context.Background(), backend, table, AtTimestamp(time.UnixMilli(1234)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.ArrowBatches) != 1 {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	batch := result.ArrowBatches[0].Batch
	values := batch.Record.Column(0).(*array.Int32)
	if batch.BaseOffset != 12 || batch.Record.NumRows() != 2 || values.Value(0) != 3 ||
		backend.fetchCalls()[0].offset != 12 {
		t.Fatalf("timestamp Arrow batch = %#v, calls %#v", batch, backend.fetchCalls())
	}
	result.Release()
	_ = scanner.Close()
}

func TestLogScannerDoesNotRegressAfterEntirelyOldBatch(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 4, 1, 2)}
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(8))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Records) != 0 || scanner.offset[0] != 8 {
		t.Fatalf("old batch Poll() = %#v, offset %d, %v", result, scanner.offset[0], err)
	}
	result.Release()

	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 8, 3)}
	result, err = scanner.Poll(context.Background())
	if err != nil || scanner.offset[0] != 9 {
		t.Fatalf("recovery Poll() = %#v, offset %d, %v", result, scanner.offset[0], err)
	}
	requireRecordOffsets(t, result.Records, 8)
	calls := backend.fetchCalls()
	if len(calls) != 2 || calls[0].offset != 8 || calls[1].offset != 8 {
		t.Fatalf("fetch calls = %#v", calls)
	}
	result.Release()
	_ = scanner.Close()
}

func TestLogScannerStoppingOffsetsReopenAfterSubscribe(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 5, 1, 2)}
	scanner, err := newLogScanner(
		context.Background(), backend, table, AtOffset(5),
		WithScanStoppingOffsets(map[int32]int64{0: 7}),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done {
		t.Fatalf("initial Poll() = %#v, %v", result, err)
	}
	result.Release()
	if err := scanner.Subscribe(context.Background(), 0, AtOffset(5)); err != nil || scanner.Done() {
		t.Fatalf("Subscribe() = done %v, %v", scanner.Done(), err)
	}
	result, err = scanner.Poll(context.Background())
	if err != nil || !result.Done {
		t.Fatalf("resubscribed Poll() = %#v, %v", result, err)
	}
	requireRecordOffsets(t, result.Records, 5, 6)
	result.Release()
	_ = scanner.Close()
}

func TestLogScannerRowLimitRemainsDoneAfterSubscribe(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 5, 1)}
	scanner, err := newLogScanner(
		context.Background(), backend, table, AtOffset(5), WithScanRowLimit(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	result.Release()
	if err := scanner.Subscribe(context.Background(), 0, AtOffset(5)); err != nil || !scanner.Done() {
		t.Fatalf("Subscribe() = done %v, %v", scanner.Done(), err)
	}
	_ = scanner.Close()
}

func TestLogScannerStoppingOffsetsIgnoreUnsubscribedBuckets(t *testing.T) {
	table := logWriterTable()
	table.BucketCount = 2
	backend := scannerBackend(0, 1)
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 5, 1, 2)}
	scanner, err := newLogScanner(
		context.Background(), backend, table, AtOffset(5),
		WithScanStoppingOffsets(map[int32]int64{0: 7, 1: 7}),
	)
	if err != nil {
		t.Fatal(err)
	}
	scanner.Unsubscribe(1)
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || len(backend.fetchCalls()) != 1 {
		t.Fatalf("Poll() = %#v, %v, calls %#v", result, err, backend.fetchCalls())
	}
	requireRecordOffsets(t, result.Records, 5, 6)
	result.Release()
	_ = scanner.Close()
}

func TestLogScannerPollPreservesBucketOrderAndPartialErrors(t *testing.T) {
	table := logWriterTable()
	table.BucketCount = 2
	backend := scannerBackend(1, 0)
	first := encodedRows(t, table.Schema, 5, 1, 2)
	second := encodedRows(t, table.Schema, 7, 3)
	backend.fetches[0] = scannerFetch{records: append(first, second...), highWatermark: 20}
	backend.fetchErr[1] = errors.New("bucket unavailable")
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(5))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if len(result.Records) != 3 || result.Records[0].Bucket != 0 ||
		result.Records[0].Record.Offset != 5 || result.Records[2].Record.Offset != 7 {
		t.Fatalf("records = %#v", result.Records)
	}
	if len(result.BucketErrors) != 1 || result.BucketErrors[0].Bucket != 1 {
		t.Fatalf("bucket errors = %#v", result.BucketErrors)
	}
	if result.HighWatermark[0] != 20 {
		t.Fatalf("high watermark = %#v", result.HighWatermark)
	}
	backend.fetches[0] = scannerFetch{}
	_, _ = scanner.Poll(context.Background())
	calls := backend.fetchCalls()
	if calls[2].bucket != 0 || calls[2].offset != 8 {
		t.Fatalf("second poll call = %#v", calls[2])
	}
	_ = scanner.Close()
}

func TestLogScannerRowLimitAndStoppingOffsets(t *testing.T) {
	table := logWriterTable()
	table.BucketCount = 2
	backend := scannerBackend(0, 1)
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 5, 1, 2, 3)}
	backend.fetches[1] = scannerFetch{records: encodedRows(t, table.Schema, 5, 4, 5)}
	scanner, err := newLogScanner(
		context.Background(), backend, table, AtOffset(5),
		WithScanStoppingOffsets(map[int32]int64{0: 7, 1: 6}),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || !scanner.Done() || len(result.Records) != 3 ||
		result.Records[0].Record.Offset != 5 || result.Records[1].Record.Offset != 6 ||
		result.Records[2].Record.Offset != 5 {
		t.Fatalf("bounded Poll() = %#v, %v", result, err)
	}
	result.Release()
	calls := len(backend.fetchCalls())
	repeated, err := scanner.Poll(context.Background())
	if err != nil || !repeated.Done || len(repeated.Records) != 0 ||
		len(backend.fetchCalls()) != calls {
		t.Fatalf("terminal Poll() = %#v, %v, calls=%d", repeated, err, len(backend.fetchCalls()))
	}
	_ = scanner.Close()

	backend = scannerBackend(0, 1)
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 0, 1, 2, 3)}
	backend.fetches[1] = scannerFetch{records: encodedRows(t, table.Schema, 0, 4)}
	scanner, err = newLogScanner(context.Background(), backend, table, AtOffset(0), WithScanRowLimit(2))
	if err != nil {
		t.Fatal(err)
	}
	result, err = scanner.Poll(context.Background())
	if err != nil || !result.Done || len(result.Records) != 2 || len(backend.fetchCalls()) != 1 {
		t.Fatalf("limited Poll() = %#v, %v, calls=%#v", result, err, backend.fetchCalls())
	}
	result.Release()
	_ = scanner.Close()

	backend = scannerBackend(0)
	scanner, err = newLogScanner(
		context.Background(), backend, table, AtOffset(5),
		WithScanStoppingOffsets(map[int32]int64{0: 5}),
	)
	if err != nil || !scanner.Done() {
		t.Fatalf("initially complete scanner = %#v, %v", scanner, err)
	}
	result, err = scanner.Poll(context.Background())
	if err != nil || !result.Done || len(backend.fetchCalls()) != 0 {
		t.Fatalf("initial terminal Poll() = %#v, %v", result, err)
	}
	_ = scanner.Close()
}

func TestLogScannerProjectionAndOffsetInitialization(t *testing.T) {
	table := logWriterTable()
	table.BucketCount = 1
	backend := scannerBackend(0)
	backend.listOffsets[0] = 12
	projected := Schema{Columns: []Column{{Name: "name", Type: StringType, Nullable: true}}}
	encoded, err := (LogBatch{
		Magic: 0, BaseOffset: 11, SchemaID: 3, AppendOnly: true,
		Records: []Record{
			{Value: Row{"skipped"}, Change: Append},
			{Value: Row{"projected"}, Change: Append},
		},
	}).EncodeRows(projected, true)
	if err != nil {
		t.Fatal(err)
	}
	backend.fetches[0] = scannerFetch{records: encoded}
	scanner, err := newLogScanner(
		context.Background(), backend, table, Earliest(),
		WithScanProjection("name"), WithScanLimits(4096, 2048, 1, time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanner.Schema(); len(got.Columns) != 1 || got.Columns[0].Name != "name" {
		t.Fatalf("projected schema = %#v", got)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Records) != 1 || result.Records[0].Record.Value[0] != "projected" {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	call := backend.fetchCalls()[0]
	if call.offset != 12 || len(call.projection) != 1 || call.projection[0] != 1 ||
		call.config.MaxBytes != 4096 {
		t.Fatalf("projected fetch = %#v", call)
	}
	result.Release()
	_ = scanner.Close()
}

func TestLogScannerDecodesIndexedTable(t *testing.T) {
	table := logWriterTable()
	table.Properties = map[string]string{"table.log.format": "INDEXED"}
	backend := scannerBackend(0)
	encoded, err := (LogBatch{
		Magic: 0, BaseOffset: 3, SchemaID: int16(table.SchemaID), AppendOnly: true,
		Records: []Record{{Value: Row{int32(7), "indexed"}, Change: Append}},
	}).EncodeRows(table.Schema, false)
	if err != nil {
		t.Fatal(err)
	}
	backend.fetches[0] = scannerFetch{records: encoded}
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(3))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Records) != 1 || result.Records[0].Record.Value[1] != "indexed" {
		t.Fatalf("indexed Poll() = %#v, %v", result, err)
	}
	result.Release()
	_ = scanner.Close()

	if _, err := newLogScanner(
		context.Background(), scannerBackend(0), table, AtOffset(0),
		WithScanProjection("name"),
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("indexed projection error = %v", err)
	}
}

func TestLogScannerArrowBatchOwnership(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil))
	builder.Field(0).(*array.Int32Builder).Append(9)
	builder.Field(1).(*array.StringBuilder).Append("arrow")
	record := builder.NewRecordBatch()
	builder.Release()
	encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, BaseOffset: 30, SchemaID: 3, Record: record, Changes: []ChangeType{Append},
	}, ArrowCompressionZSTD, memory.DefaultAllocator)
	record.Release()
	if err != nil {
		t.Fatal(err)
	}
	backend.fetches[0] = scannerFetch{records: encoded, highWatermark: 31}
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(30))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.ArrowBatches) != 1 || result.ArrowBatches[0].Batch.Record.NumRows() != 1 {
		t.Fatalf("Arrow Poll() = %#v, %v", result, err)
	}
	result.Release()
	result.Release()
	backend.fetches[0] = scannerFetch{}
	_, _ = scanner.Poll(context.Background())
	if calls := backend.fetchCalls(); calls[1].offset != 31 {
		t.Fatalf("next Arrow offset = %d", calls[1].offset)
	}
	_ = scanner.Close()
}

func TestLogScannerSlicesArrowBatchAtLimit(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil))
	for value := int32(1); value <= 3; value++ {
		builder.Field(0).(*array.Int32Builder).Append(value)
		builder.Field(1).(*array.StringBuilder).Append("arrow")
	}
	record := builder.NewRecordBatch()
	builder.Release()
	encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
		Magic: 0, BaseOffset: 10, SchemaID: 3, Record: record,
		Changes: []ChangeType{Append, Append, Append},
	}, ArrowCompressionNone, memory.DefaultAllocator)
	record.Release()
	if err != nil {
		t.Fatal(err)
	}
	backend.fetches[0] = scannerFetch{records: encoded}
	scanner, err := newLogScanner(
		context.Background(), backend, table, AtOffset(10), WithScanRowLimit(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || len(result.ArrowBatches) != 1 ||
		result.ArrowBatches[0].Batch.Record.NumRows() != 2 ||
		len(result.ArrowBatches[0].Batch.Changes) != 2 {
		t.Fatalf("sliced Arrow Poll() = %#v, %v", result, err)
	}
	result.Release()
	result.Release()
	_ = scanner.Close()
}

func TestLogScannerWakeupAndErrorPrecedence(t *testing.T) {
	table := logWriterTable()
	block := make(chan struct{})
	entered := make(chan struct{})
	backend := scannerBackend(0)
	backend.block = block
	backend.entered = entered
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(0))
	if err != nil {
		t.Fatal(err)
	}
	polled := make(chan error, 1)
	go func() {
		_, pollErr := scanner.Poll(context.Background())
		polled <- pollErr
	}()
	<-entered
	scanner.Wakeup()
	if err := <-polled; !errors.Is(err, ErrWakeup) {
		t.Fatalf("Wakeup Poll error = %v", err)
	}
	backend.block = nil
	if _, err := scanner.Poll(context.Background()); err != nil {
		t.Fatalf("Poll after Wakeup = %v", err)
	}
	scanner.Wakeup()
	if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrWakeup) {
		t.Fatalf("pending Wakeup error = %v", err)
	}

	backend.block = block
	backend.entered = make(chan struct{})
	backend.enterOnce = sync.Once{}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, pollErr := scanner.Poll(ctx)
		polled <- pollErr
	}()
	<-backend.entered
	cancel()
	scanner.Wakeup()
	if err := <-polled; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation precedence error = %v", err)
	}
	_ = scanner.Close()
}

func TestLogScannerSubscribeCloseAndCancellation(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	block := make(chan struct{})
	backend.block = block
	backend.entered = make(chan struct{})
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := scanner.Subscribe(context.Background(), 0, Latest()); err != nil {
		t.Fatal(err)
	}
	if err := scanner.Subscribe(context.Background(), 9, AtOffset(0)); !errors.Is(err, ErrUnknownBucket) {
		t.Fatalf("unknown subscribe error = %v", err)
	}
	polled := make(chan error, 1)
	go func() {
		_, pollErr := scanner.Poll(context.Background())
		polled <- pollErr
	}()
	<-backend.entered
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-polled; !errors.Is(err, ErrClosed) {
		t.Fatalf("Poll after Close wakeup = %v", err)
	}
	if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Poll closed error = %v", err)
	}
	if err := scanner.Subscribe(context.Background(), 0, AtOffset(0)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe closed error = %v", err)
	}

	backend = scannerBackend(0)
	backend.block = block
	scanner, _ = newLogScanner(context.Background(), backend, table, AtOffset(0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scanner.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Poll error = %v", err)
	}
	_ = scanner.Close()
}

func TestLogScannerEmptySubscriptionAndMalformedBatch(t *testing.T) {
	table := logWriterTable()
	backend := scannerBackend(0)
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(0))
	if err != nil {
		t.Fatal(err)
	}
	scanner.Unsubscribe(0)
	if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty Poll error = %v", err)
	}
	_ = scanner.Close()

	if _, _, _, err := decodeFetchedLog(table.Schema, 0, 4, []byte{1}); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("truncated fetch error = %v", err)
	}
	bad := make([]byte, 12)
	bad[8] = 100
	if _, _, _, err := decodeFetchedLog(table.Schema, 0, 4, bad); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("bad size error = %v", err)
	}
}

func TestLogScannerRejectsInvalidConfiguration(t *testing.T) {
	table := logWriterTable()
	for _, test := range []struct {
		name    string
		start   ScanOffset
		backend *fakeLogScannerBackend
		options []LogScannerOption
		target  error
	}{
		{"negative offset", AtOffset(-1), scannerBackend(0), nil, ErrInvalidConfig},
		{"zero timestamp", AtTimestamp(time.Time{}), scannerBackend(0), nil, ErrInvalidConfig},
		{"unknown offset", ScanOffset{Kind: 99}, scannerBackend(0), nil, ErrInvalidConfig},
		{"nil option", AtOffset(0), scannerBackend(0), []LogScannerOption{nil}, ErrInvalidConfig},
		{"bad partition", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanPartition("   ")}, ErrInvalidConfig},
		{"empty projection", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanProjection()}, ErrInvalidConfig},
		{"unknown projection", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanProjection("missing")}, ErrInvalidSchema},
		{"bad limits", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanLimits(0, 1, 2, -1)}, ErrInvalidConfig},
		{"bad row limit", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanRowLimit(0)}, ErrInvalidConfig},
		{"empty stops", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanStoppingOffsets(nil)}, ErrInvalidConfig},
		{"negative stop", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanStoppingOffsets(map[int32]int64{0: -1})}, ErrInvalidConfig},
		{"missing stop", AtOffset(0), scannerBackend(0, 1), []LogScannerOption{WithScanStoppingOffsets(map[int32]int64{0: 1})}, ErrInvalidConfig},
		{"unknown stop", AtOffset(0), scannerBackend(0), []LogScannerOption{WithScanStoppingOffsets(map[int32]int64{0: 1, 1: 1})}, ErrInvalidConfig},
		{"metadata", AtOffset(0), &fakeLogScannerBackend{metadataErr: context.Canceled}, nil, context.Canceled},
		{"no buckets", AtOffset(0), scannerBackend(), nil, ErrMetadata},
		{"list offset", Earliest(), &fakeLogScannerBackend{physicalID: 9, locations: scannerBackend(0).locations, listErr: context.Canceled}, nil, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newLogScanner(context.Background(), test.backend, table, test.start, test.options...)
			if !errors.Is(err, test.target) {
				t.Fatalf("newLogScanner() error = %v, want %v", err, test.target)
			}
		})
	}
	if err := (ScanOffset{Kind: ScanFromLatest, Offset: 1}).Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("valued latest error = %v", err)
	}
	var config LogScannerConfig
	if err := WithScanPartition("day=2026-07-30")(&config); err != nil ||
		config.Partition != "day=2026-07-30" {
		t.Fatalf("WithScanPartition() = %#v, %v", config, err)
	}
}

func TestClientLogScannerBackendMessages(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	var listed *fmsg.ListOffsetsRequest
	var fetched *fmsg.FetchLogRequest
	table := logWriterTable()
	records := encodedRows(t, table.Schema, 4, 8)
	client := routedWriterClient(t,
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			*response.Message().(*fmsg.MetadataResponse) = *metadataResponse(path)
			return response, nil
		},
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			switch message := response.Message().(type) {
			case *fmsg.ListOffsetsResponse:
				listed = request.(*fmsg.MessageRequest).Message().(*fmsg.ListOffsetsRequest)
				message.BucketsResp = []*fmsg.PbListOffsetsRespForBucket{{
					BucketId: proto.Int32(0), Offset: proto.Int64(4),
				}}
			case *fmsg.FetchLogResponse:
				fetched = request.(*fmsg.MessageRequest).Message().(*fmsg.FetchLogRequest)
				message.TablesResp = []*fmsg.PbFetchLogRespForTable{{
					TableId: proto.Int64(9), BucketsResp: []*fmsg.PbFetchLogRespForBucket{{
						BucketId: proto.Int32(0), HighWatermark: proto.Int64(5), Records: records,
					}},
				}}
			}
			return response, nil
		},
	)
	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", role: TabletServer}]
	tablet.versions[fmsg.APIKeyListOffsets] = 0
	tablet.versions[fmsg.APIKeyFetchLog] = 0
	scanner, err := client.NewLogScanner(context.Background(), table, AtTimestamp(time.UnixMilli(1234)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || len(result.Records) != 1 {
		t.Fatalf("Poll() = %#v, %v", result, err)
	}
	if listed.GetOffsetType() != 2 || listed.GetStartTimestamp() != 1234 || listed.GetFollowerServerId() != -1 {
		t.Fatalf("ListOffsets request = %#v", listed)
	}
	bucket := fetched.GetTablesReq()[0].GetBucketsReq()[0]
	if fetched.GetFollowerServerId() != -1 || bucket.GetFetchOffset() != 4 ||
		bucket.GetMaxFetchBytes() != 1<<20 {
		t.Fatalf("FetchLog request = %#v", fetched)
	}
	result.Release()
	_ = scanner.Close()
}

func TestClientLogScannerBackendResponseErrors(t *testing.T) {
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}}
	client := routedWriterClient(t,
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			*response.Message().(*fmsg.MetadataResponse) = *metadataResponse(path.TablePath)
			return response, nil
		},
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			switch message := response.Message().(type) {
			case *fmsg.ListOffsetsResponse:
				message.BucketsResp = []*fmsg.PbListOffsetsRespForBucket{{
					BucketId: proto.Int32(0), ErrorCode: proto.Int32(int32(fmsg.ErrorCodeAuthorizationException)),
				}}
			case *fmsg.FetchLogResponse:
				message.TablesResp = []*fmsg.PbFetchLogRespForTable{{
					TableId: proto.Int64(9), BucketsResp: []*fmsg.PbFetchLogRespForBucket{{
						BucketId: proto.Int32(0), ErrorCode: proto.Int32(int32(fmsg.ErrorCodeCorruptRecordException)),
					}},
				}}
			}
			return response, nil
		},
	)
	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", role: TabletServer}]
	tablet.versions[fmsg.APIKeyListOffsets], tablet.versions[fmsg.APIKeyFetchLog] = 0, 0
	backend := clientLogScannerBackend{client: client}
	if _, err := backend.listOffset(context.Background(), path, 0, 9, -1, Latest()); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("ListOffsets error = %v", err)
	}
	if _, err := backend.fetch(context.Background(), logFetchRequest{
		path: path, tableID: 9, partitionID: -1,
		config: LogScannerConfig{MaxBytes: 1, MaxBucketBytes: 1},
	}); !errors.Is(err, ErrRecord) {
		t.Fatalf("FetchLog error = %v", err)
	}
	if _, err := backend.listOffset(context.Background(), path, 0, 9, -1, AtOffset(0)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("explicit ListOffsets error = %v", err)
	}
}
