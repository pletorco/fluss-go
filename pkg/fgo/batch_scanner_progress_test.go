package fgo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type retryKVSessionBackend struct {
	fakeKVSessionBackend
	resolve func(context.Context) (Schema, error)
}

func (b *retryKVSessionBackend) schemaResolver() schemaResolver { return b }

func (b *retryKVSessionBackend) resolveSchema(ctx context.Context, _ TablePath, _ int32) (Schema, error) {
	return b.resolve(ctx)
}

func (b *retryKVSessionBackend) scanKV(
	ctx context.Context, bucket TableBucket, limit int64, batchSize int32,
	scannerID []byte, sequence int32, closing bool,
) (kvSessionBatch, error) {
	if !closing && len(b.batches) == 0 {
		return kvSessionBatch{}, errors.New("unexpected fetch after terminal KV page")
	}
	return b.fakeKVSessionBackend.scanKV(ctx, bucket, limit, batchSize, scannerID, sequence, closing)
}

func retryKVScanFixture(t *testing.T, pages int) (Table, *retryKVSessionBackend) {
	t.Helper()
	table := upsertWriterTable()
	table.Schema = evolutionSchema("name", true, StringType)
	table.SchemaID = 2
	historical := table
	historical.SchemaID = 1
	backend := &retryKVSessionBackend{
		resolve: func(context.Context) (Schema, error) { return historical.Schema, nil },
	}
	for index := range pages {
		backend.batches = append(backend.batches, kvSessionBatch{
			scannerID: []byte("session"), hasMore: index+1 < pages,
			records: encodedValueBatch(t, historical, Row{int32(index + 1), "value"}),
		})
	}
	return table, backend
}

func TestBatchScannerRetriesUndeliveredKVPage(t *testing.T) {
	for _, failure := range []string{"canceled", "transient"} {
		for failedPage := range 3 {
			t.Run(fmt.Sprintf("%s/page_%d", failure, failedPage), func(t *testing.T) {
				table, backend := retryKVScanFixture(t, 3)
				scanner, err := newBatchScanner(context.Background(), backend, nil, table, testTableBucket(table),
					WithBatchProjection("name", "id"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := scanner.Close(); err != nil {
						t.Error(err)
					}
				})
				for page := range 3 {
					if page == failedPage {
						for range 2 {
							ctx, cancel := context.WithCancel(context.Background())
							cause := errors.New("temporary schema lookup failure")
							backend.resolve = func(ctx context.Context) (Schema, error) {
								if failure == "canceled" {
									cancel()
									return Schema{}, ctx.Err()
								}
								return Schema{}, cause
							}
							if failure == "canceled" {
								cause = context.Canceled
							}
							result, err := scanner.Poll(ctx)
							cancel()
							result.Release()
							if !errors.Is(err, cause) || len(result.Rows) != 0 || result.Done || scanner.Done() {
								t.Fatalf("failed Poll = %#v, %v; want empty result and %v", result, err, cause)
							}
							if len(backend.calls) != page+1 {
								t.Fatalf("decode retry fetched another page: calls=%#v", backend.calls)
							}
						}
						backend.resolve = func(context.Context) (Schema, error) { return table.Schema, nil }
					}
					result, err := scanner.Poll(context.Background())
					want := []Row{{"value", int32(page + 1)}}
					if err != nil || !reflect.DeepEqual(result.Rows, want) || result.Done != (page == 2) {
						t.Fatalf("page %d Poll = %#v, %v; want %v", page, result, err, want)
					}
					result.Release()
				}
				terminal, err := scanner.Poll(context.Background())
				if err != nil || !terminal.Done || len(terminal.Rows) != 0 || !scanner.Done() {
					t.Fatalf("completed Poll = %#v, %v", terminal, err)
				}
				if err := scanner.Close(); err != nil {
					t.Fatal(err)
				}
				if len(backend.calls) != 3 {
					t.Fatalf("scan replayed or closed a terminal session: %#v", backend.calls)
				}
				for index, call := range backend.calls {
					if call.sequence != int32(index) || call.closeScanner ||
						index == 0 && len(call.scannerID) != 0 || index != 0 && string(call.scannerID) != "session" {
						t.Fatalf("scan call %d = %#v", index, call)
					}
				}
			})
		}
	}
}

func TestBatchScannerCloseCancelsPendingKVDecode(t *testing.T) {
	for _, pages := range []int{1, 2} {
		t.Run(fmt.Sprintf("pages_%d", pages), func(t *testing.T) {
			table, backend := retryKVScanFixture(t, pages)
			entered := make(chan struct{})
			backend.resolve = func(ctx context.Context) (Schema, error) {
				close(entered)
				<-ctx.Done()
				return Schema{}, ctx.Err()
			}
			scanner, err := newBatchScanner(context.Background(), backend, nil, table, testTableBucket(table))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = scanner.Close() })
			polled := make(chan error, 1)
			go func() {
				result, err := scanner.Poll(context.Background())
				result.Release()
				polled <- err
			}()
			<-entered
			if err := scanner.Close(); err != nil {
				t.Fatal(err)
			}
			if err := <-polled; !errors.Is(err, context.Canceled) {
				t.Fatalf("interrupted Poll error = %v", err)
			}
			wantCalls := pages // Terminal page needs no close; nonterminal page does.
			if len(backend.calls) != wantCalls {
				t.Fatalf("Close calls = %#v; want %d requests", backend.calls, wantCalls)
			}
			if pages == 2 && (!backend.calls[1].closeScanner || backend.calls[1].sequence != 1 ||
				string(backend.calls[1].scannerID) != "session") {
				t.Fatalf("session cleanup = %#v", backend.calls[1])
			}
			if err := scanner.Close(); err != nil || len(backend.calls) != wantCalls {
				t.Fatalf("repeated Close = %v; calls=%#v", err, backend.calls)
			}
			if _, err := scanner.Poll(context.Background()); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed Poll error = %v", err)
			}
			if scanner.pendingKV != nil {
				t.Fatal("Close retained the undelivered page")
			}
		})
	}
}

func TestBatchScannerKeepsKVPageAfterIgnoredCancellation(t *testing.T) {
	table, backend := retryKVScanFixture(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backend.resolve = func(context.Context) (Schema, error) {
		cancel()
		return table.Schema, nil // A cached resolver need not observe cancellation.
	}
	scanner, err := newBatchScanner(context.Background(), backend, nil, table, testTableBucket(table))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scanner.Close() })
	for range 2 {
		result, err := scanner.Poll(ctx)
		if !errors.Is(err, context.Canceled) || len(result.Rows) != 0 || result.Done || scanner.Done() {
			t.Fatalf("canceled Poll = %#v, %v; done=%v", result, err, scanner.Done())
		}
	}
	backend.resolve = func(ctx context.Context) (Schema, error) {
		if err := ctx.Err(); err != nil {
			t.Fatalf("resolver received an already canceled context: %v", err)
		}
		return table.Schema, nil
	}
	expired, expire := context.WithDeadline(context.Background(), time.Time{})
	defer expire()
	if _, err := scanner.Poll(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired Poll = %v", err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || !reflect.DeepEqual(result.Rows, []Row{{int32(1), "value"}}) || len(backend.calls) != 1 {
		t.Fatalf("retry Poll = %#v, %v; calls=%#v", result, err, backend.calls)
	}
	if scanner.pendingKV != nil {
		t.Fatal("successful delivery retained the raw page")
	}
}

func TestBatchScannerRetainsMalformedKVPage(t *testing.T) {
	table, backend := retryKVScanFixture(t, 2)
	backend.batches[0].records = []byte("malformed")
	scanner, err := newBatchScanner(context.Background(), backend, nil, table, testTableBucket(table))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scanner.Close() })
	for range 3 {
		result, err := scanner.Poll(context.Background())
		if !errors.Is(err, ErrMalformedRecordBatch) || len(result.Rows) != 0 || result.Done || scanner.Done() {
			t.Fatalf("malformed Poll = %#v, %v", result, err)
		}
		if len(backend.calls) != 1 || len(backend.batches) != 1 {
			t.Fatalf("malformed page consumed more input: calls=%#v", backend.calls)
		}
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if scanner.pendingKV != nil || len(backend.calls) != 2 || !backend.calls[1].closeScanner {
		t.Fatalf("Close retained a page or leaked a session: pending=%v, calls=%#v", scanner.pendingKV, backend.calls)
	}
}

func TestBatchScannerRetriesWholeKVPageAfterPartialDecode(t *testing.T) {
	table, backend := retryKVScanFixture(t, 1)
	historical := table
	historical.SchemaID = 1
	want := []Row{{int32(1), "one"}, {int32(2), "two"}}
	backend.batches[0].records = encodedValueBatch(t, historical, want...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolutions := 0
	backend.resolve = func(ctx context.Context) (Schema, error) {
		resolutions++
		if resolutions == 2 {
			cancel()
			return Schema{}, ctx.Err()
		}
		return table.Schema, nil
	}
	scanner, err := newBatchScanner(context.Background(), backend, nil, table, testTableBucket(table))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scanner.Close() })
	partial, err := scanner.Poll(ctx)
	if !errors.Is(err, context.Canceled) || len(partial.Rows) != 0 || scanner.Done() {
		t.Fatalf("partial decode Poll = %#v, %v", partial, err)
	}
	result, err := scanner.Poll(context.Background())
	if err != nil || !result.Done || !reflect.DeepEqual(result.Rows, want) || len(backend.calls) != 1 {
		t.Fatalf("retried page = %#v, %v; calls=%#v", result, err, backend.calls)
	}
}
