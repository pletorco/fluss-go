package fgo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

type waitingKVBackend struct {
	fakeKVSessionBackend
	reply    func(context.Context, int32) error
	closeErr error
}

func (b *waitingKVBackend) scanKV(
	ctx context.Context, bucket TableBucket, limit int64, size int32, id []byte, seq int32, closing bool,
) (kvSessionBatch, error) {
	if !closing && len(b.batches) == 0 {
		return kvSessionBatch{}, errors.New("unexpected request after last KV page")
	}
	batch, err := b.fakeKVSessionBackend.scanKV(ctx, bucket, limit, size, id, seq, closing)
	if closing {
		err = b.closeErr
	} else if err == nil && b.reply != nil {
		err = b.reply(ctx, seq)
	}
	return batch, err
}

func newWaitingKVScanner(t *testing.T, table Table, backend *waitingKVBackend) *BatchScanner {
	t.Helper()
	scanner, err := newBatchScanner(t.Context(), backend, nil, table, testTableBucket(table))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scanner.Close(); err != nil {
			t.Error(err)
		}
	})
	return scanner
}

func TestBatchScannerRetainsInFlightKVPage(t *testing.T) {
	for _, test := range []struct {
		name         string
		pages        int
		waitSequence int32
		empty        bool
	}{
		{name: "initial", pages: 3},
		{name: "continuation", pages: 3, waitSequence: 1},
		{name: "terminal", pages: 3, waitSequence: 2},
		{name: "empty bucket", pages: 1, empty: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				table := upsertWriterTable()
				release := make(chan struct{})
				backend := &waitingKVBackend{reply: func(ctx context.Context, seq int32) error {
					if seq != test.waitSequence {
						return nil
					}
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}}
				for page := range test.pages {
					batch := kvSessionBatch{}
					if !test.empty {
						batch.scannerID = []byte("session")
						batch.hasMore = page+1 < test.pages
						batch.records = encodedValueBatch(t, table, Row{int32(page + 1), "value", int64(10)})
					}
					backend.batches = append(backend.batches, batch)
				}
				scanner := newWaitingKVScanner(t, table, backend)
				for page := range test.pages {
					if int32(page) == test.waitSequence {
						assertInterruptedKVWaits(t, scanner, backend, page+1)
						close(release)
					}
					result, err := scanner.Poll(t.Context())
					defer result.Release()
					wantDone := page+1 == test.pages
					if err != nil || result.Done != wantDone || scanner.Done() != wantDone {
						t.Fatalf("page %d = %#v, %v; want Done=%v", page, result, err, wantDone)
					}
					if test.empty && len(result.Rows) != 0 ||
						!test.empty && (len(result.Rows) != 1 || result.Rows[0][0] != int32(page+1)) {
						t.Fatalf("page %d rows = %#v", page, result.Rows)
					}
				}
				result, err := scanner.Poll(t.Context())
				defer result.Release()
				if err != nil || !result.Done || len(result.Rows) != 0 {
					t.Fatalf("completed poll = %#v, %v", result, err)
				}
				if err := scanner.Close(); err != nil {
					t.Fatal(err)
				}
				assertKVSequences(t, backend, test.pages)
			})
		})
	}
}

func assertInterruptedKVWaits(t *testing.T, scanner *BatchScanner, backend *waitingKVBackend, wantCalls int) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		result, err := scanner.Poll(ctx)
		result.Release()
		finished <- err
	}()
	synctest.Wait()
	assertKVSequences(t, backend, wantCalls)
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) || scanner.Done() {
		t.Fatalf("canceled poll = %v; done=%v", err, scanner.Done())
	}
	for range 2 {
		short, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		result, err := scanner.Poll(short)
		cancel()
		assertKVSequences(t, backend, wantCalls)
		if !errors.Is(err, context.DeadlineExceeded) || result.Done || len(result.Rows) != 0 || scanner.Done() {
			t.Fatalf("timed out poll = %#v, %v; done=%v", result, err, scanner.Done())
		}
	}
}

func assertKVSequences(t *testing.T, backend *waitingKVBackend, count int) {
	t.Helper()
	if len(backend.calls) != count {
		t.Fatalf("expected %d scan requests; got %#v", count, backend.calls)
	}
	for index, call := range backend.calls {
		if call.sequence != int32(index) || call.closeScanner ||
			index == 0 && len(call.scannerID) != 0 || index != 0 && string(call.scannerID) != "session" {
			t.Fatalf("scan request %d = %#v", index, call)
		}
	}
}

func TestBatchScannerDoesNotReplayFailedKVRequest(t *testing.T) {
	for _, cause := range []error{io.ErrUnexpectedEOF, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				table := upsertWriterTable()
				backend := &waitingKVBackend{
					fakeKVSessionBackend: fakeKVSessionBackend{batches: []kvSessionBatch{
						{scannerID: []byte("session"), hasMore: true}, {scannerID: []byte("session")},
					}},
					reply: func(ctx context.Context, seq int32) error {
						if seq == 0 {
							return nil
						}
						if cause == context.DeadlineExceeded {
							<-ctx.Done()
							return ctx.Err()
						}
						return cause
					},
				}
				scanner := newWaitingKVScanner(t, table, backend)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if _, err := scanner.Poll(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("pre-canceled poll = %v", err)
				}
				assertKVSequences(t, backend, 0)
				if _, err := scanner.Poll(t.Context()); err != nil {
					t.Fatal(err)
				}
				result, failed := scanner.Poll(t.Context())
				if !errors.Is(failed, cause) || result.Done || len(result.Rows) != 0 || scanner.Done() {
					t.Fatalf("failed request = %#v, %v; want %v", result, failed, cause)
				}
				if _, err := scanner.Poll(t.Context()); err != failed {
					t.Fatalf("later poll = %v; want retained %v", err, failed)
				}
				assertKVSequences(t, backend, 2)
				if err := scanner.Close(); err != nil {
					t.Fatal(err)
				}
				if len(backend.calls) != 3 || !backend.calls[2].closeScanner ||
					string(backend.calls[2].scannerID) != "session" || backend.calls[2].sequence != 1 {
					t.Fatalf("close did not release the known session: %#v", backend.calls)
				}
			})
		})
	}
}

func TestBatchScannerCloseWithInFlightKVRequest(t *testing.T) {
	for _, test := range []struct {
		name      string
		open      bool
		completed bool
		terminal  bool
		closeRPC  bool
	}{
		{name: "cancel initial"},
		{name: "cancel continuation", open: true, closeRPC: true},
		{name: "discard completed initial", completed: true, closeRPC: true},
		{name: "discard completed terminal", open: true, completed: true, terminal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				testCloseInFlightKV(t, test.open, test.completed, test.terminal, test.closeRPC)
			})
		})
	}
}

func testCloseInFlightKV(t *testing.T, open, completed, terminal, closeRPC bool) {
	t.Helper()
	table := upsertWriterTable()
	release := make(chan struct{})
	backend := &waitingKVBackend{
		fakeKVSessionBackend: fakeKVSessionBackend{batches: []kvSessionBatch{{scannerID: []byte("session"), hasMore: !terminal}}},
		reply: func(ctx context.Context, seq int32) error {
			if open && seq == 0 {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		},
	}
	scanner := newWaitingKVScanner(t, table, backend)
	wantCalls := 1
	if open {
		backend.batches = append([]kvSessionBatch{{scannerID: []byte("session"), hasMore: true}}, backend.batches...)
		if _, err := scanner.Poll(t.Context()); err != nil {
			t.Fatal(err)
		}
		wantCalls++
	}
	assertInterruptedKVWaits(t, scanner, backend, wantCalls)
	if completed {
		close(release)
		synctest.Wait()
	}
	if err := scanner.Close(); err != nil {
		t.Fatal(err)
	}
	if closeRPC {
		wantCalls++
	}
	if len(backend.calls) != wantCalls ||
		closeRPC && (!backend.calls[wantCalls-1].closeScanner || string(backend.calls[wantCalls-1].scannerID) != "session") {
		t.Fatalf("Close requests = %#v; want %d calls, close RPC = %v", backend.calls, wantCalls, closeRPC)
	}
	if _, err := scanner.Poll(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("poll after close = %v", err)
	}
}

func TestBatchScannerCloseIgnoresGoneKVSession(t *testing.T) {
	for _, code := range []fmsg.ErrorCode{fmsg.ErrorCodeScannerExpired, fmsg.ErrorCodeUnknownScannerId, fmsg.ErrorCodeUnknownServerError} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			table := upsertWriterTable()
			backend := &waitingKVBackend{
				fakeKVSessionBackend: fakeKVSessionBackend{batches: []kvSessionBatch{{scannerID: []byte("session"), hasMore: true}}},
				closeErr:             &ServerError{Code: code},
			}
			scanner := newWaitingKVScanner(t, table, backend)
			if _, err := scanner.Poll(t.Context()); err != nil {
				t.Fatal(err)
			}
			err := scanner.Close()
			if code == fmsg.ErrorCodeUnknownServerError && !errors.Is(err, backend.closeErr) ||
				code != fmsg.ErrorCodeUnknownServerError && err != nil {
				t.Fatalf("Close = %v; server code = %v", err, code)
			}
		})
	}
}
