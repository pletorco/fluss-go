package fgo

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type writerAdmissionFixture struct {
	writer     barrierWriter
	backend    *barrierBackend
	submit     func(context.Context) *WriteFuture
	mutations  map[string]func(context.Context) *WriteFuture
	queueDepth func() int
	slots      chan struct{}
	gate       chan struct{}
}

func newWriterAdmissionFixture(t *testing.T, kind string) writerAdmissionFixture {
	t.Helper()
	f := writerAdmissionFixture{
		backend: &barrierBackend{requests: make(chan barrierRequest, 1), stop: make(chan struct{})},
	}
	if kind == "append" {
		w, err := newAppendWriter(context.Background(), barrierAppendBackend{logBackend(0), f.backend},
			appendWriterTable(), WithAppendBuffer(2), WithAppendBatchTimeout(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		f.writer, f.slots, f.gate = w, w.slots, w.admission
		f.queueDepth = func() int { return len(w.commands) }
		f.submit = func(ctx context.Context) *WriteFuture { return w.Append(ctx, Row{int32(1), "value"}) }
		schema, err := w.table.Schema.ArrowSchema()
		if err != nil {
			t.Fatal(err)
		}
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		builder.Field(0).(*array.Int32Builder).Append(1)
		builder.Field(1).(*array.StringBuilder).Append("value")
		record := builder.NewRecordBatch()
		builder.Release()
		t.Cleanup(record.Release)
		f.mutations = map[string]func(context.Context) *WriteFuture{
			"append": f.submit,
			"arrow": func(ctx context.Context) *WriteFuture {
				return w.AppendArrow(ctx, 0, record, []ChangeType{Append})
			},
		}
	} else {
		w, err := newUpsertWriter(context.Background(), barrierUpsertBackend{kvBackend(0), f.backend},
			upsertWriterTable(), WithUpsertBuffer(2), WithUpsertBatchTimeout(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		f.writer, f.slots, f.gate = w, w.slots, w.admission
		f.queueDepth = func() int { return len(w.commands) }
		f.submit = func(ctx context.Context) *WriteFuture { return w.Upsert(ctx, Row{int32(1), "value", int64(1)}) }
		f.mutations = map[string]func(context.Context) *WriteFuture{
			"upsert": f.submit,
			"partial": func(ctx context.Context) *WriteFuture {
				return w.PartialUpsert(ctx, []string{"id", "name"}, Row{int32(1), "value"})
			},
			"delete": func(ctx context.Context) *WriteFuture { return w.Delete(ctx, PrimaryKey{int32(1)}) },
		}
	}
	t.Cleanup(func() {
		close(f.backend.stop)
		_ = f.writer.Close(context.Background())
	})
	return f
}

func startWriterAdmissionCall(call func() error) <-chan error {
	result := make(chan error, 1)
	go func() { result <- call() }()
	return result
}

func awaitWriterAdmissionCall(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("call did not return independently of the blocked backend")
		return nil
	}
}

func (f writerAdmissionFixture) saturate(t *testing.T, holdGate bool) (*WriteFuture, barrierRequest, []<-chan error) {
	t.Helper()
	future := f.submit(context.Background())
	flushes := []<-chan error{startWriterAdmissionCall(func() error { return f.writer.Flush(context.Background()) })}
	request := <-f.backend.requests // Active Flush is draining the first write.
	for range 2 {
		flushes = append(flushes, startWriterAdmissionCall(func() error { return f.writer.Flush(context.Background()) }))
		synctest.Wait()
	}
	if holdGate {
		flushes = append(flushes, startWriterAdmissionCall(func() error { return f.writer.Flush(context.Background()) }))
		synctest.Wait()
	}
	if f.queueDepth() != 2 || (len(f.gate) != 0) != holdGate {
		t.Fatalf("setup: queue=%d gate=%d; hold gate=%v", f.queueDepth(), len(f.gate), holdGate)
	}
	return future, request, flushes
}

func TestWriterAdmissionCancellation(t *testing.T) {
	for _, kind := range []string{"append", "upsert"} {
		operations := []string{"flush", "close", "append", "arrow"}
		if kind == "upsert" {
			operations = []string{"flush", "close", "upsert", "partial", "delete"}
		}
		for _, operation := range operations {
			for _, holdGate := range []bool{false, true} {
				for _, mode := range []string{"cancel", "deadline"} {
					t.Run(fmt.Sprintf("%s/%s/held_gate_%t/%s", kind, operation, holdGate, mode), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							testWriterAdmissionCancellation(t, kind, operation, holdGate, mode)
						})
					})
				}
			}
		}
	}
}

func testWriterAdmissionCancellation(t *testing.T, kind, operation string, holdGate bool, mode string) {
	t.Helper()
	f := newWriterAdmissionFixture(t, kind)
	future, request, flushes := f.saturate(t, holdGate)
	ctx, cancel := context.WithCancel(context.Background())
	want := context.Canceled
	if mode == "deadline" {
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
		want = context.DeadlineExceeded
	}
	defer cancel()
	call := startWriterAdmissionCall(func() error {
		switch operation {
		case "flush":
			return f.writer.Flush(ctx)
		case "close":
			return f.writer.Close(ctx)
		default:
			return f.mutations[operation](ctx).Await(context.Background()).Err
		}
	})
	synctest.Wait()
	if mode == "cancel" {
		cancel()
	}
	if err := awaitWriterAdmissionCall(t, call); !errors.Is(err, want) || errors.Is(err, ErrClosed) {
		t.Fatalf("interrupted %s = %v; want %v", operation, err, want)
	}
	if f.queueDepth() != 2 || len(f.slots) != 1 || (len(f.gate) != 0) != holdGate {
		t.Fatalf("canceled admission leaked state: queue=%d slots=%d gate=%d", f.queueDepth(), len(f.slots), len(f.gate))
	}
	select {
	case <-future.done:
		t.Fatal("canceled admission interrupted the accepted write")
	default:
	}
	request.result <- nil
	for _, flushed := range flushes {
		if err := awaitWriterAdmissionCall(t, flushed); err != nil {
			t.Fatalf("accepted Flush = %v", err)
		}
	}
	if result := future.Await(context.Background()); result.Err != nil {
		t.Fatal(result.Err)
	}
	later := f.submit(context.Background())
	closed := startWriterAdmissionCall(func() error { return f.writer.Close(context.Background()) })
	(<-f.backend.requests).result <- nil
	if err := awaitWriterAdmissionCall(t, closed); err != nil {
		t.Fatalf("Close after interrupted admission = %v", err)
	}
	if result := later.Await(context.Background()); result.Err != nil || len(f.slots) != 0 {
		t.Fatalf("later write = %#v; slots=%d", result, len(f.slots))
	}
}

func TestWriterAdmissionRechecksCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gate := make(chan struct{}, 1)
	for range 100 {
		if err := acquireWriterAdmission(ctx, gate); !errors.Is(err, context.Canceled) || len(gate) != 0 {
			t.Fatalf("canceled free-gate admission = %v; gate=%d", err, len(gate))
		}
	}
}

func TestWriterAdmissionCloseRetainsTerminalResult(t *testing.T) {
	for _, kind := range []string{"append", "upsert"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newWriterAdmissionFixture(t, kind)
				future := f.submit(context.Background())
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				first := startWriterAdmissionCall(func() error { return f.writer.Close(ctx) })
				request := <-f.backend.requests // Close was accepted and is flushing.
				cancel()
				if err := awaitWriterAdmissionCall(t, first); !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled accepted Close = %v", err)
				}
				var callers []<-chan error
				for range 8 {
					callers = append(callers, startWriterAdmissionCall(func() error { return f.writer.Close(context.Background()) }))
				}
				synctest.Wait()
				for name, submit := range f.mutations {
					if result := submit(context.Background()).Await(context.Background()); !errors.Is(result.Err, ErrClosed) {
						t.Fatalf("%s during Close = %#v", name, result)
					}
				}
				if err := f.writer.Flush(context.Background()); !errors.Is(err, ErrClosed) {
					t.Fatalf("Flush during Close = %v", err)
				}
				select {
				case <-future.done:
					t.Fatal("canceling Close canceled the accepted write")
				default:
				}
				request.result <- errWriterTerminal
				var terminal error
				for _, caller := range callers {
					err := awaitWriterAdmissionCall(t, caller)
					if !errors.Is(err, errWriterTerminal) || terminal != nil && err != terminal {
						t.Fatalf("concurrent Close changed terminal result: %v; first=%v", err, terminal)
					}
					terminal = err
				}
				if result := future.Await(context.Background()); !errors.Is(result.Err, errWriterTerminal) {
					t.Fatalf("accepted future lost terminal error: %#v", result)
				}
				if err := f.writer.Close(ctx); err != terminal {
					t.Fatalf("completed Close lost retained result with canceled context: %v", err)
				}
				if len(f.slots) != 0 || len(f.gate) != 0 {
					t.Fatalf("Close leaked admission: slots=%d gate=%d", len(f.slots), len(f.gate))
				}
			})
		})
	}
}
