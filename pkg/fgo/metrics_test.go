package fgo

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

type metricRecorder struct {
	mu     sync.Mutex
	events []MetricEvent
}

func (r *metricRecorder) ObserveMetric(event MetricEvent) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *metricRecorder) snapshot() []MetricEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]MetricEvent(nil), r.events...)
}

func (r *metricRecorder) find(kind MetricKind, operation MetricOperation) (MetricEvent, bool) {
	for _, event := range r.snapshot() {
		if event.Kind == kind && event.Operation == operation {
			return event, true
		}
	}
	return MetricEvent{}, false
}

func TestMetricsObserverRequestsAndIsolation(t *testing.T) {
	recorder := &metricRecorder{}
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		return fmsg.NewResponse(request.APIKey(), request.Version())
	}), nil)
	client.observer = recorder
	client.role = TabletServer
	client.versions[fmsg.APIKeyLookup] = 0
	request, _ := fmsg.NewRequest(fmsg.APIKeyLookup, 0)
	if _, err := client.Request(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	event, ok := recorder.find(MetricRequest, MetricOperationRPC)
	if !ok || event.APIKey != fmsg.APIKeyLookup || event.ServerRole != TabletServer ||
		event.Failed || event.ErrorClass != MetricErrorNone || event.Duration <= 0 {
		t.Fatalf("request metric = %#v, found=%v", event, ok)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Request(context.Background(), request); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed request error = %v", err)
	}
	events := recorder.snapshot()
	if got := events[len(events)-1]; !got.Failed || got.ErrorClass != MetricErrorClosed {
		t.Fatalf("closed metric = %#v", got)
	}

	panicClient := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		return fmsg.NewResponse(request.APIKey(), request.Version())
	}), nil)
	panicClient.versions[fmsg.APIKeyLookup] = 0
	panicClient.observer = MetricsObserverFunc(func(MetricEvent) { panic("observer failure") })
	if _, err := panicClient.Request(context.Background(), request); err != nil {
		t.Fatalf("observer panic affected request: %v", err)
	}
	if err := WithMetricsObserver(nil)(&config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil observer error = %v", err)
	}
}

func TestMetricsObserverLogWriterEvent(t *testing.T) {
	recorder := &metricRecorder{}
	logBackend := logBackend(0)
	logWriter, err := newLogWriter(
		context.Background(), logBackend, logWriterTable(), WithLogLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	logWriter.observer = recorder
	if result := logWriter.Append(context.Background(), Row{int32(1), "one"}).
		Await(context.Background()); result.Err != nil {
		t.Fatal(result.Err)
	}
	if event, ok := recorder.find(MetricWriteBatch, MetricOperationLogWrite); !ok ||
		event.Records != 1 || event.Bytes <= 0 || event.QueueTime <= 0 || event.Failed {
		t.Fatalf("log writer metric = %#v, found=%v", event, ok)
	}
	_ = logWriter.Close(context.Background())
}

func TestMetricsObserverKVWriterEvent(t *testing.T) {
	recorder := &metricRecorder{}
	kvBackend := kvBackend(0)
	kvWriter, err := newKVWriter(
		context.Background(), kvBackend, kvWriterTable(), WithKVLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	kvWriter.observer = recorder
	if result := kvWriter.Upsert(context.Background(), Row{int32(1), "one", int64(1)}).
		Await(context.Background()); result.Err != nil {
		t.Fatal(result.Err)
	}
	if event, ok := recorder.find(MetricWriteBatch, MetricOperationKVWrite); !ok ||
		event.Records != 1 || event.Bytes <= 0 || event.Failed {
		t.Fatalf("KV writer metric = %#v, found=%v", event, ok)
	}
	_ = kvWriter.Close(context.Background())
}

func TestMetricsObserverScannerEvents(t *testing.T) {
	recorder := &metricRecorder{}
	table := logWriterTable()
	scanBackend := scannerBackend(0)
	scanBackend.fetches[0] = scannerFetch{
		records: encodedRows(t, table.Schema, 0, 1), highWatermark: 10,
	}
	scanner, err := newLogScanner(context.Background(), scanBackend, table, AtOffset(0))
	if err != nil {
		t.Fatal(err)
	}
	scanner.observer = recorder
	result, err := scanner.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result.Release()
	if event, ok := recorder.find(MetricScannerFetch, MetricOperationLogScan); !ok ||
		event.Records != 1 || event.Bytes <= 0 || event.Lag != 9 || event.Failed {
		t.Fatalf("scanner metric = %#v, found=%v", event, ok)
	}
	_ = scanner.Close()

	badBackend := scannerBackend(0)
	badBackend.fetches[0] = scannerFetch{records: make([]byte, 12)}
	badScanner, err := newLogScanner(context.Background(), badBackend, table, AtOffset(0))
	if err != nil {
		t.Fatal(err)
	}
	badScanner.observer = recorder
	badResult, err := badScanner.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	badResult.Release()
	if event, ok := recorder.find(MetricDecodeFailure, MetricOperationLogScan); !ok ||
		!event.Failed || event.ErrorClass != MetricErrorProtocol {
		t.Fatalf("decode metric = %#v, found=%v", event, ok)
	}
	_ = badScanner.Close()
}

func TestMetricErrorClassification(t *testing.T) {
	for _, test := range []struct {
		err  error
		want MetricErrorClass
	}{
		{nil, MetricErrorNone},
		{context.Canceled, MetricErrorCanceled},
		{context.DeadlineExceeded, MetricErrorTimeout},
		{ErrClosed, MetricErrorClosed},
		{ErrMalformedRow, MetricErrorProtocol},
		{errors.New("other"), MetricErrorOther},
	} {
		if got := metricErrorClass(test.err); got != test.want {
			t.Fatalf("metricErrorClass(%v) = %d, want %d", test.err, got, test.want)
		}
	}
	if got := metricDuration(time.Time{}); got != 0 {
		t.Fatalf("zero metric duration = %v", got)
	}
}
