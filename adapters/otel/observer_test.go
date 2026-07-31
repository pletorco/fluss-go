package otel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type measurement struct {
	name       string
	intValue   int64
	floatValue float64
	attributes attribute.Set
}

type recordingProvider struct {
	metric.MeterProvider
	meter *recordingMeter
	scope string
}

func (p *recordingProvider) Meter(
	name string,
	_ ...metric.MeterOption,
) metric.Meter {
	p.scope = name
	return p.meter
}

type recordingMeter struct {
	metric.Meter
	mu           sync.Mutex
	measurements []measurement
	names        []string
	failName     string
	panicName    string
}

func (m *recordingMeter) Int64Counter(
	name string,
	_ ...metric.Int64CounterOption,
) (metric.Int64Counter, error) {
	if err := m.instrument(name); err != nil {
		return nil, err
	}
	return recordingInt64Counter{meter: m, name: name, panic_: name == m.panicName}, nil
}

func (m *recordingMeter) Int64Histogram(
	name string,
	_ ...metric.Int64HistogramOption,
) (metric.Int64Histogram, error) {
	if err := m.instrument(name); err != nil {
		return nil, err
	}
	return recordingInt64Histogram{meter: m, name: name}, nil
}

func (m *recordingMeter) Float64Histogram(
	name string,
	_ ...metric.Float64HistogramOption,
) (metric.Float64Histogram, error) {
	if err := m.instrument(name); err != nil {
		return nil, err
	}
	return recordingFloat64Histogram{meter: m, name: name}, nil
}

func (m *recordingMeter) instrument(name string) error {
	m.names = append(m.names, name)
	if name == m.failName {
		return errors.New("instrument failure")
	}
	return nil
}

func (m *recordingMeter) append(measurement measurement) {
	m.mu.Lock()
	m.measurements = append(m.measurements, measurement)
	m.mu.Unlock()
}

type recordingInt64Counter struct {
	metric.Int64Counter
	meter  *recordingMeter
	name   string
	panic_ bool
}

func (c recordingInt64Counter) Add(
	_ context.Context,
	value int64,
	options ...metric.AddOption,
) {
	if c.panic_ {
		panic("instrument panic")
	}
	config := metric.NewAddConfig(options)
	c.meter.append(measurement{
		name: c.name, intValue: value, attributes: config.Attributes(),
	})
}

func (recordingInt64Counter) Enabled(context.Context) bool { return true }

type recordingInt64Histogram struct {
	metric.Int64Histogram
	meter *recordingMeter
	name  string
}

func (h recordingInt64Histogram) Record(
	_ context.Context,
	value int64,
	options ...metric.RecordOption,
) {
	config := metric.NewRecordConfig(options)
	h.meter.append(measurement{
		name: h.name, intValue: value, attributes: config.Attributes(),
	})
}

func (recordingInt64Histogram) Enabled(context.Context) bool { return true }

type recordingFloat64Histogram struct {
	metric.Float64Histogram
	meter *recordingMeter
	name  string
}

func (h recordingFloat64Histogram) Record(
	_ context.Context,
	value float64,
	options ...metric.RecordOption,
) {
	config := metric.NewRecordConfig(options)
	h.meter.append(measurement{
		name: h.name, floatValue: value, attributes: config.Attributes(),
	})
}

func (recordingFloat64Histogram) Enabled(context.Context) bool { return true }

func newRecordingObserver(t *testing.T) (*Observer, *recordingMeter) {
	t.Helper()
	meter := &recordingMeter{}
	observer, err := New(&recordingProvider{meter: meter})
	if err != nil {
		t.Fatal(err)
	}
	return observer, meter
}

func TestObserverMapsBoundedMetricsAndAttributes(t *testing.T) {
	meter := &recordingMeter{}
	provider := &recordingProvider{meter: meter}
	observer, err := New(provider)
	if err != nil {
		t.Fatal(err)
	}
	if provider.scope != instrumentationScope || len(meter.names) != 9 {
		t.Fatalf("scope=%q instruments=%#v", provider.scope, meter.names)
	}
	observer.ObserveMetric(fgo.MetricEvent{
		Kind: fgo.MetricWriteBatch, Operation: fgo.MetricOperationKVWrite,
		APIKey: fmsg.APIKeyPutKv, ServerRole: fgo.TabletServer,
		Duration: 2 * time.Second, QueueTime: 500 * time.Millisecond,
		Attempt: 2, QueueSize: 3, Records: 4, Bytes: 5, Lag: -1,
		Failed: true, ErrorClass: fgo.MetricErrorTimeout,
	})
	observer.ObserveMetric(fgo.MetricEvent{
		Kind: fgo.MetricScannerFetch, Operation: fgo.MetricOperationLogScan,
		QueueSize: -1, Lag: 6,
	})
	meter.mu.Lock()
	measurements := append([]measurement(nil), meter.measurements...)
	meter.mu.Unlock()
	if len(measurements) != 10 {
		t.Fatalf("measurements = %#v", measurements)
	}
	expected := map[string]float64{
		"fluss.client.events":         1,
		"fluss.client.failures":       1,
		"fluss.client.bytes":          5,
		"fluss.client.records":        4,
		"fluss.client.duration":       2,
		"fluss.client.queue.duration": 0.5,
		"fluss.client.queue.size":     3,
		"fluss.client.attempt":        2,
		"fluss.client.lag":            6,
	}
	for _, got := range measurements {
		value := float64(got.intValue)
		if got.floatValue != 0 {
			value = got.floatValue
		}
		if value != expected[got.name] {
			t.Errorf("%s = %v, want %v", got.name, value, expected[got.name])
		}
		attributes := got.attributes.ToSlice()
		if len(attributes) != 6 {
			t.Fatalf("%s attributes = %#v", got.name, attributes)
		}
		for _, item := range attributes {
			switch string(item.Key) {
			case "fluss.event.kind", "fluss.operation", "fluss.error.class",
				"fluss.api.key", "fluss.server.role", "fluss.failed":
			default:
				t.Fatalf("unexpected attribute %q", item.Key)
			}
		}
	}
}

func TestObserverOmitsUnavailableMeasurements(t *testing.T) {
	observer, meter := newRecordingObserver(t)
	observer.ObserveMetric(fgo.MetricEvent{})
	meter.mu.Lock()
	defer meter.mu.Unlock()
	if len(meter.measurements) != 1 ||
		meter.measurements[0].name != "fluss.client.events" {
		t.Fatalf("measurements = %#v", meter.measurements)
	}
	(*Observer)(nil).ObserveMetric(fgo.MetricEvent{})
}

func TestObserverConstructionErrors(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("New(nil) error = %v", err)
	}
	for _, name := range []string{
		"fluss.client.events",
		"fluss.client.failures",
		"fluss.client.bytes",
		"fluss.client.records",
		"fluss.client.duration",
		"fluss.client.queue.duration",
		"fluss.client.queue.size",
		"fluss.client.attempt",
		"fluss.client.lag",
	} {
		_, err := New(&recordingProvider{meter: &recordingMeter{failName: name}})
		if err == nil {
			t.Errorf("instrument %q error = nil", name)
		}
	}
}

func TestObserverIsolatesPanicsAndSupportsConcurrency(t *testing.T) {
	panicMeter := &recordingMeter{panicName: "fluss.client.events"}
	panicObserver, err := New(&recordingProvider{meter: panicMeter})
	if err != nil {
		t.Fatal(err)
	}
	panicObserver.ObserveMetric(fgo.MetricEvent{Failed: true})

	observer, meter := newRecordingObserver(t)
	const calls = 100
	var wait sync.WaitGroup
	for range calls {
		wait.Add(1)
		go func() {
			defer wait.Done()
			observer.ObserveMetric(fgo.MetricEvent{QueueSize: -1, Lag: -1})
		}()
	}
	wait.Wait()
	meter.mu.Lock()
	defer meter.mu.Unlock()
	if len(meter.measurements) != calls {
		t.Fatalf("concurrent measurements = %d", len(meter.measurements))
	}
}

func TestObserverAllocationBound(t *testing.T) {
	observer, _ := newRecordingObserver(t)
	allocations := testing.AllocsPerRun(100, func() {
		observer.ObserveMetric(fgo.MetricEvent{QueueSize: -1, Lag: -1})
	})
	if allocations > 12 {
		t.Fatalf("ObserveMetric allocations = %.1f, want <= 12", allocations)
	}
}
