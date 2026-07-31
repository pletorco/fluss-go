// Package otel maps bounded fgo client events to OpenTelemetry metrics.
package otel

import (
	"context"
	"fmt"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationScope = "github.com/pletorco/fluss-go/adapters/otel"

// Observer records fgo client events through synchronous OpenTelemetry API
// instruments. It owns no SDK, reader, exporter, or shutdown lifecycle.
type Observer struct {
	events        metric.Int64Counter
	failures      metric.Int64Counter
	bytes         metric.Int64Counter
	records       metric.Int64Counter
	duration      metric.Float64Histogram
	queueDuration metric.Float64Histogram
	queueSize     metric.Int64Histogram
	attempts      metric.Int64Histogram
	lag           metric.Int64Histogram
}

// New creates an observer from an application-owned MeterProvider.
func New(provider metric.MeterProvider, options ...metric.MeterOption) (*Observer, error) {
	if provider == nil {
		return nil, fmt.Errorf("%w: nil OpenTelemetry meter provider", fgo.ErrInvalidConfig)
	}
	meter := provider.Meter(instrumentationScope, options...)
	observer := &Observer{}
	var err error
	if observer.events, err = meter.Int64Counter(
		"fluss.client.events",
		metric.WithDescription("Completed fluss-go client events."),
		metric.WithUnit("{event}"),
	); err != nil {
		return nil, fmt.Errorf("otel: create event counter: %w", err)
	}
	if observer.failures, err = meter.Int64Counter(
		"fluss.client.failures",
		metric.WithDescription("Failed fluss-go client events."),
		metric.WithUnit("{failure}"),
	); err != nil {
		return nil, fmt.Errorf("otel: create failure counter: %w", err)
	}
	if observer.bytes, err = meter.Int64Counter(
		"fluss.client.bytes",
		metric.WithDescription("Bytes processed by fluss-go client operations."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, fmt.Errorf("otel: create byte counter: %w", err)
	}
	if observer.records, err = meter.Int64Counter(
		"fluss.client.records",
		metric.WithDescription("Records processed by fluss-go client operations."),
		metric.WithUnit("{record}"),
	); err != nil {
		return nil, fmt.Errorf("otel: create record counter: %w", err)
	}
	if observer.duration, err = meter.Float64Histogram(
		"fluss.client.duration",
		metric.WithDescription("Duration of fluss-go client operations."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, fmt.Errorf("otel: create duration histogram: %w", err)
	}
	if observer.queueDuration, err = meter.Float64Histogram(
		"fluss.client.queue.duration",
		metric.WithDescription("Time fluss-go work spent queued."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, fmt.Errorf("otel: create queue duration histogram: %w", err)
	}
	if observer.queueSize, err = meter.Int64Histogram(
		"fluss.client.queue.size",
		metric.WithDescription("Observed fluss-go queue depth."),
		metric.WithUnit("{item}"),
	); err != nil {
		return nil, fmt.Errorf("otel: create queue size histogram: %w", err)
	}
	if observer.attempts, err = meter.Int64Histogram(
		"fluss.client.attempt",
		metric.WithDescription("Attempt number observed by fluss-go."),
		metric.WithUnit("{attempt}"),
	); err != nil {
		return nil, fmt.Errorf("otel: create attempt histogram: %w", err)
	}
	if observer.lag, err = meter.Int64Histogram(
		"fluss.client.lag",
		metric.WithDescription("Non-negative record lag observed by fluss-go."),
		metric.WithUnit("{record}"),
	); err != nil {
		return nil, fmt.Errorf("otel: create lag histogram: %w", err)
	}
	return observer, nil
}

// ObserveMetric records one event. It recovers instrument panics so a
// telemetry implementation cannot fail the observed client operation.
func (o *Observer) ObserveMetric(event fgo.MetricEvent) {
	if o == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	ctx := context.Background()
	attributes := metric.WithAttributeSet(attribute.NewSet(
		attribute.Int64("fluss.event.kind", int64(event.Kind)),
		attribute.Int64("fluss.operation", int64(event.Operation)),
		attribute.Int64("fluss.error.class", int64(event.ErrorClass)),
		attribute.Int64("fluss.api.key", int64(event.APIKey)),
		attribute.Int64("fluss.server.role", int64(event.ServerRole)),
		attribute.Bool("fluss.failed", event.Failed),
	))
	o.events.Add(ctx, 1, attributes)
	if event.Failed {
		o.failures.Add(ctx, 1, attributes)
	}
	if event.Bytes > 0 {
		o.bytes.Add(ctx, event.Bytes, attributes)
	}
	if event.Records > 0 {
		o.records.Add(ctx, event.Records, attributes)
	}
	if event.Duration > 0 {
		o.duration.Record(ctx, event.Duration.Seconds(), attributes)
	}
	if event.QueueTime > 0 {
		o.queueDuration.Record(ctx, event.QueueTime.Seconds(), attributes)
	}
	if event.Kind == fgo.MetricWriteBatch && event.QueueSize >= 0 {
		o.queueSize.Record(ctx, int64(event.QueueSize), attributes)
	}
	if event.Attempt > 0 {
		o.attempts.Record(ctx, int64(event.Attempt), attributes)
	}
	if event.Kind == fgo.MetricScannerFetch && event.Lag >= 0 {
		o.lag.Record(ctx, event.Lag, attributes)
	}
}

var _ fgo.MetricsObserver = (*Observer)(nil)
