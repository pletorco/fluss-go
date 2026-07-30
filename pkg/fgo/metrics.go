package fgo

import (
	"context"
	"errors"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

// MetricKind identifies the client activity represented by an event.
type MetricKind uint8

// Metric event kinds emitted by the client.
const (
	MetricRequest MetricKind = iota
	MetricRetry
	MetricRemoteIO
	MetricWriteBatch
	MetricScannerFetch
	MetricDecodeFailure
	MetricThrottle
)

// MetricOperation identifies the high-level operation producing an event.
type MetricOperation uint8

// Metric operations emitted by the client.
const (
	MetricOperationRPC MetricOperation = iota
	MetricOperationDial
	MetricOperationLogWrite
	MetricOperationKVWrite
	MetricOperationLogScan
	MetricOperationRemoteRead
)

// MetricErrorClass is a bounded-cardinality failure classification.
type MetricErrorClass uint8

// Metric error classes emitted by the client.
const (
	MetricErrorNone MetricErrorClass = iota
	MetricErrorCanceled
	MetricErrorTimeout
	MetricErrorClosed
	MetricErrorServer
	MetricErrorProtocol
	MetricErrorOther
)

// MetricEvent contains bounded-cardinality measurements and never includes request payloads,
// addresses, table paths, bucket IDs, credentials, or server error messages.
type MetricEvent struct {
	Kind       MetricKind
	Operation  MetricOperation
	APIKey     fmsg.APIKey
	ServerRole ServerRole
	Duration   time.Duration
	QueueTime  time.Duration
	Attempt    int
	QueueSize  int
	Records    int64
	Bytes      int64
	Lag        int64
	Failed     bool
	ErrorClass MetricErrorClass
}

// MetricsObserver receives synchronous client events.
// Implementations should return quickly and must not retain sensitive data.
type MetricsObserver interface {
	ObserveMetric(MetricEvent)
}

// MetricsObserverFunc adapts a function to [MetricsObserver].
type MetricsObserverFunc func(MetricEvent)

// ObserveMetric calls f with event.
func (f MetricsObserverFunc) ObserveMetric(event MetricEvent) { f(event) }

func observeMetric(observer MetricsObserver, event MetricEvent) {
	if observer == nil {
		return
	}
	func() {
		defer func() {
			_ = recover()
		}()
		observer.ObserveMetric(event)
	}()
}

func metricErrorClass(err error) MetricErrorClass {
	switch {
	case err == nil:
		return MetricErrorNone
	case errors.Is(err, context.Canceled):
		return MetricErrorCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return MetricErrorTimeout
	case errors.Is(err, ErrClosed):
		return MetricErrorClosed
	}
	var server *ServerError
	if errors.As(err, &server) {
		return MetricErrorServer
	}
	if errors.Is(err, ErrValidation) || errors.Is(err, ErrMalformedRow) ||
		errors.Is(err, ErrMalformedRecordBatch) || errors.Is(err, ErrInvalidSchema) {
		return MetricErrorProtocol
	}
	return MetricErrorOther
}

func metricStart(observer MetricsObserver) time.Time {
	if observer == nil {
		return time.Time{}
	}
	return time.Now()
}

func metricDuration(start time.Time) time.Duration {
	if start.IsZero() {
		return 0
	}
	return time.Since(start)
}
