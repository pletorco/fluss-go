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
	MetricOperationLookup
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
	// Kind identifies the event category.
	Kind MetricKind
	// Operation identifies the bounded high-level operation.
	Operation MetricOperation
	// APIKey is set for RPC events and zero otherwise.
	APIKey fmsg.APIKey
	// ServerType is set for server-bound events.
	ServerType ServerType
	// Duration is the completed operation duration.
	Duration time.Duration
	// QueueTime is time spent waiting before execution.
	QueueTime time.Duration
	// Attempt starts at one and is set for request and retry events.
	Attempt int
	// QueueSize is the observed bounded queue depth.
	QueueSize int
	// Records is the event record count.
	Records int64
	// Bytes is the encoded or transferred byte count.
	Bytes int64
	// Lag is a non-negative offset lag when available.
	Lag int64
	// Failed reports whether the observed operation failed.
	Failed bool
	// ErrorClass classifies failures without exposing error text.
	ErrorClass MetricErrorClass
}

// MetricsObserver receives synchronous client events.
// Implementations should return quickly and must not retain sensitive data.
type MetricsObserver interface {
	// ObserveMetric receives an event synchronously and should return quickly.
	// Panics are recovered and ignored by the client.
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
