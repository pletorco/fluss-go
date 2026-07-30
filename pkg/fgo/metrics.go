package fgo

import (
	"context"
	"errors"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

type MetricKind uint8

const (
	MetricRequest MetricKind = iota
	MetricRetry
	MetricRemoteIO
	MetricWriteBatch
	MetricScannerFetch
	MetricDecodeFailure
	MetricThrottle
)

type MetricOperation uint8

const (
	MetricOperationRPC MetricOperation = iota
	MetricOperationDial
	MetricOperationLogWrite
	MetricOperationKVWrite
	MetricOperationLogScan
	MetricOperationRemoteRead
)

type MetricErrorClass uint8

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

type MetricsObserver interface {
	ObserveMetric(MetricEvent)
}

type MetricsObserverFunc func(MetricEvent)

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
