package fgo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

const maxWriterAttempts = 100

// WriterRetryPolicy controls bounded retries of idempotent writer batches.
// The first request counts as an attempt. Retries preserve the writer ID,
// bucket sequence, and encoded batch bytes.
type WriterRetryPolicy struct {
	// MaxAttempts includes the initial request.
	MaxAttempts int
	// Backoff returns the delay before the next attempt. The argument is the
	// failed attempt number, starting at one.
	Backoff func(attempt int) time.Duration
}

func defaultWriterRetryPolicy() WriterRetryPolicy {
	return WriterRetryPolicy{
		MaxAttempts: 1,
		Backoff: func(attempt int) time.Duration {
			delay := 100 * time.Millisecond
			for current := 1; current < attempt && delay < time.Second; current++ {
				delay *= 2
			}
			if delay > time.Second {
				return time.Second
			}
			return delay
		},
	}
}

func validateWriterRetryPolicy(policy WriterRetryPolicy, acks int32) error {
	if policy.MaxAttempts < 1 || policy.MaxAttempts > maxWriterAttempts {
		return fmt.Errorf(
			"%w: writer retry attempts must be in [1, %d]",
			ErrInvalidConfig, maxWriterAttempts,
		)
	}
	if policy.MaxAttempts > 1 && acks != -1 {
		return fmt.Errorf("%w: writer retries require acks=-1", ErrInvalidConfig)
	}
	return nil
}

func writerRetryable(err error) bool {
	var server *ServerError
	if errors.As(err, &server) {
		return server.Retriable
	}
	return shouldReplaceConnection(err)
}

func duplicateWriterSequence(err error) bool {
	var server *ServerError
	return errors.As(err, &server) &&
		server.Code == fmsg.ErrorCodeDuplicateSequenceException
}

type writerAttemptResult struct {
	offset      int64
	offsetKnown bool
	attempts    int
	err         error
}

func executeWriterAttempts(
	ctx context.Context,
	policy WriterRetryPolicy,
	observer MetricsObserver,
	operation MetricOperation,
	call func(context.Context) (int64, bool, error),
) writerAttemptResult {
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		offset, offsetKnown, err := call(ctx)
		if err == nil || duplicateWriterSequence(err) {
			return writerAttemptResult{
				offset: offset, offsetKnown: offsetKnown,
				attempts: attempt, err: nil,
			}
		}
		if attempt == policy.MaxAttempts || !writerRetryable(err) || ctx.Err() != nil {
			return writerAttemptResult{
				offset: offset, offsetKnown: offsetKnown,
				attempts: attempt, err: err,
			}
		}
		observeMetric(observer, MetricEvent{
			Kind: MetricRetry, Operation: operation, Attempt: attempt + 1,
			Failed: true, ErrorClass: metricErrorClass(err),
		})
		delay := time.Duration(0)
		if policy.Backoff != nil {
			delay = policy.Backoff(attempt)
		}
		if err := waitContext(ctx, delay); err != nil {
			return writerAttemptResult{attempts: attempt, err: err}
		}
	}
	panic("unreachable writer retry loop")
}
