package fgo

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

func TestWriterRetryPolicyValidation(t *testing.T) {
	tests := []struct {
		name   string
		policy WriterRetryPolicy
		acks   int32
		valid  bool
	}{
		{"single attempt with leader ack", WriterRetryPolicy{MaxAttempts: 1}, 1, true},
		{"retries with all replicas", WriterRetryPolicy{MaxAttempts: 2}, -1, true},
		{"zero attempts", WriterRetryPolicy{}, -1, false},
		{"too many attempts", WriterRetryPolicy{MaxAttempts: maxWriterAttempts + 1}, -1, false},
		{"unsafe acknowledgements", WriterRetryPolicy{MaxAttempts: 2}, 1, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateWriterRetryPolicy(test.policy, test.acks)
			if (err == nil) != test.valid {
				t.Fatalf("validateWriterRetryPolicy() error = %v", err)
			}
		})
	}
}

func TestDefaultWriterRetryBackoffAndTransportClassification(t *testing.T) {
	policy := defaultWriterRetryPolicy()
	if policy.MaxAttempts != 1 ||
		policy.Backoff(1) != 100*time.Millisecond ||
		policy.Backoff(4) != 800*time.Millisecond ||
		policy.Backoff(8) != time.Second {
		t.Fatalf("defaultWriterRetryPolicy() = %#v", policy)
	}
	if !writerRetryable(io.EOF) || writerRetryable(errors.New("application failure")) {
		t.Fatal("transport retry classification is incorrect")
	}
}

func TestExecuteWriterAttemptsRequiresValidatedPolicy(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("zero-attempt policy did not panic")
		}
	}()
	executeWriterAttempts(
		context.Background(), WriterRetryPolicy{}, nil, MetricOperationLogWrite,
		func(context.Context) (int64, bool, error) { return 0, false, nil },
	)
}

func TestExecuteWriterAttemptsRecoversDuplicateSequence(t *testing.T) {
	attempt := 0
	result := executeWriterAttempts(
		context.Background(), WriterRetryPolicy{MaxAttempts: 2}, nil,
		MetricOperationLogWrite,
		func(context.Context) (int64, bool, error) {
			attempt++
			if attempt == 1 {
				return 0, false, responseServerError(
					int32(fmsg.ErrorCodeNetworkException), "lost response",
					fmsg.APIKeyProduceLog,
				)
			}
			return 0, false, responseServerError(
				int32(fmsg.ErrorCodeDuplicateSequenceException), "already committed",
				fmsg.APIKeyProduceLog,
			)
		},
	)
	if result.err != nil || result.offsetKnown || result.attempts != 2 {
		t.Fatalf("executeWriterAttempts() = %#v", result)
	}
}

func TestExecuteWriterAttemptsStopsAtUnsafeError(t *testing.T) {
	attempts := 0
	result := executeWriterAttempts(
		context.Background(), WriterRetryPolicy{MaxAttempts: 3}, nil,
		MetricOperationKVWrite,
		func(context.Context) (int64, bool, error) {
			attempts++
			return 0, false, responseServerError(
				int32(fmsg.ErrorCodeOutOfOrderSequenceException), "sequence gap",
				fmsg.APIKeyPutKv,
			)
		},
	)
	if result.err == nil || attempts != 1 || !errors.Is(result.err, ErrSequence) {
		t.Fatalf("executeWriterAttempts() = %#v after %d attempts", result, attempts)
	}
}

func TestExecuteWriterAttemptsObservesRetryAndBackoffCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	observer := &metricRecorder{}
	result := executeWriterAttempts(
		ctx, WriterRetryPolicy{
			MaxAttempts: 2,
			Backoff: func(int) time.Duration {
				cancel()
				return time.Hour
			},
		},
		observer, MetricOperationKVWrite,
		func(context.Context) (int64, bool, error) {
			return 0, false, responseServerError(
				int32(fmsg.ErrorCodeRequestTimeOut), "retry",
				fmsg.APIKeyPutKv,
			)
		},
	)
	if !errors.Is(result.err, context.Canceled) || result.attempts != 1 {
		t.Fatalf("executeWriterAttempts() = %#v", result)
	}
	event, ok := observer.find(MetricRetry, MetricOperationKVWrite)
	if !ok || event.Attempt != 2 || !event.Failed {
		t.Fatalf("retry metric = %#v, %t", event, ok)
	}
}
