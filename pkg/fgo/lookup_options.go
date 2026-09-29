package fgo

import (
	"fmt"
	"math"
	"time"
)

// LookupConfig controls batching, concurrency, partition routing, and optional
// atomic insertion of missing rows.
type LookupConfig struct {
	// MaxBatchKeys bounds keys in one lookup request.
	MaxBatchKeys int
	// MaxInFlightRequests bounds in-flight lookup requests.
	MaxInFlightRequests int
	// MaxQueuedKeys bounds accepted keys awaiting completion across callers.
	MaxQueuedKeys int
	// BatchTimeout is the maximum time a partial cross-call batch waits.
	BatchTimeout time.Duration
	// RetryPolicy controls safe read-only lookup retries. Insert-if-not-exists
	// clients require one attempt.
	RetryPolicy RetryPolicy
	// Partition selects one named physical partition; empty selects the table.
	Partition string
	// InsertIfNotExists enables atomic insertion for missing full keys.
	InsertIfNotExists bool
	// RequestTimeout bounds each lookup request and is sent to insertion requests.
	RequestTimeout time.Duration
	// Acks is the insertion acknowledgement mode.
	Acks int32
}

// LookupOption configures a [Lookuper].
type LookupOption func(*LookupConfig) error

// WithLookupBatchLimits sets the maximum keys per request and in-flight requests.
func WithLookupBatchLimits(keys, inFlightRequests int) LookupOption {
	return func(config *LookupConfig) error {
		if keys <= 0 || inFlightRequests <= 0 {
			return fmt.Errorf("%w: lookup limits must be positive", ErrInvalidConfig)
		}
		config.MaxBatchKeys, config.MaxInFlightRequests = keys, inFlightRequests
		return nil
	}
}

// WithLookupQueue sets the accepted-key limit and batch timeout used to
// combine compatible concurrent calls.
func WithLookupQueue(keys int, batchTimeout time.Duration) LookupOption {
	return func(config *LookupConfig) error {
		if keys <= 0 || batchTimeout < 0 {
			return fmt.Errorf("%w: invalid lookup queue settings", ErrInvalidConfig)
		}
		config.MaxQueuedKeys, config.BatchTimeout = keys, batchTimeout
		return nil
	}
}

// WithLookupRetryPolicy configures bounded retries for read-only point and
// prefix lookups. Insert-if-not-exists clients cannot enable retries.
func WithLookupRetryPolicy(policy RetryPolicy) LookupOption {
	return func(config *LookupConfig) error {
		if policy.MaxAttempts < 1 || policy.MaxAttempts > maxWriterAttempts {
			return fmt.Errorf("%w: lookup retry attempts must be in [1, %d]", ErrInvalidConfig, maxWriterAttempts)
		}
		config.RetryPolicy = policy
		return nil
	}
}

// WithLookupRequestTimeout sets the client-side timeout for each lookup request.
func WithLookupRequestTimeout(timeout time.Duration) LookupOption {
	return func(config *LookupConfig) error {
		if timeout <= 0 || timeout/time.Millisecond > math.MaxInt32 {
			return fmt.Errorf("%w: invalid lookup timeout", ErrInvalidConfig)
		}
		config.RequestTimeout = timeout
		return nil
	}
}

// WithLookupPartition routes lookups to the named physical partition.
func WithLookupPartition(partition string) LookupOption {
	return func(config *LookupConfig) error {
		path := PhysicalTablePath{TablePath: TablePath{Database: "d", Table: "t"}, Partition: partition}
		if err := path.Validate(); err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

// WithLookupInsertIfNotExists atomically inserts a missing primary-key row before returning it.
// Fluss fills auto-increment columns and sets nullable non-key columns to null.
func WithLookupInsertIfNotExists(timeout time.Duration, acks int32) LookupOption {
	return func(config *LookupConfig) error {
		if timeout <= 0 || timeout/time.Millisecond > math.MaxInt32 ||
			(acks != 0 && acks != 1 && acks != -1) {
			return fmt.Errorf("%w: invalid insert-if-not-exists request settings", ErrInvalidConfig)
		}
		config.InsertIfNotExists, config.RequestTimeout, config.Acks = true, timeout, acks
		return nil
	}
}

// LookupResult is the outcome associated with one requested primary key.
type LookupResult struct {
	// Key is the requested primary key.
	Key PrimaryKey
	// Row is valid only when Found is true and Err is nil.
	Row Row
	// Found distinguishes a missing row from an empty row.
	Found bool
	// Bucket is the routed bucket when routing succeeded.
	Bucket int32
	// Err is a key-local encoding, routing, request, or decoding failure.
	Err error
}

// PrefixLookupResult contains rows matching one leading primary-key prefix.
type PrefixLookupResult struct {
	// Prefix is the requested leading primary-key prefix.
	Prefix PrimaryKey
	// Rows contains all decoded matches when Err is nil.
	Rows []Row
	// Bucket is the routed bucket when routing succeeded.
	Bucket int32
	// Err is a prefix-local encoding, routing, request, or decoding failure.
	Err error
}
