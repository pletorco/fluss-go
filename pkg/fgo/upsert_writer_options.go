package fgo

import (
	"fmt"
	"math"
	"time"
)

// UpsertWriterConfig controls batching, buffering, acknowledgements, partition
// routing, and merge behavior for a primary-key writer.
type UpsertWriterConfig struct {
	// MaxBatchBytes bounds encoded bytes in one put request.
	MaxBatchBytes int
	// MaxBatchRecords bounds mutations in one put request.
	MaxBatchRecords int
	// MaxBuffered bounds accepted mutations awaiting completion.
	MaxBuffered int
	// MaxConcurrentRequests bounds active put calls across distinct buckets.
	MaxConcurrentRequests int
	// BatchTimeout is the maximum delay used to fill a non-full batch.
	BatchTimeout time.Duration
	// RequestTimeout bounds both the server-side put operation and the client call.
	RequestTimeout time.Duration
	// Acks is 0, 1, or -1 using the Fluss acknowledgement contract.
	Acks int32
	// RetryPolicy controls bounded retries of idempotent batches. More than one
	// attempt requires Acks=-1.
	RetryPolicy WriterRetryPolicy
	// BackpressureMaxThrottle bounds the per-bucket delay derived from successful KV pressure signals.
	BackpressureMaxThrottle time.Duration
	// Partition selects one named physical partition; empty selects the table.
	Partition string
	// MergeMode selects merge-engine or overwrite semantics.
	MergeMode MergeMode
}

// UpsertWriterOption configures a [UpsertWriter].
type UpsertWriterOption func(*UpsertWriterConfig) error

// MergeMode controls whether Fluss applies or bypasses a table's merge engine.
type MergeMode int32

const (
	// MergeModeDefault applies the table's configured merge engine.
	MergeModeDefault MergeMode = 0
	// MergeModeOverwrite bypasses the merge engine and replaces stored values.
	MergeModeOverwrite MergeMode = 1
)

func (m MergeMode) valid() bool {
	return m == MergeModeDefault || m == MergeModeOverwrite
}

// WithUpsertMergeMode sets one merge mode for every record written by the writer.
func WithUpsertMergeMode(mode MergeMode) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		if !mode.valid() {
			return fmt.Errorf("%w: unsupported KV merge mode %d", ErrInvalidConfig, mode)
		}
		config.MergeMode = mode
		return nil
	}
}

// WithUpsertBatchLimits sets maximum encoded bytes and records in one request.
func WithUpsertBatchLimits(bytes, records int) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		if bytes <= kvBatchHeaderSize || bytes > maxRowBytes || records <= 0 {
			return fmt.Errorf("%w: invalid KV batch limits", ErrInvalidConfig)
		}
		config.MaxBatchBytes, config.MaxBatchRecords = bytes, records
		return nil
	}
}

// WithUpsertBuffer bounds the number of queued records awaiting completion.
func WithUpsertBuffer(records int) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		if records <= 0 {
			return fmt.Errorf("%w: KV buffer must be positive", ErrInvalidConfig)
		}
		config.MaxBuffered = records
		return nil
	}
}

// WithUpsertConcurrency bounds active put calls across distinct buckets.
// Calls for one bucket remain strictly ordered.
func WithUpsertConcurrency(requests int) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		if requests < 1 || requests > 64 {
			return fmt.Errorf("%w: KV concurrency must be in [1, 64]", ErrInvalidConfig)
		}
		config.MaxConcurrentRequests = requests
		return nil
	}
}

// WithUpsertBatchTimeout sets the maximum delay used to collect a non-full batch.
func WithUpsertBatchTimeout(timeout time.Duration) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		if timeout < 0 {
			return fmt.Errorf("%w: negative upsert batch timeout", ErrInvalidConfig)
		}
		config.BatchTimeout = timeout
		return nil
	}
}

// WithUpsertRequest sets the server and client call timeout and acknowledgement mode.
func WithUpsertRequest(timeout time.Duration, acks int32) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		if timeout <= 0 || timeout/time.Millisecond > math.MaxInt32 ||
			(acks != 0 && acks != 1 && acks != -1) {
			return fmt.Errorf("%w: invalid KV request settings", ErrInvalidConfig)
		}
		config.RequestTimeout, config.Acks = timeout, acks
		return nil
	}
}

// WithUpsertRetryPolicy configures bounded idempotent put retries.
// Retries preserve the encoded batch, writer ID, and bucket sequence.
func WithUpsertRetryPolicy(policy WriterRetryPolicy) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		config.RetryPolicy = policy
		return nil
	}
}

// WithUpsertBackpressureMaxThrottle sets the maximum per-bucket pressure delay.
// Positive pressure p applies a quadratic delay of max*p*p before the next batch.
func WithUpsertBackpressureMaxThrottle(maximum time.Duration) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		if maximum < 0 || maximum > time.Minute {
			return fmt.Errorf("%w: KV backpressure throttle must be between zero and one minute", ErrInvalidConfig)
		}
		config.BackpressureMaxThrottle = maximum
		return nil
	}
}

// WithUpsertPartition routes writes to the named physical partition.
func WithUpsertPartition(partition string) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		path := PhysicalTablePath{TablePath: TablePath{Database: "d", Table: "t"}, Partition: partition}
		if err := path.Validate(); err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

// WithUpsertPartitionSpec selects a partition using the table schema's partition-key order.
func WithUpsertPartitionSpec(schema Schema, spec PartitionSpec) UpsertWriterOption {
	return func(config *UpsertWriterConfig) error {
		partition, err := schema.PartitionName(spec)
		if err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}
