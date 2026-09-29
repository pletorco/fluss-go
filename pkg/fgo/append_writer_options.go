package fgo

import (
	"fmt"
	"math"
	"time"
)

// AppendWriterConfig controls batching, buffering, acknowledgements, routing, and
// encoding for an append writer.
type AppendWriterConfig struct {
	// MaxBatchBytes bounds encoded bytes in one produce request.
	MaxBatchBytes int
	// MaxBatchRecords bounds records in one produce request.
	MaxBatchRecords int
	// MaxBuffered bounds accepted records awaiting completion.
	MaxBuffered int
	// MaxConcurrentRequests bounds active produce calls across distinct buckets.
	MaxConcurrentRequests int
	// BatchTimeout is the maximum delay used to fill a non-full batch.
	BatchTimeout time.Duration
	// RequestTimeout bounds both the server-side produce operation and the client call.
	RequestTimeout time.Duration
	// Acks is 0, 1, or -1 using the Fluss acknowledgement contract.
	Acks int32
	// RetryPolicy controls bounded retries of idempotent batches. More than one
	// attempt requires Acks=-1.
	RetryPolicy WriterRetryPolicy
	// NoKeyAssigner selects routing for records without a bucket key.
	NoKeyAssigner NoKeyAssigner
	// Partition selects one named physical partition; empty selects the table.
	Partition string
	// Format selects row or Arrow encoding.
	Format LogFormat
	// ArrowCompressionType applies only when Format resolves to Arrow.
	ArrowCompressionType ArrowCompressionType

	arrowCompressionSet bool
}

// AppendWriterOption configures a [AppendWriter].
type AppendWriterOption func(*AppendWriterConfig) error

// WithAppendBatchLimits sets maximum encoded bytes and records in one request.
func WithAppendBatchLimits(bytes, records int) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		if bytes <= logBatchV0HeaderSize || bytes > maxRowBytes || records <= 0 {
			return fmt.Errorf("%w: invalid log batch limits", ErrInvalidConfig)
		}
		config.MaxBatchBytes, config.MaxBatchRecords = bytes, records
		return nil
	}
}

// WithAppendBuffer bounds the number of queued records awaiting completion.
func WithAppendBuffer(records int) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		if records <= 0 {
			return fmt.Errorf("%w: log buffer must be positive", ErrInvalidConfig)
		}
		config.MaxBuffered = records
		return nil
	}
}

// WithAppendConcurrency bounds active produce calls across distinct buckets.
// Calls for one bucket remain strictly ordered.
func WithAppendConcurrency(requests int) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		if requests < 1 || requests > 64 {
			return fmt.Errorf("%w: log concurrency must be in [1, 64]", ErrInvalidConfig)
		}
		config.MaxConcurrentRequests = requests
		return nil
	}
}

// WithAppendBatchTimeout sets the maximum delay used to collect a non-full batch.
func WithAppendBatchTimeout(timeout time.Duration) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		if timeout < 0 {
			return fmt.Errorf("%w: negative append batch timeout", ErrInvalidConfig)
		}
		config.BatchTimeout = timeout
		return nil
	}
}

// WithAppendRequest sets the server and client call timeout and acknowledgement mode.
func WithAppendRequest(timeout time.Duration, acks int32) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		if timeout <= 0 || timeout/time.Millisecond > math.MaxInt32 ||
			(acks != 0 && acks != 1 && acks != -1) {
			return fmt.Errorf("%w: invalid log request settings", ErrInvalidConfig)
		}
		config.RequestTimeout, config.Acks = timeout, acks
		return nil
	}
}

// WithAppendRetryPolicy configures bounded idempotent produce retries.
// Retries preserve the encoded batch, writer ID, and bucket sequence.
func WithAppendRetryPolicy(policy WriterRetryPolicy) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		config.RetryPolicy = policy
		return nil
	}
}

// WithAppendNoKeyAssigner selects routing for rows without a bucket key.
func WithAppendNoKeyAssigner(assigner NoKeyAssigner) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		switch assigner {
		case NoKeyAssignerSticky, NoKeyAssignerRoundRobin:
			config.NoKeyAssigner = assigner
			return nil
		default:
			return fmt.Errorf("%w: unknown no-key assigner %q", ErrInvalidConfig, assigner)
		}
	}
}

// WithAppendPartition routes writes to the named physical partition.
func WithAppendPartition(partition string) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		path := PhysicalTablePath{TablePath: TablePath{Database: "d", Table: "t"}, Partition: partition}
		if err := path.Validate(); err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

// WithAppendPartitionSpec selects a partition using the table schema's partition-key order.
func WithAppendPartitionSpec(schema Schema, spec PartitionSpec) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		partition, err := schema.PartitionName(spec)
		if err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

// WithAppendLogFormat selects the server-supported encoding used by this writer.
func WithAppendLogFormat(format LogFormat) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		switch format {
		case LogFormatAuto, LogFormatArrow, LogFormatIndexed, LogFormatCompacted:
			config.Format = format
			return nil
		default:
			return fmt.Errorf("%w: unsupported log write format %q", ErrInvalidConfig, format)
		}
	}
}

// WithAppendArrowCompression selects the compression for Arrow log batches.
func WithAppendArrowCompression(compression ArrowCompressionType) AppendWriterOption {
	return func(config *AppendWriterConfig) error {
		switch compression {
		case ArrowCompressionNone, ArrowCompressionLZ4, ArrowCompressionZSTD:
			config.ArrowCompressionType = compression
			config.arrowCompressionSet = true
			return nil
		default:
			return fmt.Errorf("%w: unsupported Arrow compression %d", ErrInvalidConfig, compression)
		}
	}
}
