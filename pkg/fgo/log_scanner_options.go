package fgo

import (
	"fmt"
	"math"
	"time"
)

// LogScannerConfig controls projection, partition selection, fetch limits, and
// optional completion bounds.
type LogScannerConfig struct {
	// Projection lists returned columns in result order; nil selects all.
	Projection []string
	// Partition selects one named physical partition; empty selects the table.
	Partition string
	// FetchMaxBytes bounds aggregate encoded bytes per fetch.
	FetchMaxBytes int32
	// FetchMaxBytesForBucket bounds encoded bytes per bucket per fetch.
	FetchMaxBytesForBucket int32
	// FetchMinBytes is the aggregate byte threshold before a fetch may return.
	FetchMinBytes int32
	// FetchWaitMaxTime bounds server waiting when FetchMinBytes is not reached.
	FetchWaitMaxTime time.Duration
	// RowLimit is the total delivered row bound; zero is unbounded.
	RowLimit int64
	// StoppingOffsets maps every initial bucket to an exclusive end offset.
	StoppingOffsets map[int32]int64
}

// LogScannerOption configures a [LogScanner].
type LogScannerOption func(*LogScannerConfig) error

// WithScanProjection selects result columns in the requested order.
func WithScanProjection(columns ...string) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if len(columns) == 0 {
			return fmt.Errorf("%w: projection is empty", ErrInvalidConfig)
		}
		config.Projection = append([]string(nil), columns...)
		return nil
	}
}

// WithScanPartition scans the named physical partition.
func WithScanPartition(partition string) LogScannerOption {
	return func(config *LogScannerConfig) error {
		path := PhysicalTablePath{TablePath: TablePath{Database: "d", Table: "t"}, Partition: partition}
		if err := path.Validate(); err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

// WithScanPartitionSpec selects a partition using the table schema's partition-key order.
func WithScanPartitionSpec(schema Schema, spec PartitionSpec) LogScannerOption {
	return func(config *LogScannerConfig) error {
		partition, err := schema.PartitionName(spec)
		if err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

// WithLogFetchLimits sets aggregate and per-bucket fetch byte limits and wait time.
func WithLogFetchLimits(maxBytes, maxBytesForBucket, minBytes int32, waitMaxTime time.Duration) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if maxBytes <= 0 || maxBytesForBucket <= 0 || minBytes < 0 || minBytes > maxBytes ||
			waitMaxTime < 0 || waitMaxTime/time.Millisecond > math.MaxInt32 {
			return fmt.Errorf("%w: invalid scan limits", ErrInvalidConfig)
		}
		config.FetchMaxBytes, config.FetchMaxBytesForBucket = maxBytes, maxBytesForBucket
		config.FetchMinBytes, config.FetchWaitMaxTime = minBytes, waitMaxTime
		return nil
	}
}

// WithScanRowLimit completes a scanner after it has delivered limit rows across all buckets.
func WithScanRowLimit(limit int64) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if limit <= 0 {
			return fmt.Errorf("%w: scan row limit must be positive", ErrInvalidConfig)
		}
		config.RowLimit = limit
		return nil
	}
}

// WithScanStoppingOffsets sets the exclusive stopping offset for every initial bucket.
func WithScanStoppingOffsets(offsets map[int32]int64) LogScannerOption {
	return func(config *LogScannerConfig) error {
		if len(offsets) == 0 {
			return fmt.Errorf("%w: stopping offsets are empty", ErrInvalidConfig)
		}
		config.StoppingOffsets = make(map[int32]int64, len(offsets))
		for bucket, offset := range offsets {
			if bucket < 0 || offset < 0 {
				return fmt.Errorf("%w: invalid stopping offset for bucket %d", ErrInvalidConfig, bucket)
			}
			config.StoppingOffsets[bucket] = offset
		}
		return nil
	}
}
