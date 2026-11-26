package fgo

import (
	"context"
	"fmt"
	"math"
	"reflect"
)

// BatchScannerOption configures a bounded current-state or snapshot scan.
type BatchScannerOption func(*BatchScannerConfig) error

// BatchScannerConfig contains limits and projection for a bounded scan.
type BatchScannerConfig struct {
	// Limit is the maximum rows requested per poll.
	Limit int
	// Projection lists returned columns in result order; nil selects all.
	Projection []string
	// BatchSizeBytes bounds each Fluss 1.0 KV scan-session response.
	BatchSizeBytes int32
}

// WithBatchLimit bounds the number of rows returned by a current-state request or snapshot poll.
func WithBatchLimit(limit int) BatchScannerOption {
	return func(config *BatchScannerConfig) error {
		if limit <= 0 || limit > math.MaxInt32 {
			return fmt.Errorf("%w: invalid batch scan limit", ErrInvalidConfig)
		}
		config.Limit = limit
		return nil
	}
}

// WithBatchProjection selects result columns in the requested order.
func WithBatchProjection(columns ...string) BatchScannerOption {
	return func(config *BatchScannerConfig) error {
		if len(columns) == 0 {
			return fmt.Errorf("%w: batch projection is empty", ErrInvalidConfig)
		}
		config.Projection = append([]string(nil), columns...)
		return nil
	}
}

// WithBatchSizeBytes bounds each server-side KV scan-session response.
func WithBatchSizeBytes(bytes int32) BatchScannerOption {
	return func(config *BatchScannerConfig) error {
		if bytes <= 0 {
			return fmt.Errorf("%w: batch scan byte limit must be positive", ErrInvalidConfig)
		}
		config.BatchSizeBytes = bytes
		return nil
	}
}

// BatchResult owns any Arrow batches it contains. Call Release after consuming the result.
type BatchResult struct {
	// Rows contains row-oriented results.
	Rows []Row
	// ArrowBatches contains owned Arrow results released by [BatchResult.Release].
	ArrowBatches []ArrowLogBatch
	// Done reports that no additional rows remain.
	Done bool
}

// Release frees Arrow records owned by the result.
// Release is safe to call more than once.
func (r *BatchResult) Release() {
	if r == nil {
		return
	}
	for index := range r.ArrowBatches {
		r.ArrowBatches[index].Release()
	}
	r.ArrowBatches = nil
}

// SnapshotBatchRequest identifies one immutable primary-key snapshot.
type SnapshotBatchRequest struct {
	// Table is the authoritative table metadata for the snapshot.
	Table Table
	// Bucket identifies the physical bucket being read.
	Bucket TableBucket
	// SnapshotID identifies the immutable server snapshot.
	SnapshotID int64
	// Projection lists requested columns in result order. Providers may
	// apply it when it selects fewer columns than Table.Schema.
	Projection []string
	// Limit is the maximum rows requested from one reader call.
	Limit int
}

// SnapshotBatchReader streams rows from one immutable snapshot. io.EOF marks completion.
type SnapshotBatchReader interface {
	// ReadBatch returns at most limit rows in a consistent layout. Full rows
	// use table schema order. A narrower projection may be returned directly
	// in requested column order. io.EOF may accompany a final non-empty batch.
	// ReadBatch must honor context cancellation and return promptly so the
	// scanner can safely close the reader.
	ReadBatch(context.Context, int) ([]Row, error)
	// Close releases reader resources, including after EOF or a read error.
	// The scanner calls Close once, after any active ReadBatch returns.
	Close() error
}

// An interface holding a typed nil is not itself nil. Neither is a valid
// reader: a successful open must supply a value whose methods can be called.
func isNilSnapshotReader(reader SnapshotBatchReader) bool {
	value := reflect.ValueOf(reader)
	switch value.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// SnapshotBatchProvider opens readers for implementation-specific Fluss snapshot storage.
type SnapshotBatchProvider interface {
	// OpenSnapshot returns a new reader owned by the caller. Neither a nil
	// interface nor a typed-nil reader is a successful result.
	OpenSnapshot(context.Context, SnapshotBatchRequest) (SnapshotBatchReader, error)
}

// SnapshotBatchProviderFunc adapts a function to SnapshotBatchProvider.
type SnapshotBatchProviderFunc func(context.Context, SnapshotBatchRequest) (SnapshotBatchReader, error)

// OpenSnapshot calls f with the requested snapshot and projection.
func (f SnapshotBatchProviderFunc) OpenSnapshot(
	ctx context.Context,
	request SnapshotBatchRequest,
) (SnapshotBatchReader, error) {
	return f(ctx, request)
}

// WithSnapshotBatchProvider configures explicit snapshot-ID scans.
func WithSnapshotBatchProvider(provider SnapshotBatchProvider) Option {
	return func(config *config) error {
		if provider == nil {
			return fmt.Errorf("%w: nil snapshot batch provider", ErrInvalidConfig)
		}
		config.snapshotProvider = provider
		return nil
	}
}
