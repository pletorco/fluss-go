package fgo

import (
	"context"
	"fmt"
	"time"
)

// Codec is the stable, reflection-free mapping between an application type and a Fluss row.
type Codec[T any] interface {
	// Encode maps one application value to schema column order.
	Encode(T) (Row, error)
	// Decode maps one row to an application value without retaining the row.
	Decode(Row) (T, error)
}

// CodecFuncs adapts encode and decode functions to Codec.
type CodecFuncs[T any] struct {
	// EncodeFunc implements [Codec.Encode].
	EncodeFunc func(T) (Row, error)
	// DecodeFunc implements [Codec.Decode].
	DecodeFunc func(Row) (T, error)
}

// Encode calls EncodeFunc and rejects an unset function.
func (c CodecFuncs[T]) Encode(value T) (Row, error) {
	if c.EncodeFunc == nil {
		return nil, fmt.Errorf("%w: typed encoder is nil", ErrInvalidConfig)
	}
	return c.EncodeFunc(value)
}

// Decode calls DecodeFunc and rejects an unset function.
func (c CodecFuncs[T]) Decode(row Row) (T, error) {
	if c.DecodeFunc == nil {
		var zero T
		return zero, fmt.Errorf("%w: typed decoder is nil", ErrInvalidConfig)
	}
	return c.DecodeFunc(row)
}

// KeyCodec maps an application key to primary-key column order.
type KeyCodec[K any] interface {
	// EncodeKey maps one application key to primary-key column order.
	EncodeKey(K) (PrimaryKey, error)
}

// KeyCodecFunc adapts a function to KeyCodec.
type KeyCodecFunc[K any] func(K) (PrimaryKey, error)

// EncodeKey calls f and rejects a nil function.
func (f KeyCodecFunc[K]) EncodeKey(key K) (PrimaryKey, error) {
	if f == nil {
		return nil, fmt.Errorf("%w: typed key encoder is nil", ErrInvalidConfig)
	}
	return f(key)
}

func validateTypedCodec[T any](codec Codec[T]) error {
	if codec == nil {
		return fmt.Errorf("%w: typed codec is nil", ErrInvalidConfig)
	}
	return nil
}

func validateTypedKeyCodec[K any](codec KeyCodec[K]) error {
	if codec == nil {
		return fmt.Errorf("%w: typed key codec is nil", ErrInvalidConfig)
	}
	return nil
}

// TypedLogWriter encodes application values before appending them.
type TypedLogWriter[T any] struct {
	writer *LogWriter
	codec  Codec[T]
}

// NewTypedLogWriter opens a log writer that encodes application values with codec.
func NewTypedLogWriter[T any](
	ctx context.Context,
	client *Client,
	table Table,
	codec Codec[T],
	options ...LogWriterOption,
) (*TypedLogWriter[T], error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil client", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	writer, err := client.NewLogWriter(ctx, table, options...)
	if err != nil {
		return nil, err
	}
	return &TypedLogWriter[T]{writer: writer, codec: codec}, nil
}

func wrapTypedLogWriter[T any](writer *LogWriter, codec Codec[T]) (*TypedLogWriter[T], error) {
	if writer == nil {
		return nil, fmt.Errorf("%w: nil log writer", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	return &TypedLogWriter[T]{writer: writer, codec: codec}, nil
}

// Append encodes value and queues it using the wrapped writer.
// Encoding failures are returned through the completed future.
func (w *TypedLogWriter[T]) Append(ctx context.Context, value T) *WriteFuture {
	if w == nil || w.writer == nil {
		return completedWriteError(fmt.Errorf("%w: nil typed log writer", ErrInvalidConfig))
	}
	row, err := w.codec.Encode(value)
	if err != nil {
		return completedWriteError(err)
	}
	return w.writer.Append(ctx, row)
}

// Flush waits until all values accepted before the call have completed.
func (w *TypedLogWriter[T]) Flush(ctx context.Context) error {
	if w == nil || w.writer == nil {
		return fmt.Errorf("%w: nil typed log writer", ErrInvalidConfig)
	}
	return w.writer.Flush(ctx)
}

// Close flushes accepted values and closes the wrapped writer.
func (w *TypedLogWriter[T]) Close(ctx context.Context) error {
	if w == nil || w.writer == nil {
		return nil
	}
	return w.writer.Close(ctx)
}

// TypedKVWriter encodes application values and delete keys for a primary-key
// table.
type TypedKVWriter[T, K any] struct {
	writer   *KVWriter
	codec    Codec[T]
	keyCodec KeyCodec[K]
}

// NewTypedKVWriter opens a primary-key writer that encodes values and delete keys.
func NewTypedKVWriter[T, K any](
	ctx context.Context,
	client *Client,
	table Table,
	codec Codec[T],
	keyCodec KeyCodec[K],
	options ...KVWriterOption,
) (*TypedKVWriter[T, K], error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil client", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	if err := validateTypedKeyCodec(keyCodec); err != nil {
		return nil, err
	}
	writer, err := client.NewKVWriter(ctx, table, options...)
	if err != nil {
		return nil, err
	}
	return &TypedKVWriter[T, K]{writer: writer, codec: codec, keyCodec: keyCodec}, nil
}

func wrapTypedKVWriter[T, K any](
	writer *KVWriter,
	codec Codec[T],
	keyCodec KeyCodec[K],
) (*TypedKVWriter[T, K], error) {
	if writer == nil {
		return nil, fmt.Errorf("%w: nil KV writer", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	if err := validateTypedKeyCodec(keyCodec); err != nil {
		return nil, err
	}
	return &TypedKVWriter[T, K]{writer: writer, codec: codec, keyCodec: keyCodec}, nil
}

// Upsert encodes value and queues a primary-key upsert.
// Encoding failures are returned through the completed future.
func (w *TypedKVWriter[T, K]) Upsert(ctx context.Context, value T) *WriteFuture {
	if w == nil || w.writer == nil {
		return completedWriteError(fmt.Errorf("%w: nil typed KV writer", ErrInvalidConfig))
	}
	row, err := w.codec.Encode(value)
	if err != nil {
		return completedWriteError(err)
	}
	return w.writer.Upsert(ctx, row)
}

// Delete encodes key and queues a primary-key delete.
// Encoding failures are returned through the completed future.
func (w *TypedKVWriter[T, K]) Delete(ctx context.Context, key K) *WriteFuture {
	if w == nil || w.writer == nil {
		return completedWriteError(fmt.Errorf("%w: nil typed KV writer", ErrInvalidConfig))
	}
	primaryKey, err := w.keyCodec.EncodeKey(key)
	if err != nil {
		return completedWriteError(err)
	}
	return w.writer.Delete(ctx, primaryKey)
}

// Flush waits until all mutations accepted before the call have completed.
func (w *TypedKVWriter[T, K]) Flush(ctx context.Context) error {
	if w == nil || w.writer == nil {
		return fmt.Errorf("%w: nil typed KV writer", ErrInvalidConfig)
	}
	return w.writer.Flush(ctx)
}

// Close flushes accepted mutations and closes the wrapped writer.
func (w *TypedKVWriter[T, K]) Close(ctx context.Context) error {
	if w == nil || w.writer == nil {
		return nil
	}
	return w.writer.Close(ctx)
}

func completedWriteError(err error) *WriteFuture {
	future := newWriteFuture()
	future.complete(WriteResult{Err: err})
	return future
}

// TypedLookupResult is one decoded point-lookup outcome.
type TypedLookupResult[K, T any] struct {
	// Key is the application key associated with this result.
	Key K
	// Found reports whether Value contains a server row.
	Found bool
	// Value is valid only when Found is true and Err is nil.
	Value T
	// Err is a key-local encoding, request, or decoding failure.
	Err error
}

// TypedPrefixLookupResult contains decoded rows for one requested key prefix.
type TypedPrefixLookupResult[K, T any] struct {
	// Prefix is the application prefix associated with this result.
	Prefix K
	// Values contains decoded matches when Err is nil.
	Values []T
	// Err is a prefix-local encoding, request, or decoding failure.
	Err error
}

// TypedLookupClient encodes application keys and decodes returned rows.
type TypedLookupClient[T, K any] struct {
	lookup   *LookupClient
	codec    Codec[T]
	keyCodec KeyCodec[K]
}

// NewTypedLookupClient opens a lookup client that encodes keys and decodes rows.
func NewTypedLookupClient[T, K any](
	ctx context.Context,
	client *Client,
	table Table,
	codec Codec[T],
	keyCodec KeyCodec[K],
	options ...LookupOption,
) (*TypedLookupClient[T, K], error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil client", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	if err := validateTypedKeyCodec(keyCodec); err != nil {
		return nil, err
	}
	lookup, err := client.NewLookupClient(ctx, table, options...)
	if err != nil {
		return nil, err
	}
	return &TypedLookupClient[T, K]{lookup: lookup, codec: codec, keyCodec: keyCodec}, nil
}

func wrapTypedLookupClient[T, K any](
	lookup *LookupClient,
	codec Codec[T],
	keyCodec KeyCodec[K],
) (*TypedLookupClient[T, K], error) {
	if lookup == nil {
		return nil, fmt.Errorf("%w: nil lookup client", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	if err := validateTypedKeyCodec(keyCodec); err != nil {
		return nil, err
	}
	return &TypedLookupClient[T, K]{lookup: lookup, codec: codec, keyCodec: keyCodec}, nil
}

// Lookup returns one result per input key in input order.
// A failure for one key does not discard results for other keys.
func (c *TypedLookupClient[T, K]) Lookup(ctx context.Context, keys ...K) []TypedLookupResult[K, T] {
	results := make([]TypedLookupResult[K, T], len(keys))
	if c == nil || c.lookup == nil {
		for index, key := range keys {
			results[index] = TypedLookupResult[K, T]{
				Key: key, Err: fmt.Errorf("%w: nil typed lookup client", ErrInvalidConfig),
			}
		}
		return results
	}
	var requestKeys []PrimaryKey
	var requestIndexes []int
	for index, key := range keys {
		results[index].Key = key
		primaryKey, err := c.keyCodec.EncodeKey(key)
		if err != nil {
			results[index].Err = err
			continue
		}
		requestKeys = append(requestKeys, primaryKey)
		requestIndexes = append(requestIndexes, index)
	}
	raw := c.lookup.Lookup(ctx, requestKeys...)
	for rawIndex, item := range raw {
		index := requestIndexes[rawIndex]
		results[index].Found, results[index].Err = item.Found, item.Err
		if item.Err == nil && item.Found {
			results[index].Value, results[index].Err = c.codec.Decode(item.Row)
		}
	}
	return results
}

// PrefixLookup returns one result per input prefix in input order.
// A failure for one prefix does not discard results for other prefixes.
func (c *TypedLookupClient[T, K]) PrefixLookup(
	ctx context.Context,
	prefixes ...K,
) []TypedPrefixLookupResult[K, T] {
	results := make([]TypedPrefixLookupResult[K, T], len(prefixes))
	if c == nil || c.lookup == nil {
		for index, prefix := range prefixes {
			results[index] = TypedPrefixLookupResult[K, T]{
				Prefix: prefix, Err: fmt.Errorf("%w: nil typed lookup client", ErrInvalidConfig),
			}
		}
		return results
	}
	var requestKeys []PrimaryKey
	var requestIndexes []int
	for index, prefix := range prefixes {
		results[index].Prefix = prefix
		key, err := c.keyCodec.EncodeKey(prefix)
		if err != nil {
			results[index].Err = err
			continue
		}
		requestKeys = append(requestKeys, key)
		requestIndexes = append(requestIndexes, index)
	}
	raw := c.lookup.PrefixLookup(ctx, requestKeys...)
	for rawIndex, item := range raw {
		index := requestIndexes[rawIndex]
		results[index].Err = item.Err
		for _, row := range item.Rows {
			value, err := c.codec.Decode(row)
			if err != nil {
				results[index].Values = nil
				results[index].Err = err
				break
			}
			results[index].Values = append(results[index].Values, value)
		}
	}
	return results
}

// Close prevents new lookups and releases wrapped client resources.
func (c *TypedLookupClient[T, K]) Close() error {
	if c == nil || c.lookup == nil {
		return nil
	}
	return c.lookup.Close()
}

// TypedScanRecord is one decoded log record and its source metadata.
type TypedScanRecord[T any] struct {
	// Bucket identifies the source table bucket.
	Bucket int32
	// Value is the decoded row value.
	Value T
	// Change identifies the row mutation represented by the record.
	Change ChangeType
	// Timestamp is the server commit timestamp.
	Timestamp time.Time
	// Offset is the record offset within Bucket.
	Offset int64
}

// TypedScanResult is a decoded log poll result.
type TypedScanResult[T any] struct {
	// Records contains successfully decoded records.
	Records []TypedScanRecord[T]
	// BucketErrors contains failures that did not invalidate other buckets.
	BucketErrors []BucketScanError
	// HighWatermark maps bucket IDs to the observed log end offset.
	HighWatermark map[int32]int64
	// Done reports that configured row or stopping-offset bounds were reached.
	Done bool
}

// TypedLogScanner decodes row-based log scan results.
type TypedLogScanner[T any] struct {
	scanner *LogScanner
	codec   Codec[T]
}

// NewTypedLogScanner opens a log scanner that decodes every row with codec.
func NewTypedLogScanner[T any](
	ctx context.Context,
	client *Client,
	table Table,
	start ScanOffset,
	codec Codec[T],
	options ...LogScannerOption,
) (*TypedLogScanner[T], error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil client", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	scanner, err := client.NewLogScanner(ctx, table, start, options...)
	if err != nil {
		return nil, err
	}
	return &TypedLogScanner[T]{scanner: scanner, codec: codec}, nil
}

func wrapTypedLogScanner[T any](
	scanner *LogScanner,
	codec Codec[T],
) (*TypedLogScanner[T], error) {
	if scanner == nil {
		return nil, fmt.Errorf("%w: nil log scanner", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	return &TypedLogScanner[T]{scanner: scanner, codec: codec}, nil
}

// Poll waits for the next decoded result.
// A decode failure returns no partial decoded records.
func (s *TypedLogScanner[T]) Poll(ctx context.Context) (TypedScanResult[T], error) {
	if s == nil || s.scanner == nil {
		return TypedScanResult[T]{}, fmt.Errorf("%w: nil typed log scanner", ErrInvalidConfig)
	}
	raw, err := s.scanner.Poll(ctx)
	if err != nil {
		return TypedScanResult[T]{}, err
	}
	defer raw.Release()
	if len(raw.ArrowBatches) != 0 {
		return TypedScanResult[T]{}, fmt.Errorf(
			"%w: typed row scanner does not decode Arrow batches", ErrUnsupportedAPI,
		)
	}
	result := TypedScanResult[T]{
		Records:       make([]TypedScanRecord[T], 0, len(raw.Records)),
		BucketErrors:  append([]BucketScanError(nil), raw.BucketErrors...),
		HighWatermark: make(map[int32]int64, len(raw.HighWatermark)),
		Done:          raw.Done,
	}
	for bucket, watermark := range raw.HighWatermark {
		result.HighWatermark[bucket] = watermark
	}
	for _, item := range raw.Records {
		value, err := s.codec.Decode(item.Record.Value)
		if err != nil {
			return TypedScanResult[T]{}, err
		}
		result.Records = append(result.Records, TypedScanRecord[T]{
			Bucket: item.Bucket, Value: value, Change: item.Record.Change,
			Timestamp: item.Record.Timestamp, Offset: item.Record.Offset,
		})
	}
	return result, nil
}

// Done reports whether configured scan bounds have been reached.
func (s *TypedLogScanner[T]) Done() bool {
	return s == nil || s.scanner == nil || s.scanner.Done()
}

// Wakeup interrupts a blocked Poll with [ErrWakeup].
func (s *TypedLogScanner[T]) Wakeup() {
	if s != nil && s.scanner != nil {
		s.scanner.Wakeup()
	}
}

// Close interrupts blocked work and releases scanner resources.
func (s *TypedLogScanner[T]) Close() error {
	if s == nil || s.scanner == nil {
		return nil
	}
	return s.scanner.Close()
}

// TypedBatchResult contains decoded values from one bounded poll.
type TypedBatchResult[T any] struct {
	// Values contains successfully decoded rows from one poll.
	Values []T
	// Done reports that the bounded scan has completed.
	Done bool
}

// TypedBatchScanner decodes bounded current-state or snapshot scan rows.
type TypedBatchScanner[T any] struct {
	scanner *BatchScanner
	codec   Codec[T]
}

// NewTypedBatchScanner opens a typed current-state scanner for one table bucket.
func NewTypedBatchScanner[T any](
	ctx context.Context,
	client *Client,
	table Table,
	bucket TableBucket,
	codec Codec[T],
	options ...BatchScannerOption,
) (*TypedBatchScanner[T], error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil client", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	scanner, err := client.NewBatchScanner(ctx, table, bucket, options...)
	if err != nil {
		return nil, err
	}
	return &TypedBatchScanner[T]{scanner: scanner, codec: codec}, nil
}

// NewTypedSnapshotBatchScanner opens a typed scanner for one immutable snapshot.
func NewTypedSnapshotBatchScanner[T any](
	ctx context.Context,
	client *Client,
	table Table,
	bucket TableBucket,
	snapshotID int64,
	codec Codec[T],
	options ...BatchScannerOption,
) (*TypedBatchScanner[T], error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil client", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	scanner, err := client.NewSnapshotBatchScanner(
		ctx, table, bucket, snapshotID, options...,
	)
	if err != nil {
		return nil, err
	}
	return &TypedBatchScanner[T]{scanner: scanner, codec: codec}, nil
}

// WrapTypedBatchScanner wraps an existing row-oriented batch scanner.
func WrapTypedBatchScanner[T any](
	scanner *BatchScanner,
	codec Codec[T],
) (*TypedBatchScanner[T], error) {
	if scanner == nil {
		return nil, fmt.Errorf("%w: nil batch scanner", ErrInvalidConfig)
	}
	if err := validateTypedCodec(codec); err != nil {
		return nil, err
	}
	return &TypedBatchScanner[T]{scanner: scanner, codec: codec}, nil
}

// Poll returns the next decoded bounded-scan result.
// A decode failure returns no partial decoded values.
func (s *TypedBatchScanner[T]) Poll(ctx context.Context) (TypedBatchResult[T], error) {
	if s == nil || s.scanner == nil {
		return TypedBatchResult[T]{}, fmt.Errorf("%w: nil typed batch scanner", ErrInvalidConfig)
	}
	raw, err := s.scanner.Poll(ctx)
	if err != nil {
		return TypedBatchResult[T]{}, err
	}
	defer raw.Release()
	if len(raw.ArrowBatches) != 0 {
		return TypedBatchResult[T]{}, fmt.Errorf(
			"%w: typed batch scanner does not decode Arrow batches", ErrUnsupportedAPI,
		)
	}
	result := TypedBatchResult[T]{Values: make([]T, 0, len(raw.Rows)), Done: raw.Done}
	for _, row := range raw.Rows {
		value, err := s.codec.Decode(row)
		if err != nil {
			return TypedBatchResult[T]{}, err
		}
		result.Values = append(result.Values, value)
	}
	return result, nil
}

// Done reports whether the bounded scan has completed.
func (s *TypedBatchScanner[T]) Done() bool {
	return s == nil || s.scanner == nil || s.scanner.Done()
}

// Close closes the wrapped scanner or snapshot reader.
func (s *TypedBatchScanner[T]) Close() error {
	if s == nil || s.scanner == nil {
		return nil
	}
	return s.scanner.Close()
}
