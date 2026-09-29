package fgo

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"time"
)

// ErrNotFound reports a missing primary-key row.
var ErrNotFound = fmt.Errorf("fgo: record not found")

// Lookuper performs batched point and prefix lookups for a primary-key
// table. A Lookuper is safe for concurrent calls and must be closed.
type Lookuper struct {
	table       Table
	path        PhysicalTablePath
	backend     lookupBackend
	config      LookupConfig
	tableID     int64
	partitionID int64
	buckets     []int32
	resolver    schemaResolver
	observer    MetricsObserver

	mu     sync.RWMutex
	closed bool
	life   context.Context
	cancel context.CancelFunc
	queue  chan *lookupTask
	jobs   chan lookupBatch
	slots  chan struct{}
	done   chan struct{}
}

// NewLookuper creates a point and prefix lookuper for table.
func (c *Client) NewLookuper(ctx context.Context, table Table, options ...LookupOption) (*Lookuper, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	lookup, err := newLookuper(ctx, clientLookupBackend{client: c}, table, options...)
	if err == nil {
		lookup.observer = c.observer
	}
	return lookup, err
}

func newLookuper(ctx context.Context, backend lookupBackend, table Table, options ...LookupOption) (*Lookuper, error) {
	if err := table.RequirePrimaryKey(); err != nil {
		return nil, err
	}
	if err := table.Schema.Validate(); err != nil {
		return nil, err
	}
	config, err := lookupConfig(options)
	if err != nil {
		return nil, err
	}
	if err := validateLookupInsertSchema(table.Schema, config); err != nil {
		return nil, err
	}
	path := PhysicalTablePath{TablePath: table.Path, Partition: config.Partition}
	physicalID, locations, err := backend.metadata(ctx, path)
	if err != nil {
		return nil, err
	}
	if path.Partition == "" && physicalID != table.ID {
		return nil, fmt.Errorf("%w: metadata table ID %d does not match opened table %d", ErrMetadata, physicalID, table.ID)
	}
	buckets, err := sortedBuckets(locations)
	if err != nil {
		return nil, err
	}
	client := &Lookuper{
		table: table, path: path, backend: backend, config: config, tableID: table.ID,
		partitionID: -1, buckets: buckets, resolver: resolverFor(backend, table),
		queue: make(chan *lookupTask, config.MaxQueuedKeys),
		jobs:  make(chan lookupBatch, config.MaxInFlightRequests),
		slots: make(chan struct{}, config.MaxQueuedKeys),
		done:  make(chan struct{}),
	}
	if path.Partition != "" {
		client.partitionID = physicalID
	}
	client.life, client.cancel = context.WithCancel(context.Background()) // NOSONAR: Close owns the lookuper lifecycle.
	go client.runScheduler()
	return client, nil
}

func lookupConfig(options []LookupOption) (LookupConfig, error) {
	config := LookupConfig{
		MaxBatchKeys: 1000, MaxInFlightRequests: 8,
		MaxQueuedKeys: 10_000, BatchTimeout: time.Millisecond,
		RetryPolicy:    RetryPolicy{MaxAttempts: 1},
		RequestTimeout: 30 * time.Second, Acks: -1,
	}
	for _, option := range options {
		if option == nil {
			return LookupConfig{}, fmt.Errorf("%w: nil lookup option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return LookupConfig{}, err
		}
	}
	if config.InsertIfNotExists && config.RetryPolicy.MaxAttempts > 1 {
		return LookupConfig{}, fmt.Errorf(
			"%w: insert-if-not-exists lookup retries are unsafe",
			ErrInvalidConfig,
		)
	}
	return config, nil
}

func validateLookupInsertSchema(schema Schema, config LookupConfig) error {
	if !config.InsertIfNotExists {
		return nil
	}
	for _, column := range schema.Columns {
		if !column.Nullable && !slices.Contains(schema.PrimaryKey, column.Name) &&
			!slices.Contains(schema.AutoIncrement, column.Name) {
			return fmt.Errorf(
				"%w: insert-if-not-exists cannot fill required column %q",
				ErrInvalidSchema, column.Name,
			)
		}
	}
	return nil
}

type lookupMode uint8

const (
	pointLookupMode lookupMode = iota
	prefixLookupMode
)

type lookupTask struct {
	ctx     context.Context
	mode    lookupMode
	bucket  int32
	encoded []byte
	result  chan rawLookupResult
	release func()
	once    sync.Once
}

type rawLookupResult struct {
	value []byte
	rows  [][]byte
	err   error
}

func (t *lookupTask) complete(result rawLookupResult) {
	t.once.Do(func() {
		t.result <- result
		if t.release != nil {
			t.release()
		}
	})
}

type lookupGroup struct {
	mode   lookupMode
	bucket int32
}

type lookupBatch struct {
	group lookupGroup
	tasks []*lookupTask
}

type lookupScheduler struct {
	client  *Lookuper
	groups  map[lookupGroup][]*lookupTask
	timer   *time.Timer
	timerC  <-chan time.Time
	workers sync.WaitGroup
}

// Lookup returns one result for each input key in input order.
func (c *Lookuper) Lookup(ctx context.Context, keys ...PrimaryKey) []LookupResult {
	results := make([]LookupResult, len(keys))
	if len(keys) == 0 {
		return results
	}
	if err := c.lookupContextError(ctx); err != nil {
		for index, key := range keys {
			results[index] = LookupResult{Key: key, Err: err}
		}
		return results
	}
	tasks := make([]*lookupTask, len(keys))
	for index, key := range keys {
		results[index].Key = key
		encoded, bucket, err := c.encodePoint(key)
		if err != nil {
			results[index].Err = err
			continue
		}
		results[index].Bucket = bucket
		tasks[index], results[index].Err = c.enqueueLookup(ctx, pointLookupMode, bucket, encoded)
	}
	for index, task := range tasks {
		if task == nil {
			continue
		}
		select {
		case raw := <-task.result:
			if raw.err != nil {
				results[index].Err = raw.err
			} else if raw.value == nil {
				results[index].Err = ErrNotFound
			} else {
				results[index].Row, results[index].Err = decodeLookupValueWithResolver(
					ctx, c.resolver, c.table, raw.value,
				)
				results[index].Found = results[index].Err == nil
			}
		case <-ctx.Done():
			results[index].Err = ctx.Err()
		}
	}
	return results
}

// PrefixLookup returns one result for each leading primary-key prefix in input
// order.
func (c *Lookuper) PrefixLookup(ctx context.Context, prefixes ...PrimaryKey) []PrefixLookupResult {
	results := make([]PrefixLookupResult, len(prefixes))
	if len(prefixes) == 0 {
		return results
	}
	if c.config.InsertIfNotExists {
		for index, prefix := range prefixes {
			results[index] = PrefixLookupResult{
				Prefix: prefix,
				Err:    fmt.Errorf("%w: insert-if-not-exists does not support prefix lookup", ErrInvalidConfig),
			}
		}
		return results
	}
	if err := c.lookupContextError(ctx); err != nil {
		for index, prefix := range prefixes {
			results[index] = PrefixLookupResult{Prefix: prefix, Err: err}
		}
		return results
	}
	tasks := make([]*lookupTask, len(prefixes))
	for index, prefix := range prefixes {
		results[index].Prefix = prefix
		encoded, bucket, err := c.encodePrefix(prefix)
		if err != nil {
			results[index].Err = err
			continue
		}
		results[index].Bucket = bucket
		tasks[index], results[index].Err = c.enqueueLookup(ctx, prefixLookupMode, bucket, encoded)
	}
	for index, task := range tasks {
		if task == nil {
			continue
		}
		c.awaitPrefixLookup(ctx, task, &results[index])
	}
	return results
}

func (c *Lookuper) awaitPrefixLookup(
	ctx context.Context,
	task *lookupTask,
	result *PrefixLookupResult,
) {
	select {
	case raw := <-task.result:
		if raw.err != nil {
			result.Err = raw.err
			return
		}
		for _, value := range raw.rows {
			row, err := decodeLookupValueWithResolver(ctx, c.resolver, c.table, value)
			if err != nil {
				result.Err = err
				return
			}
			result.Rows = append(result.Rows, row)
		}
	case <-ctx.Done():
		result.Err = ctx.Err()
	}
}

func (c *Lookuper) lookupContextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	return ctx.Err()
}

func (c *Lookuper) enqueueLookup(
	ctx context.Context,
	mode lookupMode,
	bucket int32,
	encoded []byte,
) (*lookupTask, error) {
	if err := c.lookupContextError(ctx); err != nil {
		return nil, err
	}
	select {
	case c.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.life.Done():
		return nil, ErrClosed
	}
	task := &lookupTask{
		ctx: ctx, mode: mode, bucket: bucket, encoded: encoded,
		result: make(chan rawLookupResult, 1),
		release: func() {
			<-c.slots
		},
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		task.complete(rawLookupResult{err: ErrClosed})
		return nil, ErrClosed
	}
	select {
	case c.queue <- task:
		c.mu.RUnlock()
		return task, nil
	case <-ctx.Done():
		c.mu.RUnlock()
		task.complete(rawLookupResult{err: ctx.Err()})
		return nil, ctx.Err()
	}
}

func (c *Lookuper) encodePoint(key PrimaryKey) ([]byte, int32, error) {
	encoded, err := EncodeLookupKey(c.table.Schema, key, 1)
	if err != nil {
		return nil, 0, err
	}
	values := make(map[string]any, len(key))
	for index, name := range c.table.Schema.PrimaryKey {
		values[name] = key[index]
	}
	bucket, err := c.bucketForValues(values)
	return encoded, bucket, err
}

func (c *Lookuper) encodePrefix(prefix PrimaryKey) ([]byte, int32, error) {
	bucketNames := c.table.Schema.BucketKey
	if len(bucketNames) == 0 {
		bucketNames = c.table.Schema.PrimaryKey
	}
	if len(prefix) < len(bucketNames) {
		return nil, 0, fmt.Errorf("%w: prefix does not contain the complete bucket key", ErrInvalidRow)
	}
	for index, name := range bucketNames {
		if index >= len(c.table.Schema.PrimaryKey) || c.table.Schema.PrimaryKey[index] != name {
			return nil, 0, fmt.Errorf("%w: bucket key is not a leading primary-key prefix", ErrInvalidSchema)
		}
	}
	encoded, err := EncodePrefixLookupKey(c.table.Schema, prefix, 1)
	if err != nil {
		return nil, 0, err
	}
	values := make(map[string]any, len(prefix))
	for index := range prefix {
		values[c.table.Schema.PrimaryKey[index]] = prefix[index]
	}
	bucket, err := c.bucketForValues(values)
	return encoded, bucket, err
}

func (c *Lookuper) bucketForValues(values map[string]any) (int32, error) {
	names := c.table.Schema.BucketKey
	if len(names) == 0 {
		names = c.table.Schema.PrimaryKey
	}
	key := make(PrimaryKey, len(names))
	for index, name := range names {
		key[index] = values[name]
	}
	encoded, err := encodeKeyColumns(c.table.Schema, names, key)
	if err != nil {
		return 0, err
	}
	bucket, err := flussBucket(encoded, len(c.buckets))
	if err != nil {
		return 0, err
	}
	return c.buckets[int(bucket)], nil
}

func decodeLookupValueWithResolver(
	ctx context.Context,
	resolver schemaResolver,
	table Table,
	value []byte,
) (Row, error) {
	if len(value) < 2 {
		return nil, fmt.Errorf("%w: lookup value omits schema ID", ErrMalformedRow)
	}
	schemaID := int32(int16(binary.LittleEndian.Uint16(value)))
	schema, err := resolver.resolveSchema(ctx, table.Path, schemaID)
	if err != nil {
		return nil, fmt.Errorf("fgo: resolve lookup schema %d: %w", schemaID, err)
	}
	row, err := DecodeCompactedRow(schema, value[2:])
	if err != nil {
		return nil, err
	}
	return evolveRow(schema, table.Schema, row)
}

// Close cancels active requests and rejects new lookups.
// Close is idempotent.
func (c *Lookuper) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.cancel()
	}
	c.mu.Unlock()
	<-c.done
	return nil
}
