package fgo

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// ErrNotFound reports a missing primary-key row.
var ErrNotFound = fmt.Errorf("fgo: record not found")

// LookupConfig controls batching, concurrency, partition routing, and optional
// atomic insertion of missing rows.
type LookupConfig struct {
	// MaxBatchKeys bounds keys in one lookup request.
	MaxBatchKeys int
	// MaxConcurrent bounds in-flight lookup requests.
	MaxConcurrent int
	// MaxQueuedKeys bounds accepted keys awaiting completion across callers.
	MaxQueuedKeys int
	// BatchDelay is the maximum time a partial cross-call batch waits.
	BatchDelay time.Duration
	// Retry controls safe read-only lookup retries. Insert-if-not-exists
	// clients require one attempt.
	Retry RetryPolicy
	// Partition selects one named physical partition; empty selects the table.
	Partition string
	// InsertIfNotExists enables atomic insertion for missing full keys.
	InsertIfNotExists bool
	// Timeout bounds each lookup request and is sent to insertion requests.
	Timeout time.Duration
	// Acks is the insertion acknowledgement mode.
	Acks int32
}

// LookupOption configures a [LookupClient].
type LookupOption func(*LookupConfig) error

// WithLookupBatch sets the maximum keys per request and concurrent requests.
func WithLookupBatch(keys, concurrent int) LookupOption {
	return func(config *LookupConfig) error {
		if keys <= 0 || concurrent <= 0 {
			return fmt.Errorf("%w: lookup limits must be positive", ErrInvalidConfig)
		}
		config.MaxBatchKeys, config.MaxConcurrent = keys, concurrent
		return nil
	}
}

// WithLookupQueue sets the accepted-key limit and partial-batch delay used to
// combine compatible concurrent calls.
func WithLookupQueue(keys int, delay time.Duration) LookupOption {
	return func(config *LookupConfig) error {
		if keys <= 0 || delay < 0 {
			return fmt.Errorf("%w: invalid lookup queue settings", ErrInvalidConfig)
		}
		config.MaxQueuedKeys, config.BatchDelay = keys, delay
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
		config.Retry = policy
		return nil
	}
}

// WithLookupTimeout sets the client-side timeout for each lookup request.
func WithLookupTimeout(timeout time.Duration) LookupOption {
	return func(config *LookupConfig) error {
		if timeout <= 0 || timeout/time.Millisecond > math.MaxInt32 {
			return fmt.Errorf("%w: invalid lookup timeout", ErrInvalidConfig)
		}
		config.Timeout = timeout
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
		config.InsertIfNotExists, config.Timeout, config.Acks = true, timeout, acks
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

type lookupBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error)
	lookup(context.Context, lookupRequest) ([][]byte, error)
	prefixLookup(context.Context, PhysicalTablePath, int32, int64, int64, [][]byte) ([][][]byte, error)
}

type clientLookupBackend struct{ client *Client }

func (b clientLookupBackend) schemaResolver() schemaResolver {
	if b.client == nil || b.client.schemas == nil {
		return nil
	}
	return b.client
}

type lookupRequest struct {
	path             PhysicalTablePath
	bucket           int32
	tableID          int64
	partitionID      int64
	keys             [][]byte
	insertIfNotExist bool
	timeout          time.Duration
	acks             int32
}

func (b clientLookupBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]Node, error) {
	return (clientLogWriterBackend{client: b.client}).metadata(ctx, path)
}

func (b clientLookupBackend) lookup(
	ctx context.Context,
	input lookupRequest,
) ([][]byte, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyLookup, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.LookupRequest)
	message.TableId = proto.Int64(input.tableID)
	if input.insertIfNotExist {
		message.InsertIfNotExists = proto.Bool(true)
		message.TimeoutMs = proto.Int32(int32(input.timeout / time.Millisecond))
		message.Acks = proto.Int32(input.acks)
	}
	bucketRequest := &fmsg.PbLookupReqForBucket{
		BucketId: proto.Int32(input.bucket),
		Keys:     cloneBytesList(input.keys),
	}
	if input.partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(input.partitionID)
	}
	message.BucketsReq = []*fmsg.PbLookupReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, input.path, input.bucket, request)
	if err != nil {
		return nil, err
	}
	lookup, ok := response.Message().(*fmsg.LookupResponse)
	if !ok {
		return nil, fmt.Errorf("fgo: lookup: unexpected response %T", response.Message())
	}
	if len(lookup.GetBucketsResp()) != 1 || lookup.GetBucketsResp()[0].GetBucketId() != input.bucket {
		return nil, fmt.Errorf("%w: lookup response omitted bucket %d", ErrValidation, input.bucket)
	}
	result := lookup.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyLookup); err != nil {
		return nil, err
	}
	if len(result.GetValues()) != len(input.keys) {
		return nil, fmt.Errorf(
			"%w: lookup returned %d values for %d keys",
			ErrValidation, len(result.GetValues()), len(input.keys),
		)
	}
	values := make([][]byte, len(input.keys))
	for index, value := range result.GetValues() {
		if value != nil && value.Values != nil {
			values[index] = append([]byte(nil), value.GetValues()...)
		}
	}
	return values, nil
}

func (b clientLookupBackend) prefixLookup(
	ctx context.Context,
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	keys [][]byte,
) ([][][]byte, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyPrefixLookup, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.PrefixLookupRequest)
	message.TableId = proto.Int64(tableID)
	bucketRequest := &fmsg.PbPrefixLookupReqForBucket{BucketId: proto.Int32(bucket), Keys: cloneBytesList(keys)}
	if partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(partitionID)
	}
	message.BucketsReq = []*fmsg.PbPrefixLookupReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return nil, err
	}
	lookup, ok := response.Message().(*fmsg.PrefixLookupResponse)
	if !ok {
		return nil, fmt.Errorf("fgo: prefix lookup: unexpected response %T", response.Message())
	}
	if len(lookup.GetBucketsResp()) != 1 || lookup.GetBucketsResp()[0].GetBucketId() != bucket {
		return nil, fmt.Errorf("%w: prefix lookup response omitted bucket %d", ErrValidation, bucket)
	}
	result := lookup.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyPrefixLookup); err != nil {
		return nil, err
	}
	if len(result.GetValueLists()) != len(keys) {
		return nil, fmt.Errorf("%w: prefix lookup returned %d lists for %d keys", ErrValidation, len(result.GetValueLists()), len(keys))
	}
	values := make([][][]byte, len(keys))
	for index, list := range result.GetValueLists() {
		values[index] = cloneBytesList(list.GetValues())
	}
	return values, nil
}

func cloneBytesList(values [][]byte) [][]byte {
	cloned := make([][]byte, len(values))
	for index, value := range values {
		cloned[index] = append([]byte(nil), value...)
	}
	return cloned
}

// LookupClient performs batched point and prefix lookups for a primary-key
// table. A LookupClient is safe for concurrent calls and must be closed.
type LookupClient struct {
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

// NewLookupClient creates a point and prefix lookup client for table.
func (c *Client) NewLookupClient(ctx context.Context, table Table, options ...LookupOption) (*LookupClient, error) {
	lookup, err := newLookupClient(ctx, clientLookupBackend{client: c}, table, options...)
	if err == nil {
		lookup.observer = c.observer
	}
	return lookup, err
}

func newLookupClient(ctx context.Context, backend lookupBackend, table Table, options ...LookupOption) (*LookupClient, error) {
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
	client := &LookupClient{
		table: table, path: path, backend: backend, config: config, tableID: table.ID,
		partitionID: -1, buckets: buckets, resolver: resolverFor(backend, table),
		queue: make(chan *lookupTask, config.MaxQueuedKeys),
		jobs:  make(chan lookupBatch, config.MaxConcurrent),
		slots: make(chan struct{}, config.MaxQueuedKeys),
		done:  make(chan struct{}),
	}
	if path.Partition != "" {
		client.partitionID = physicalID
	}
	client.life, client.cancel = context.WithCancel(context.Background())
	go client.runScheduler()
	return client, nil
}

func lookupConfig(options []LookupOption) (LookupConfig, error) {
	config := LookupConfig{
		MaxBatchKeys: 1000, MaxConcurrent: 8,
		MaxQueuedKeys: 10_000, BatchDelay: time.Millisecond,
		Retry:   RetryPolicy{MaxAttempts: 1},
		Timeout: 30 * time.Second, Acks: -1,
	}
	for _, option := range options {
		if option == nil {
			return LookupConfig{}, fmt.Errorf("%w: nil lookup option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return LookupConfig{}, err
		}
	}
	if config.InsertIfNotExists && config.Retry.MaxAttempts > 1 {
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
		if !column.Nullable && !contains(schema.PrimaryKey, column.Name) &&
			!contains(schema.AutoIncrement, column.Name) {
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
	client  *LookupClient
	groups  map[lookupGroup][]*lookupTask
	timer   *time.Timer
	timerC  <-chan time.Time
	workers sync.WaitGroup
}

// Lookup returns one result for each input key in input order.
func (c *LookupClient) Lookup(ctx context.Context, keys ...PrimaryKey) []LookupResult {
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
func (c *LookupClient) PrefixLookup(ctx context.Context, prefixes ...PrimaryKey) []PrefixLookupResult {
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

func (c *LookupClient) awaitPrefixLookup(
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

func (c *LookupClient) lookupContextError(ctx context.Context) error {
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

func (c *LookupClient) enqueueLookup(
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

func (c *LookupClient) encodePoint(key PrimaryKey) ([]byte, int32, error) {
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

func (c *LookupClient) encodePrefix(prefix PrimaryKey) ([]byte, int32, error) {
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

func (c *LookupClient) bucketForValues(values map[string]any) (int32, error) {
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

func (c *LookupClient) runScheduler() {
	scheduler := &lookupScheduler{
		client: c, groups: make(map[lookupGroup][]*lookupTask),
		timer: time.NewTimer(time.Hour),
	}
	if !scheduler.timer.Stop() {
		<-scheduler.timer.C
	}
	for range c.config.MaxConcurrent {
		scheduler.workers.Add(1)
		go func() {
			defer scheduler.workers.Done()
			c.runLookupWorker()
		}()
	}
	scheduler.run()
}

func (s *lookupScheduler) run() {
	for {
		select {
		case task := <-s.client.queue:
			if err := task.ctx.Err(); err != nil {
				task.complete(rawLookupResult{err: err})
				continue
			}
			group := lookupGroup{mode: task.mode, bucket: task.bucket}
			s.groups[group] = append(s.groups[group], task)
			if len(s.groups[group]) >= s.client.config.MaxBatchKeys ||
				s.client.config.BatchDelay == 0 {
				if !s.dispatchGroup(group) {
					s.shutdown()
					return
				}
			} else {
				s.armTimer()
			}
		case <-s.timerC:
			s.timerC = nil
			for group := range s.groups {
				if !s.dispatchGroup(group) {
					s.shutdown()
					return
				}
			}
		case <-s.client.life.Done():
			s.shutdown()
			return
		}
	}
}

func (s *lookupScheduler) armTimer() {
	if s.timerC == nil {
		s.timer.Reset(s.client.config.BatchDelay)
		s.timerC = s.timer.C
	}
}

func (s *lookupScheduler) stopTimer() {
	if s.timerC != nil && !s.timer.Stop() {
		select {
		case <-s.timer.C:
		default:
		}
	}
	s.timerC = nil
}

func (s *lookupScheduler) dispatchGroup(group lookupGroup) bool {
	tasks := s.groups[group]
	for len(tasks) != 0 {
		count := s.client.config.MaxBatchKeys
		if len(tasks) < count {
			count = len(tasks)
		}
		batchTasks := append([]*lookupTask(nil), tasks[:count]...)
		select {
		case s.client.jobs <- lookupBatch{group: group, tasks: batchTasks}:
			tasks = tasks[count:]
		case <-s.client.life.Done():
			completeLookupTasks(batchTasks, rawLookupResult{err: ErrClosed})
			return false
		}
	}
	delete(s.groups, group)
	return true
}

func (s *lookupScheduler) shutdown() {
	s.stopTimer()
	for _, tasks := range s.groups {
		completeLookupTasks(tasks, rawLookupResult{err: ErrClosed})
	}
	for {
		select {
		case task := <-s.client.queue:
			task.complete(rawLookupResult{err: ErrClosed})
		default:
			close(s.client.jobs)
			s.workers.Wait()
			close(s.client.done)
			return
		}
	}
}

func (c *LookupClient) runLookupWorker() {
	for batch := range c.jobs {
		active := batch.tasks[:0]
		for _, task := range batch.tasks {
			if err := task.ctx.Err(); err != nil {
				task.complete(rawLookupResult{err: err})
			} else {
				active = append(active, task)
			}
		}
		if len(active) == 0 {
			continue
		}
		requestCtx, cancel := context.WithTimeout(c.life, c.config.Timeout)
		if batch.group.mode == pointLookupMode {
			values, err := c.runPointAttempts(requestCtx, batch.group.bucket, active)
			completePointLookupBatch(active, values, err)
		} else {
			values, err := c.runPrefixAttempts(requestCtx, batch.group.bucket, active)
			completePrefixLookupBatch(active, values, err)
		}
		cancel()
	}
}

func (c *LookupClient) runPointAttempts(
	ctx context.Context,
	bucket int32,
	tasks []*lookupTask,
) ([][]byte, error) {
	return retryLookup(ctx, c.config.Retry, c.observer, c.config.InsertIfNotExists, func() ([][]byte, error) {
		return c.backend.lookup(ctx, lookupRequest{
			path: c.path, bucket: bucket, tableID: c.tableID, partitionID: c.partitionID,
			keys: encodedLookupTasks(tasks), insertIfNotExist: c.config.InsertIfNotExists,
			timeout: c.config.Timeout, acks: c.config.Acks,
		})
	})
}

func (c *LookupClient) runPrefixAttempts(
	ctx context.Context,
	bucket int32,
	tasks []*lookupTask,
) ([][][]byte, error) {
	return retryLookup(ctx, c.config.Retry, c.observer, false, func() ([][][]byte, error) {
		return c.backend.prefixLookup(
			ctx, c.path, bucket, c.tableID, c.partitionID, encodedLookupTasks(tasks),
		)
	})
}

func retryLookup[T any](
	ctx context.Context,
	policy RetryPolicy,
	observer MetricsObserver,
	mutation bool,
	call func() (T, error),
) (T, error) {
	var zero T
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		value, err := call()
		if err == nil {
			return value, nil
		}
		if mutation || attempt == policy.MaxAttempts || !writerRetryable(err) || ctx.Err() != nil {
			return zero, err
		}
		observeMetric(observer, MetricEvent{
			Kind: MetricRetry, Operation: MetricOperationLookup, Attempt: attempt + 1,
			Failed: true, ErrorClass: metricErrorClass(err),
		})
		delay := time.Duration(0)
		if policy.Backoff != nil {
			delay = policy.Backoff(attempt + 1)
		}
		if err := waitContext(ctx, delay); err != nil {
			return zero, err
		}
	}
	return zero, fmt.Errorf("%w: unreachable lookup retry loop", ErrValidation)
}

func encodedLookupTasks(tasks []*lookupTask) [][]byte {
	encoded := make([][]byte, len(tasks))
	for index, task := range tasks {
		encoded[index] = task.encoded
	}
	return encoded
}

func completeLookupTasks(tasks []*lookupTask, result rawLookupResult) {
	for _, task := range tasks {
		task.complete(result)
	}
}

func completePointLookupBatch(tasks []*lookupTask, values [][]byte, err error) {
	if err != nil {
		completeLookupTasks(tasks, rawLookupResult{err: err})
		return
	}
	if len(values) != len(tasks) {
		completeLookupTasks(tasks, rawLookupResult{
			err: fmt.Errorf(
				"%w: lookup returned %d values for %d keys",
				ErrValidation, len(values), len(tasks),
			),
		})
		return
	}
	for index, task := range tasks {
		task.complete(rawLookupResult{value: values[index]})
	}
}

func completePrefixLookupBatch(tasks []*lookupTask, values [][][]byte, err error) {
	if err != nil {
		completeLookupTasks(tasks, rawLookupResult{err: err})
		return
	}
	if len(values) != len(tasks) {
		completeLookupTasks(tasks, rawLookupResult{
			err: fmt.Errorf(
				"%w: prefix lookup returned %d lists for %d keys",
				ErrValidation, len(values), len(tasks),
			),
		})
		return
	}
	for index, task := range tasks {
		task.complete(rawLookupResult{rows: values[index]})
	}
}

func decodeLookupValue(table Table, value []byte) (Row, error) {
	return decodeLookupValueWithResolver(
		context.Background(),
		fixedSchemaResolver{path: table.Path, schemaID: table.SchemaID, schema: table.Schema},
		table,
		value,
	)
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
func (c *LookupClient) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.cancel()
	}
	c.mu.Unlock()
	<-c.done
	return nil
}
