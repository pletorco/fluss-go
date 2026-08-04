package fgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
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

type upsertWriterBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]ServerNode, error)
	initWriter(context.Context, PhysicalTablePath, int32) (int64, error)
	put(context.Context, kvPutRequest) (int64, error)
}

type clientUpsertWriterBackend struct{ client *Client }

func (b clientUpsertWriterBackend) ensurePartition(
	ctx context.Context,
	path PhysicalTablePath,
	partitionKeys []string,
) error {
	return b.client.ensureDynamicPartition(ctx, path, partitionKeys)
}

type kvPutRequest struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	targets     []int32
	records     []byte
	timeout     time.Duration
	acks        int32
	mergeMode   MergeMode
}

func (b clientUpsertWriterBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]ServerNode, error) {
	return (clientAppendWriterBackend{client: b.client}).metadata(ctx, path)
}

func (b clientUpsertWriterBackend) initWriter(ctx context.Context, path PhysicalTablePath, bucket int32) (int64, error) {
	return (clientAppendWriterBackend{client: b.client}).initWriter(ctx, path, bucket)
}

func (b clientUpsertWriterBackend) put(
	ctx context.Context,
	input kvPutRequest,
) (int64, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyPutKv, 0)
	if err != nil {
		return 0, err
	}
	message := request.Message().(*fmsg.PutKvRequest)
	message.Acks = proto.Int32(input.acks)
	message.TableId = proto.Int64(input.tableID)
	message.TimeoutMs = proto.Int32(int32(input.timeout / time.Millisecond))
	message.TargetColumns = append([]int32(nil), input.targets...)
	message.AggMode = proto.Int32(int32(input.mergeMode))
	bucketRequest := &fmsg.PbPutKvReqForBucket{BucketId: proto.Int32(input.bucket), Records: input.records}
	if input.partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(input.partitionID)
	}
	message.BucketsReq = []*fmsg.PbPutKvReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, input.path, input.bucket, request)
	if err != nil {
		return 0, err
	}
	put, ok := response.Message().(*fmsg.PutKvResponse)
	if !ok {
		return 0, fmt.Errorf("fgo: put KV: unexpected response %T", response.Message())
	}
	if len(put.GetBucketsResp()) != 1 || put.GetBucketsResp()[0].GetBucketId() != input.bucket {
		return 0, fmt.Errorf("%w: put KV response omitted bucket %d", ErrValidation, input.bucket)
	}
	result := put.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyPutKv); err != nil {
		return 0, err
	}
	return result.GetLogEndOffset(), nil
}

// UpsertWriter batches primary-key upserts and deletes.
// It owns one background scheduler and must be closed after use.
type UpsertWriter struct {
	table       Table
	path        PhysicalTablePath
	backend     upsertWriterBackend
	config      UpsertWriterConfig
	tableID     int64
	partitionID int64
	writerID    int64
	buckets     []int32
	commands    chan upsertWriterCommand
	slots       chan struct{}
	done        chan struct{}
	appendMu    sync.Mutex
	closed      bool
	closeErr    error
	observer    MetricsObserver
}

type upsertWriterCommand struct {
	item  *pendingKVWrite
	flush chan error
	close chan error
}

type pendingKVWrite struct {
	ctx      context.Context
	record   KVRecord
	bucket   int32
	targets  []int32
	size     int
	queuedAt time.Time
	future   *WriteFuture
}

type kvPendingBatch struct {
	items   []*pendingKVWrite
	records []KVRecord
	targets []int32
	bytes   int
}

type upsertWriterLoop struct {
	writer      *UpsertWriter
	batches     map[int32]*kvPendingBatch
	pending     map[int32][]*kvPendingBatch
	active      map[int32]bool
	sequences   map[int32]int32
	poisoned    map[int32]error
	completions chan kvBatchCompletion
	inFlight    int
	timer       *time.Timer
	timerC      <-chan time.Time
}

type kvBatchCompletion struct {
	bucket      int32
	batch       *kvPendingBatch
	logEnd      int64
	offsetKnown bool
	bytes       int
	started     time.Time
	err         error
}

// NewUpsertWriter creates a primary-key writer for table.
func (c *Client) NewUpsertWriter(ctx context.Context, table Table, options ...UpsertWriterOption) (*UpsertWriter, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	writer, err := newUpsertWriter(ctx, clientUpsertWriterBackend{client: c}, table, options...)
	if err == nil {
		writer.observer = c.observer
	}
	return writer, err
}

func newUpsertWriter(ctx context.Context, backend upsertWriterBackend, table Table, options ...UpsertWriterOption) (*UpsertWriter, error) {
	if err := validateUpsertWriterTable(table); err != nil {
		return nil, err
	}
	config, err := upsertWriterConfig(options)
	if err != nil {
		return nil, err
	}
	if config.MergeMode == MergeModeOverwrite && table.Properties != nil &&
		strings.TrimSpace(table.Properties["table.merge-engine"]) == "" {
		return nil, fmt.Errorf(
			"%w: KV overwrite requires table %s to configure table.merge-engine",
			ErrInvalidConfig, table.Path,
		)
	}
	path := PhysicalTablePath{TablePath: table.Path, Partition: config.Partition}
	if backend, ok := backend.(interface {
		ensurePartition(context.Context, PhysicalTablePath, []string) error
	}); ok {
		if err := backend.ensurePartition(ctx, path, table.Schema.PartitionKey); err != nil {
			return nil, err
		}
	}
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
	writerID, err := backend.initWriter(ctx, path, buckets[0])
	if err != nil {
		return nil, err
	}
	writer := &UpsertWriter{
		table: table, path: path, backend: backend, config: config, tableID: table.ID,
		partitionID: -1, writerID: writerID, buckets: buckets,
		commands: make(chan upsertWriterCommand, config.MaxBuffered),
		slots:    make(chan struct{}, config.MaxBuffered), done: make(chan struct{}),
	}
	if path.Partition != "" {
		writer.partitionID = physicalID
	}
	go writer.run()
	return writer, nil
}

func validateUpsertWriterTable(table Table) error {
	if err := table.RequirePrimaryKey(); err != nil {
		return err
	}
	if err := table.Schema.Validate(); err != nil {
		return err
	}
	if table.SchemaID < 0 || table.SchemaID > math.MaxInt16 {
		return fmt.Errorf("%w: schema ID exceeds KV batch range", ErrInvalidSchema)
	}
	if len(table.Schema.PrimaryKey) == 0 {
		return fmt.Errorf("%w: primary-key table has no primary key", ErrInvalidSchema)
	}
	primary := make(map[string]bool, len(table.Schema.PrimaryKey))
	for _, name := range table.Schema.PrimaryKey {
		primary[name] = true
	}
	for _, name := range table.Schema.BucketKey {
		if !primary[name] {
			return fmt.Errorf("%w: bucket key %q is not part of the primary key", ErrInvalidSchema, name)
		}
	}
	return nil
}

func upsertWriterConfig(options []UpsertWriterOption) (UpsertWriterConfig, error) {
	config := UpsertWriterConfig{
		MaxBatchBytes: 1 << 20, MaxBatchRecords: 1000, MaxBuffered: 10_000,
		MaxConcurrentRequests: 4,
		BatchTimeout:          5 * time.Millisecond, RequestTimeout: 30 * time.Second, Acks: -1,
		RetryPolicy: defaultWriterRetryPolicy(),
	}
	for _, option := range options {
		if option == nil {
			return UpsertWriterConfig{}, fmt.Errorf("%w: nil upsert writer option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return UpsertWriterConfig{}, err
		}
	}
	if err := validateWriterRetryPolicy(config.RetryPolicy, config.Acks); err != nil {
		return UpsertWriterConfig{}, err
	}
	return config, nil
}

// Upsert queues a complete primary-key row for insertion or update.
func (w *UpsertWriter) Upsert(ctx context.Context, row Row) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: nil context", ErrInvalidConfig)})
		return future
	}
	if len(w.table.Schema.AutoIncrement) != 0 {
		future.complete(WriteResult{Err: fmt.Errorf("%w: full upsert includes auto-increment columns", ErrInvalidRow)})
		return future
	}
	if err := w.table.Schema.ValidateRow(row, nil); err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	value, err := EncodeCompactedRow(w.table.Schema, row)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	w.enqueueMutation(ctx, rowValues(w.table.Schema, row, nil), value, nil, future)
	return future
}

// PartialUpsert writes only columns named by columns. The columns must contain the complete
// primary key and must be in the same order as values.
func (w *UpsertWriter) PartialUpsert(ctx context.Context, columns []string, values Row) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: nil context", ErrInvalidConfig)})
		return future
	}
	targets, err := w.validatePartial(columns, values)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	full := make(Row, len(w.table.Schema.Columns))
	positions := make(map[string]int, len(w.table.Schema.Columns))
	for index, column := range w.table.Schema.Columns {
		positions[column.Name] = index
	}
	for index, column := range columns {
		full[positions[column]] = values[index]
	}
	value, err := EncodeCompactedRow(w.table.Schema, full)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	w.enqueueMutation(ctx, rowValues(w.table.Schema, values, columns), value, targets, future)
	return future
}

// Delete queues removal of the row identified by key.
func (w *UpsertWriter) Delete(ctx context.Context, key PrimaryKey) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: nil context", ErrInvalidConfig)})
		return future
	}
	if err := w.table.Schema.ValidatePrimaryKey(key); err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	values := make(map[string]any, len(key))
	for index, name := range w.table.Schema.PrimaryKey {
		values[name] = key[index]
	}
	w.enqueueMutation(ctx, values, nil, nil, future)
	return future
}

func (w *UpsertWriter) enqueueMutation(
	ctx context.Context,
	values map[string]any,
	value []byte,
	targets []int32,
	future *WriteFuture,
) {
	keyValues := make(PrimaryKey, len(w.table.Schema.PrimaryKey))
	for index, name := range w.table.Schema.PrimaryKey {
		keyValues[index] = values[name]
	}
	key, err := EncodePrimaryKey(w.table.Schema, keyValues)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return
	}
	bucketNames := w.table.Schema.BucketKey
	if len(bucketNames) == 0 {
		bucketNames = w.table.Schema.PrimaryKey
	}
	bucketValues := make(PrimaryKey, len(bucketNames))
	for index, name := range bucketNames {
		bucketValues[index] = values[name]
	}
	bucketKey, err := encodeKeyColumns(w.table.Schema, bucketNames, bucketValues)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return
	}
	hashBucket, err := flussBucket(bucketKey, len(w.buckets))
	if err != nil {
		future.complete(WriteResult{Err: err})
		return
	}
	bucket := w.buckets[int(hashBucket)]
	item := &pendingKVWrite{
		ctx: ctx, record: KVRecord{Key: key, Value: value}, bucket: bucket,
		targets: append([]int32(nil), targets...), size: len(key) + len(value) + 8, future: future,
	}
	if w.observer != nil {
		item.queuedAt = time.Now()
	}
	w.enqueue(ctx, item)
}

func (w *UpsertWriter) validatePartial(columns []string, values Row) ([]int32, error) {
	if len(columns) == 0 || len(columns) != len(values) {
		return nil, fmt.Errorf("%w: partial update columns and values must be non-empty and aligned", ErrInvalidRow)
	}
	if err := w.table.Schema.ValidateRow(values, columns); err != nil {
		return nil, err
	}
	positions := make(map[string]int32, len(w.table.Schema.Columns))
	for index, column := range w.table.Schema.Columns {
		positions[column.Name] = int32(index)
	}
	selected := make(map[string]bool, len(columns))
	targets := make([]int32, len(columns))
	for index, name := range columns {
		if selected[name] {
			return nil, fmt.Errorf("%w: duplicate target column %q", ErrInvalidRow, name)
		}
		selected[name] = true
		targets[index] = positions[name]
	}
	if err := w.validatePartialSelection(selected); err != nil {
		return nil, err
	}
	return targets, nil
}

func (w *UpsertWriter) validatePartialSelection(selected map[string]bool) error {
	for _, name := range w.table.Schema.PrimaryKey {
		if !selected[name] {
			return fmt.Errorf("%w: partial update omits primary key %q", ErrInvalidRow, name)
		}
	}
	for _, name := range w.table.Schema.AutoIncrement {
		if selected[name] {
			return fmt.Errorf("%w: partial update includes auto-increment column %q", ErrInvalidRow, name)
		}
	}
	for _, column := range w.table.Schema.Columns {
		if !selected[column.Name] && !column.Nullable && !contains(w.table.Schema.PrimaryKey, column.Name) &&
			!contains(w.table.Schema.AutoIncrement, column.Name) {
			return fmt.Errorf("%w: omitted column %q is not nullable", ErrInvalidRow, column.Name)
		}
	}
	return nil
}

func rowValues(schema Schema, row Row, columns []string) map[string]any {
	if len(columns) == 0 {
		columns = schema.selectedColumns(nil)
	}
	values := make(map[string]any, len(columns))
	for index, name := range columns {
		values[name] = row[index]
	}
	return values
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (w *UpsertWriter) enqueue(ctx context.Context, item *pendingKVWrite) {
	select {
	case w.slots <- struct{}{}:
		item.future.release = func() { <-w.slots }
	case <-ctx.Done():
		item.future.complete(WriteResult{Err: ctx.Err()})
		return
	case <-w.done:
		item.future.complete(WriteResult{Err: ErrClosed})
		return
	}
	w.appendMu.Lock()
	defer w.appendMu.Unlock()
	if w.closed {
		item.future.complete(WriteResult{Err: ErrClosed})
		return
	}
	select {
	case w.commands <- upsertWriterCommand{item: item}:
	case <-ctx.Done():
		item.future.complete(WriteResult{Err: ctx.Err()})
	case <-w.done:
		item.future.complete(WriteResult{Err: ErrClosed})
	}
}

// Flush waits until every previously accepted mutation reaches a terminal
// result.
func (w *UpsertWriter) Flush(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	barrier := make(chan error, 1)
	w.appendMu.Lock()
	if w.closed {
		w.appendMu.Unlock()
		return ErrClosed
	}
	select {
	case w.commands <- upsertWriterCommand{flush: barrier}:
		w.appendMu.Unlock()
	case <-ctx.Done():
		w.appendMu.Unlock()
		return ctx.Err()
	}
	select {
	case err := <-barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes accepted mutations and stops the writer.
// Close is idempotent.
func (w *UpsertWriter) Close(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	w.appendMu.Lock()
	if w.closed {
		w.appendMu.Unlock()
		select {
		case <-w.done:
			w.appendMu.Lock()
			err := w.closeErr
			w.appendMu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.closed = true
	barrier := make(chan error, 1)
	select {
	case w.commands <- upsertWriterCommand{close: barrier}:
		w.appendMu.Unlock()
	case <-ctx.Done():
		w.closed = false
		w.appendMu.Unlock()
		return ctx.Err()
	}
	select {
	case err := <-barrier:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *UpsertWriter) run() {
	defer close(w.done)
	timer := time.NewTimer(w.config.BatchTimeout)
	if !timer.Stop() {
		<-timer.C
	}
	loop := &upsertWriterLoop{
		writer: w, batches: make(map[int32]*kvPendingBatch), pending: make(map[int32][]*kvPendingBatch),
		active: make(map[int32]bool), sequences: make(map[int32]int32),
		poisoned:    make(map[int32]error),
		completions: make(chan kvBatchCompletion, w.config.MaxConcurrentRequests),
		timer:       timer,
	}
	loop.run()
}

func (l *upsertWriterLoop) run() {
	for {
		select {
		case command := <-l.writer.commands:
			switch {
			case command.item != nil:
				l.add(command.item)
			case command.flush != nil:
				command.flush <- l.flushAll()
			case command.close != nil:
				err := l.flushAll()
				l.writer.appendMu.Lock()
				l.writer.closeErr = err
				l.writer.appendMu.Unlock()
				command.close <- err
				l.timer.Stop()
				return
			}
		case <-l.timerC:
			l.timerC = nil
			_ = l.queueAll()
		case completion := <-l.completions:
			_ = l.handleCompletion(completion)
		}
	}
}

func (l *upsertWriterLoop) add(item *pendingKVWrite) {
	if err := item.ctx.Err(); err != nil {
		item.future.complete(WriteResult{Err: err})
		return
	}
	batch := l.compatibleBatch(item)
	if len(batch.items) > 0 && l.batchFullWith(batch, item) {
		_ = l.flushBucket(item.bucket)
		batch = l.newBatch(item)
	}
	batch.items = append(batch.items, item)
	batch.records = append(batch.records, item.record)
	batch.bytes += item.size
	if l.batchFull(batch) || l.writer.config.BatchTimeout == 0 {
		_ = l.flushBucket(item.bucket)
		return
	}
	l.armTimer()
}

func (l *upsertWriterLoop) compatibleBatch(item *pendingKVWrite) *kvPendingBatch {
	batch := l.batches[item.bucket]
	if batch != nil && targetKey(batch.targets) != targetKey(item.targets) {
		_ = l.flushBucket(item.bucket)
		batch = nil
	}
	if batch == nil {
		batch = l.newBatch(item)
	}
	return batch
}

func (l *upsertWriterLoop) newBatch(item *pendingKVWrite) *kvPendingBatch {
	batch := &kvPendingBatch{targets: append([]int32(nil), item.targets...)}
	l.batches[item.bucket] = batch
	return batch
}

func (l *upsertWriterLoop) batchFull(batch *kvPendingBatch) bool {
	return len(batch.items) >= l.writer.config.MaxBatchRecords || batch.bytes >= l.writer.config.MaxBatchBytes
}

func (l *upsertWriterLoop) batchFullWith(batch *kvPendingBatch, item *pendingKVWrite) bool {
	return len(batch.items) >= l.writer.config.MaxBatchRecords ||
		batch.bytes+item.size > l.writer.config.MaxBatchBytes
}

func (l *upsertWriterLoop) flushBucket(bucket int32) error {
	batch := l.batches[bucket]
	if batch == nil || len(batch.items) == 0 {
		return nil
	}
	delete(l.batches, bucket)
	if err := l.poisoned[bucket]; err != nil {
		poisoned := fmt.Errorf("%w: bucket %d: %v", ErrWriterState, bucket, err)
		l.writer.completeKVBatch(batch, bucket, 0, false, poisoned)
		return poisoned
	}
	l.pending[bucket] = append(l.pending[bucket], batch)
	l.dispatch()
	return nil
}

func (l *upsertWriterLoop) dispatch() {
	for l.inFlight < l.writer.config.MaxConcurrentRequests {
		dispatched := false
		for _, bucket := range l.writer.buckets {
			if l.active[bucket] || len(l.pending[bucket]) == 0 {
				continue
			}
			batch := l.pending[bucket][0]
			l.pending[bucket] = l.pending[bucket][1:]
			l.active[bucket] = true
			l.inFlight++
			sequence := l.sequences[bucket]
			go l.executeBatch(bucket, batch, sequence)
			dispatched = true
			if l.inFlight == l.writer.config.MaxConcurrentRequests {
				break
			}
		}
		if !dispatched {
			return
		}
	}
}

func (l *upsertWriterLoop) executeBatch(bucket int32, batch *kvPendingBatch, sequence int32) {
	encoded, err := (KVBatch{
		SchemaID: int16(l.writer.table.SchemaID), WriterID: l.writer.writerID,
		BatchSequence: sequence, Records: batch.records,
	}).Encode()
	var result writerAttemptResult
	started := metricStart(l.writer.observer)
	if err == nil {
		requestCtx, cancel := context.WithTimeout(context.Background(), l.writer.config.RequestTimeout)
		result = executeWriterAttempts(
			requestCtx, l.writer.config.RetryPolicy, l.writer.observer, MetricOperationKVWrite,
			func(ctx context.Context) (int64, bool, error) {
				offset, err := l.writer.backend.put(ctx, kvPutRequest{
					path: l.writer.path, bucket: bucket, tableID: l.writer.tableID, partitionID: l.writer.partitionID,
					targets: batch.targets, records: encoded, timeout: l.writer.config.RequestTimeout, acks: l.writer.config.Acks,
					mergeMode: l.writer.config.MergeMode,
				})
				return offset, err == nil, err
			},
		)
		if result.err != nil && requestCtx.Err() != nil {
			result.err = requestCtx.Err()
		}
		cancel()
	} else {
		result.err = err
	}
	l.completions <- kvBatchCompletion{
		bucket: bucket, batch: batch, logEnd: result.offset, offsetKnown: result.offsetKnown,
		bytes: len(encoded), started: started, err: result.err,
	}
}

func (l *upsertWriterLoop) handleCompletion(completion kvBatchCompletion) error {
	delete(l.active, completion.bucket)
	l.inFlight--
	queueTime := time.Duration(0)
	if len(completion.batch.items) != 0 && !completion.batch.items[0].queuedAt.IsZero() {
		queueTime = time.Since(completion.batch.items[0].queuedAt)
	}
	observeMetric(l.writer.observer, MetricEvent{
		Kind: MetricWriteBatch, Operation: MetricOperationKVWrite,
		Duration: metricDuration(completion.started), QueueTime: queueTime,
		QueueSize: len(l.writer.commands), Records: int64(len(completion.batch.records)), Bytes: int64(completion.bytes),
		Failed: completion.err != nil, ErrorClass: metricErrorClass(completion.err),
	})
	if completion.err != nil {
		l.poisoned[completion.bucket] = completion.err
		l.writer.completeKVBatch(completion.batch, completion.bucket, 0, false, completion.err)
		for _, batch := range l.pending[completion.bucket] {
			poisoned := fmt.Errorf(
				"%w: bucket %d: %v", ErrWriterState, completion.bucket, completion.err,
			)
			l.writer.completeKVBatch(batch, completion.bucket, 0, false, poisoned)
		}
		delete(l.pending, completion.bucket)
	} else {
		l.sequences[completion.bucket]++
		l.writer.completeKVBatch(
			completion.batch, completion.bucket, completion.logEnd, completion.offsetKnown, nil,
		)
	}
	l.dispatch()
	return completion.err
}

func (l *upsertWriterLoop) flushAll() error {
	l.stopTimer()
	err := l.queueAll()
	for l.inFlight > 0 {
		err = errors.Join(err, l.handleCompletion(<-l.completions))
	}
	return err
}

func (l *upsertWriterLoop) queueAll() error {
	l.stopTimer()
	var joined error
	for _, bucket := range l.writer.buckets {
		joined = errors.Join(joined, l.flushBucket(bucket))
	}
	return joined
}

func (l *upsertWriterLoop) armTimer() {
	if l.timerC == nil && l.writer.config.BatchTimeout > 0 {
		l.timer.Reset(l.writer.config.BatchTimeout)
		l.timerC = l.timer.C
	}
}

func (l *upsertWriterLoop) stopTimer() {
	if l.timerC != nil && !l.timer.Stop() {
		select {
		case <-l.timer.C:
		default:
		}
	}
	l.timerC = nil
}

func (w *UpsertWriter) completeKVBatch(
	batch *kvPendingBatch,
	bucket int32,
	logEnd int64,
	offsetKnown bool,
	err error,
) {
	first := int64(0)
	if offsetKnown {
		first = logEnd - int64(len(batch.items))
	}
	for index, item := range batch.items {
		item.future.complete(WriteResult{
			Bucket: bucket, BaseOffset: first + int64(index), OffsetKnown: offsetKnown,
			Records: 1, Err: err,
		})
	}
}

func targetKey(targets []int32) string {
	var builder strings.Builder
	for _, target := range targets {
		builder.WriteString(strconv.FormatInt(int64(target), 10))
		builder.WriteByte(',')
	}
	return builder.String()
}
