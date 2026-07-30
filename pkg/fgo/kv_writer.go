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

type KVWriterConfig struct {
	MaxBatchBytes   int
	MaxBatchRecords int
	MaxBuffered     int
	Linger          time.Duration
	Timeout         time.Duration
	Acks            int32
	Partition       string
}

type KVWriterOption func(*KVWriterConfig) error

func WithKVBatchLimits(bytes, records int) KVWriterOption {
	return func(config *KVWriterConfig) error {
		if bytes <= kvBatchHeaderSize || bytes > maxRowBytes || records <= 0 {
			return fmt.Errorf("%w: invalid KV batch limits", ErrInvalidConfig)
		}
		config.MaxBatchBytes, config.MaxBatchRecords = bytes, records
		return nil
	}
}

func WithKVBuffer(records int) KVWriterOption {
	return func(config *KVWriterConfig) error {
		if records <= 0 {
			return fmt.Errorf("%w: KV buffer must be positive", ErrInvalidConfig)
		}
		config.MaxBuffered = records
		return nil
	}
}

func WithKVLinger(linger time.Duration) KVWriterOption {
	return func(config *KVWriterConfig) error {
		if linger < 0 {
			return fmt.Errorf("%w: negative KV linger", ErrInvalidConfig)
		}
		config.Linger = linger
		return nil
	}
}

func WithKVRequest(timeout time.Duration, acks int32) KVWriterOption {
	return func(config *KVWriterConfig) error {
		if timeout <= 0 || timeout/time.Millisecond > math.MaxInt32 ||
			(acks != 0 && acks != 1 && acks != -1) {
			return fmt.Errorf("%w: invalid KV request settings", ErrInvalidConfig)
		}
		config.Timeout, config.Acks = timeout, acks
		return nil
	}
}

func WithKVPartition(partition string) KVWriterOption {
	return func(config *KVWriterConfig) error {
		path := PhysicalTablePath{TablePath: TablePath{Database: "d", Table: "t"}, Partition: partition}
		if err := path.Validate(); err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

type kvWriterBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error)
	initWriter(context.Context, PhysicalTablePath, int32) (int64, error)
	put(context.Context, PhysicalTablePath, int32, int64, int64, []int32, []byte, time.Duration, int32) (int64, error)
}

type clientKVWriterBackend struct{ client *Client }

func (b clientKVWriterBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]Node, error) {
	return (clientLogWriterBackend{client: b.client}).metadata(ctx, path)
}

func (b clientKVWriterBackend) initWriter(ctx context.Context, path PhysicalTablePath, bucket int32) (int64, error) {
	return (clientLogWriterBackend{client: b.client}).initWriter(ctx, path, bucket)
}

func (b clientKVWriterBackend) put(
	ctx context.Context,
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	targets []int32,
	records []byte,
	timeout time.Duration,
	acks int32,
) (int64, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyPutKv, 0)
	if err != nil {
		return 0, err
	}
	message := request.Message().(*fmsg.PutKvRequest)
	message.Acks = proto.Int32(acks)
	message.TableId = proto.Int64(tableID)
	message.TimeoutMs = proto.Int32(int32(timeout / time.Millisecond))
	message.TargetColumns = append([]int32(nil), targets...)
	message.AggMode = proto.Int32(0)
	bucketRequest := &fmsg.PbPutKvReqForBucket{BucketId: proto.Int32(bucket), Records: records}
	if partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(partitionID)
	}
	message.BucketsReq = []*fmsg.PbPutKvReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return 0, err
	}
	put, ok := response.Message().(*fmsg.PutKvResponse)
	if !ok {
		return 0, fmt.Errorf("fgo: put KV: unexpected response %T", response.Message())
	}
	if len(put.GetBucketsResp()) != 1 || put.GetBucketsResp()[0].GetBucketId() != bucket {
		return 0, fmt.Errorf("%w: put KV response omitted bucket %d", ErrValidation, bucket)
	}
	result := put.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyPutKv); err != nil {
		return 0, err
	}
	return result.GetLogEndOffset(), nil
}

type KVWriter struct {
	table       Table
	path        PhysicalTablePath
	backend     kvWriterBackend
	config      KVWriterConfig
	tableID     int64
	partitionID int64
	writerID    int64
	buckets     []int32
	commands    chan kvWriterCommand
	done        chan struct{}
	appendMu    sync.Mutex
	closed      bool
}

type kvWriterCommand struct {
	item  *pendingKVWrite
	flush chan error
	close chan error
}

type pendingKVWrite struct {
	ctx     context.Context
	record  KVRecord
	bucket  int32
	targets []int32
	size    int
	future  *WriteFuture
}

type kvPendingBatch struct {
	items   []*pendingKVWrite
	records []KVRecord
	targets []int32
	bytes   int
}

func (c *Client) NewKVWriter(ctx context.Context, table Table, options ...KVWriterOption) (*KVWriter, error) {
	return newKVWriter(ctx, clientKVWriterBackend{client: c}, table, options...)
}

func newKVWriter(ctx context.Context, backend kvWriterBackend, table Table, options ...KVWriterOption) (*KVWriter, error) {
	if err := table.RequirePrimaryKey(); err != nil {
		return nil, err
	}
	if err := table.Schema.Validate(); err != nil {
		return nil, err
	}
	if table.SchemaID < 0 || table.SchemaID > math.MaxInt16 {
		return nil, fmt.Errorf("%w: schema ID exceeds KV batch range", ErrInvalidSchema)
	}
	if len(table.Schema.PrimaryKey) == 0 {
		return nil, fmt.Errorf("%w: primary-key table has no primary key", ErrInvalidSchema)
	}
	primary := make(map[string]bool, len(table.Schema.PrimaryKey))
	for _, name := range table.Schema.PrimaryKey {
		primary[name] = true
	}
	for _, name := range table.Schema.BucketKey {
		if !primary[name] {
			return nil, fmt.Errorf("%w: bucket key %q is not part of the primary key", ErrInvalidSchema, name)
		}
	}
	config := KVWriterConfig{
		MaxBatchBytes: 1 << 20, MaxBatchRecords: 1000, MaxBuffered: 10_000,
		Linger: 5 * time.Millisecond, Timeout: 30 * time.Second, Acks: -1,
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil KV writer option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return nil, err
		}
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
	writerID, err := backend.initWriter(ctx, path, buckets[0])
	if err != nil {
		return nil, err
	}
	writer := &KVWriter{
		table: table, path: path, backend: backend, config: config, tableID: table.ID,
		partitionID: -1, writerID: writerID, buckets: buckets,
		commands: make(chan kvWriterCommand, config.MaxBuffered), done: make(chan struct{}),
	}
	if path.Partition != "" {
		writer.partitionID = physicalID
	}
	go writer.run()
	return writer, nil
}

func (w *KVWriter) Upsert(ctx context.Context, row Row) *WriteFuture {
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
func (w *KVWriter) PartialUpsert(ctx context.Context, columns []string, values Row) *WriteFuture {
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
	value, err := EncodeCompactedProjectedRow(w.table.Schema, columns, values)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	w.enqueueMutation(ctx, rowValues(w.table.Schema, values, columns), value, targets, future)
	return future
}

func (w *KVWriter) Delete(ctx context.Context, key PrimaryKey) *WriteFuture {
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

func (w *KVWriter) enqueueMutation(
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
	w.enqueue(ctx, &pendingKVWrite{
		ctx: ctx, record: KVRecord{Key: key, Value: value}, bucket: bucket,
		targets: append([]int32(nil), targets...), size: len(key) + len(value) + 8, future: future,
	})
}

func (w *KVWriter) validatePartial(columns []string, values Row) ([]int32, error) {
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
	for _, name := range w.table.Schema.PrimaryKey {
		if !selected[name] {
			return nil, fmt.Errorf("%w: partial update omits primary key %q", ErrInvalidRow, name)
		}
	}
	for _, name := range w.table.Schema.AutoIncrement {
		if selected[name] {
			return nil, fmt.Errorf("%w: partial update includes auto-increment column %q", ErrInvalidRow, name)
		}
	}
	for _, column := range w.table.Schema.Columns {
		if !selected[column.Name] && !column.Nullable && !contains(w.table.Schema.PrimaryKey, column.Name) &&
			!contains(w.table.Schema.AutoIncrement, column.Name) {
			return nil, fmt.Errorf("%w: omitted column %q is not nullable", ErrInvalidRow, column.Name)
		}
	}
	return targets, nil
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

func (w *KVWriter) enqueue(ctx context.Context, item *pendingKVWrite) {
	w.appendMu.Lock()
	defer w.appendMu.Unlock()
	if w.closed {
		item.future.complete(WriteResult{Err: ErrClosed})
		return
	}
	select {
	case w.commands <- kvWriterCommand{item: item}:
	case <-ctx.Done():
		item.future.complete(WriteResult{Err: ctx.Err()})
	case <-w.done:
		item.future.complete(WriteResult{Err: ErrClosed})
	}
}

func (w *KVWriter) Flush(ctx context.Context) error {
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
	case w.commands <- kvWriterCommand{flush: barrier}:
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

func (w *KVWriter) Close(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	w.appendMu.Lock()
	if w.closed {
		w.appendMu.Unlock()
		select {
		case <-w.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w.closed = true
	barrier := make(chan error, 1)
	select {
	case w.commands <- kvWriterCommand{close: barrier}:
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

func (w *KVWriter) run() {
	defer close(w.done)
	batches := make(map[int32]*kvPendingBatch)
	sequences := make(map[int32]int32)
	poisoned := make(map[int32]error)
	timer := time.NewTimer(w.config.Linger)
	if !timer.Stop() {
		<-timer.C
	}
	var timerChannel <-chan time.Time
	arm := func() {
		if timerChannel == nil && w.config.Linger > 0 {
			timer.Reset(w.config.Linger)
			timerChannel = timer.C
		}
	}
	stop := func() {
		if timerChannel != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerChannel = nil
	}
	flushBucket := func(bucket int32) error {
		batch := batches[bucket]
		if batch == nil || len(batch.items) == 0 {
			return nil
		}
		delete(batches, bucket)
		if err := poisoned[bucket]; err != nil {
			w.completeKVBatch(batch, bucket, 0, fmt.Errorf("%w: bucket %d: %v", ErrWriterState, bucket, err))
			return err
		}
		encoded, err := (KVBatch{
			SchemaID: int16(w.table.SchemaID), WriterID: w.writerID,
			BatchSequence: sequences[bucket], Records: batch.records,
		}).Encode()
		var logEnd int64
		if err == nil {
			logEnd, err = w.backend.put(
				context.Background(), w.path, bucket, w.tableID, w.partitionID,
				batch.targets, encoded, w.config.Timeout, w.config.Acks,
			)
		}
		if err != nil {
			poisoned[bucket] = err
			w.completeKVBatch(batch, bucket, 0, err)
			return err
		}
		sequences[bucket]++
		w.completeKVBatch(batch, bucket, logEnd, nil)
		return nil
	}
	flushAll := func() error {
		stop()
		var joined error
		for _, bucket := range w.buckets {
			joined = errors.Join(joined, flushBucket(bucket))
		}
		return joined
	}
	for {
		select {
		case command := <-w.commands:
			switch {
			case command.item != nil:
				item := command.item
				if err := item.ctx.Err(); err != nil {
					item.future.complete(WriteResult{Err: err})
					continue
				}
				batch := batches[item.bucket]
				if batch != nil && targetKey(batch.targets) != targetKey(item.targets) {
					_ = flushBucket(item.bucket)
					batch = nil
				}
				if batch == nil {
					batch = &kvPendingBatch{targets: append([]int32(nil), item.targets...)}
					batches[item.bucket] = batch
				}
				if len(batch.items) > 0 && (len(batch.items) >= w.config.MaxBatchRecords ||
					batch.bytes+item.size > w.config.MaxBatchBytes) {
					_ = flushBucket(item.bucket)
					batch = &kvPendingBatch{targets: append([]int32(nil), item.targets...)}
					batches[item.bucket] = batch
				}
				batch.items = append(batch.items, item)
				batch.records = append(batch.records, item.record)
				batch.bytes += item.size
				if len(batch.items) >= w.config.MaxBatchRecords || batch.bytes >= w.config.MaxBatchBytes ||
					w.config.Linger == 0 {
					_ = flushBucket(item.bucket)
				} else {
					arm()
				}
			case command.flush != nil:
				command.flush <- flushAll()
			case command.close != nil:
				command.close <- flushAll()
				timer.Stop()
				return
			}
		case <-timerChannel:
			timerChannel = nil
			_ = flushAll()
		}
	}
}

func (w *KVWriter) completeKVBatch(batch *kvPendingBatch, bucket int32, logEnd int64, err error) {
	first := logEnd - int64(len(batch.items))
	for index, item := range batch.items {
		item.future.complete(WriteResult{
			Bucket: bucket, BaseOffset: first + int64(index), Records: 1, Err: err,
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
