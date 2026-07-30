package fgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

var ErrWriterState = errors.New("fgo: writer state is uncertain")

type BucketAssignment string

const (
	AssignmentAuto       BucketAssignment = "auto"
	AssignmentSticky     BucketAssignment = "sticky"
	AssignmentRoundRobin BucketAssignment = "round-robin"
)

type LogWriterConfig struct {
	MaxBatchBytes   int
	MaxBatchRecords int
	MaxBuffered     int
	Linger          time.Duration
	Timeout         time.Duration
	Acks            int32
	Assignment      BucketAssignment
	Partition       string
}

type LogWriterOption func(*LogWriterConfig) error

func WithLogBatchLimits(bytes, records int) LogWriterOption {
	return func(config *LogWriterConfig) error {
		if bytes <= logBatchV0HeaderSize || bytes > maxRowBytes || records <= 0 {
			return fmt.Errorf("%w: invalid log batch limits", ErrInvalidConfig)
		}
		config.MaxBatchBytes, config.MaxBatchRecords = bytes, records
		return nil
	}
}

func WithLogBuffer(records int) LogWriterOption {
	return func(config *LogWriterConfig) error {
		if records <= 0 {
			return fmt.Errorf("%w: log buffer must be positive", ErrInvalidConfig)
		}
		config.MaxBuffered = records
		return nil
	}
}

func WithLogLinger(linger time.Duration) LogWriterOption {
	return func(config *LogWriterConfig) error {
		if linger < 0 {
			return fmt.Errorf("%w: negative log linger", ErrInvalidConfig)
		}
		config.Linger = linger
		return nil
	}
}

func WithLogRequest(timeout time.Duration, acks int32) LogWriterOption {
	return func(config *LogWriterConfig) error {
		if timeout <= 0 || (acks != 0 && acks != 1 && acks != -1) {
			return fmt.Errorf("%w: invalid log request settings", ErrInvalidConfig)
		}
		config.Timeout, config.Acks = timeout, acks
		return nil
	}
}

func WithLogBucketAssignment(assignment BucketAssignment) LogWriterOption {
	return func(config *LogWriterConfig) error {
		switch assignment {
		case AssignmentAuto, AssignmentSticky, AssignmentRoundRobin:
			config.Assignment = assignment
			return nil
		default:
			return fmt.Errorf("%w: unknown bucket assignment %q", ErrInvalidConfig, assignment)
		}
	}
}

func WithLogPartition(partition string) LogWriterOption {
	return func(config *LogWriterConfig) error {
		path := PhysicalTablePath{TablePath: TablePath{Database: "d", Table: "t"}, Partition: partition}
		if err := path.Validate(); err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

type WriteResult struct {
	Bucket     int32
	BaseOffset int64
	Records    int
	Err        error
}

type WriteFuture struct {
	done chan struct{}
	once sync.Once
	data WriteResult
}

func newWriteFuture() *WriteFuture { return &WriteFuture{done: make(chan struct{})} }

func (f *WriteFuture) complete(result WriteResult) {
	f.once.Do(func() {
		f.data = result
		close(f.done)
	})
}

func (f *WriteFuture) Await(ctx context.Context) WriteResult {
	if f == nil {
		return WriteResult{Err: fmt.Errorf("%w: nil write future", ErrInvalidConfig)}
	}
	select {
	case <-f.done:
		return f.data
	case <-ctx.Done():
		return WriteResult{Err: ctx.Err()}
	}
}

type logWriterBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error)
	initWriter(context.Context, PhysicalTablePath, int32) (int64, error)
	produce(context.Context, PhysicalTablePath, int32, int64, int64, []byte, time.Duration, int32) (int64, error)
}

type clientLogWriterBackend struct{ client *Client }

func (b clientLogWriterBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]Node, error) {
	if path.Partition == "" {
		table, err := b.client.fetchTableMetadata(ctx, path.TablePath)
		return table.ID, table.Buckets, err
	}
	partition, err := b.client.fetchPartitionMetadata(ctx, path)
	return partition.ID, partition.Buckets, err
}

func (b clientLogWriterBackend) initWriter(ctx context.Context, path PhysicalTablePath, bucket int32) (int64, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyInitWriter, 0)
	if err != nil {
		return 0, err
	}
	request.Message().(*fmsg.InitWriterRequest).TablePath = []*fmsg.PbTablePath{pbTablePath(path.TablePath)}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return 0, err
	}
	message, ok := response.Message().(*fmsg.InitWriterResponse)
	if !ok {
		return 0, fmt.Errorf("fgo: init writer: unexpected response %T", response.Message())
	}
	return message.GetWriterId(), nil
}

func (b clientLogWriterBackend) produce(
	ctx context.Context,
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	records []byte,
	timeout time.Duration,
	acks int32,
) (int64, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyProduceLog, 0)
	if err != nil {
		return 0, err
	}
	if timeout/time.Millisecond > math.MaxInt32 {
		return 0, fmt.Errorf("%w: log timeout exceeds protocol range", ErrInvalidConfig)
	}
	message := request.Message().(*fmsg.ProduceLogRequest)
	message.Acks = proto.Int32(acks)
	message.TableId = proto.Int64(tableID)
	message.TimeoutMs = proto.Int32(int32(timeout / time.Millisecond))
	bucketRequest := &fmsg.PbProduceLogReqForBucket{BucketId: proto.Int32(bucket), Records: records}
	if path.Partition != "" {
		bucketRequest.PartitionId = proto.Int64(partitionID)
	}
	message.BucketsReq = []*fmsg.PbProduceLogReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return 0, err
	}
	produced, ok := response.Message().(*fmsg.ProduceLogResponse)
	if !ok {
		return 0, fmt.Errorf("fgo: produce log: unexpected response %T", response.Message())
	}
	if len(produced.GetBucketsResp()) != 1 || produced.GetBucketsResp()[0].GetBucketId() != bucket {
		return 0, fmt.Errorf("%w: produce response omitted bucket %d", ErrValidation, bucket)
	}
	result := produced.GetBucketsResp()[0]
	if code := fmsg.ErrorCode(result.GetErrorCode()); code != fmsg.ErrorCodeNone {
		metadata, known := fmsg.LookupErrorCode(int32(code))
		name := "UNKNOWN_FUTURE_ERROR"
		if known {
			name = metadata.Name
		}
		return 0, &ServerError{
			Code: code, Name: name, Message: result.GetErrorMessage(), API: fmsg.APIKeyProduceLog,
			Retriable: known && retriableErrorCode(code), category: errorCategory(code),
		}
	}
	return result.GetBaseOffset(), nil
}

type LogWriter struct {
	table       Table
	path        PhysicalTablePath
	backend     logWriterBackend
	config      LogWriterConfig
	tableID     int64
	partitionID int64
	writerID    int64
	buckets     []int32
	commands    chan writerCommand
	done        chan struct{}
	appendMu    sync.Mutex
	closed      bool
	roundRobin  int
	stickyIndex int
}

type writerCommand struct {
	item  *pendingWrite
	flush chan error
	close chan error
}

type pendingWrite struct {
	ctx     context.Context
	row     Row
	arrow   arrow.RecordBatch
	changes []ChangeType
	bucket  *int32
	size    int
	future  *WriteFuture
}

type bucketBatch struct {
	items   []*pendingWrite
	records []Record
	bytes   int
}

func (c *Client) NewLogWriter(ctx context.Context, table Table, options ...LogWriterOption) (*LogWriter, error) {
	return newLogWriter(ctx, clientLogWriterBackend{client: c}, table, options...)
}

func newLogWriter(ctx context.Context, backend logWriterBackend, table Table, options ...LogWriterOption) (*LogWriter, error) {
	if err := table.RequireLog(); err != nil {
		return nil, err
	}
	if err := table.Schema.Validate(); err != nil {
		return nil, err
	}
	if table.SchemaID < 0 || table.SchemaID > math.MaxInt16 {
		return nil, fmt.Errorf("%w: schema ID exceeds log batch range", ErrInvalidSchema)
	}
	config := LogWriterConfig{
		MaxBatchBytes: 1 << 20, MaxBatchRecords: 1000, MaxBuffered: 10_000,
		Linger: 5 * time.Millisecond, Timeout: 30 * time.Second, Acks: -1, Assignment: AssignmentAuto,
	}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil log writer option", ErrInvalidConfig)
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
	if table.BucketCount != 0 && table.BucketCount != len(buckets) {
		return nil, fmt.Errorf("%w: metadata has %d of %d buckets", ErrMetadata, len(buckets), table.BucketCount)
	}
	writerID, err := backend.initWriter(ctx, path, buckets[0])
	if err != nil {
		return nil, err
	}
	writer := &LogWriter{
		table: table, path: path, backend: backend, config: config, tableID: table.ID, partitionID: -1,
		writerID: writerID, buckets: buckets, commands: make(chan writerCommand, config.MaxBuffered),
		done: make(chan struct{}), stickyIndex: -1,
	}
	if path.Partition != "" {
		writer.partitionID = physicalID
	}
	go writer.run()
	return writer, nil
}

// Append queues one row. The returned future completes exactly once after acknowledgment or
// failure. The writer copies row bytes while encoding, so the caller may reuse the row afterward.
func (w *LogWriter) Append(ctx context.Context, row Row) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: nil context", ErrInvalidConfig)})
		return future
	}
	if err := w.table.Schema.ValidateRow(row, nil); err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	encoded, err := EncodeCompactedRow(w.table.Schema, row)
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	w.enqueue(ctx, &pendingWrite{ctx: ctx, row: append(Row(nil), row...), size: len(encoded) + 5, future: future})
	return future
}

// AppendArrow queues one Arrow record batch for an explicit bucket. The caller must retain the
// record batch until the returned future completes.
func (w *LogWriter) AppendArrow(ctx context.Context, bucket int32, batch arrow.RecordBatch, changes []ChangeType) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil || batch == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: context and Arrow batch are required", ErrInvalidConfig)})
		return future
	}
	if !w.hasBucket(bucket) || int64(len(changes)) != batch.NumRows() {
		future.complete(WriteResult{Err: fmt.Errorf("%w: invalid Arrow bucket or change count", ErrInvalidRow)})
		return future
	}
	for _, change := range changes {
		if err := change.Validate(); err != nil {
			future.complete(WriteResult{Err: err})
			return future
		}
	}
	w.enqueue(ctx, &pendingWrite{
		ctx: ctx, arrow: batch, changes: append([]ChangeType(nil), changes...),
		bucket: &bucket, size: int(batch.NumRows()), future: future,
	})
	return future
}

func (w *LogWriter) enqueue(ctx context.Context, item *pendingWrite) {
	w.appendMu.Lock()
	defer w.appendMu.Unlock()
	if w.closed {
		item.future.complete(WriteResult{Err: ErrClosed})
		return
	}
	select {
	case w.commands <- writerCommand{item: item}:
	case <-ctx.Done():
		item.future.complete(WriteResult{Err: ctx.Err()})
	case <-w.done:
		item.future.complete(WriteResult{Err: ErrClosed})
	}
}

func (w *LogWriter) Flush(ctx context.Context) error {
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
	case w.commands <- writerCommand{flush: barrier}:
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

func (w *LogWriter) Close(ctx context.Context) error {
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
	case w.commands <- writerCommand{close: barrier}:
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

func (w *LogWriter) run() {
	defer close(w.done)
	batches := make(map[int32]*bucketBatch)
	sequences := make(map[int32]int32)
	poisoned := make(map[int32]error)
	timer := time.NewTimer(w.config.Linger)
	if !timer.Stop() {
		<-timer.C
	}
	var timerChannel <-chan time.Time
	armTimer := func() {
		if timerChannel == nil && w.config.Linger > 0 {
			timer.Reset(w.config.Linger)
			timerChannel = timer.C
		}
	}
	stopTimer := func() {
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
			w.completeBatch(batch, bucket, 0, fmt.Errorf("%w: bucket %d: %v", ErrWriterState, bucket, err))
			return err
		}
		encoded, count, err := w.encodeBatch(batch, sequences[bucket])
		if err == nil {
			var baseOffset int64
			baseOffset, err = w.backend.produce(
				context.Background(), w.path, bucket, w.tableID, w.partitionID,
				encoded, w.config.Timeout, w.config.Acks,
			)
			if err == nil {
				sequences[bucket]++
				w.completeBatch(batch, bucket, baseOffset, nil)
				return nil
			}
		}
		poisoned[bucket] = err
		w.completeBatch(batch, bucket, 0, err)
		_ = count
		return err
	}
	flushAll := func() error {
		stopTimer()
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
				bucket, err := w.assignBucket(item)
				if err != nil {
					item.future.complete(WriteResult{Err: err})
					continue
				}
				if item.arrow != nil {
					_ = flushBucket(bucket)
					batches[bucket] = &bucketBatch{items: []*pendingWrite{item}}
					_ = flushBucket(bucket)
					continue
				}
				batch := batches[bucket]
				if batch == nil {
					batch = &bucketBatch{}
					batches[bucket] = batch
				}
				if len(batch.items) > 0 && (len(batch.items) >= w.config.MaxBatchRecords || batch.bytes+item.size > w.config.MaxBatchBytes) {
					_ = flushBucket(bucket)
					if w.effectiveAssignment() == AssignmentSticky {
						w.advanceSticky()
						bucket, err = w.assignBucket(item)
						if err != nil {
							item.future.complete(WriteResult{Err: err})
							continue
						}
					}
					batch = batches[bucket]
					if batch == nil {
						batch = &bucketBatch{}
						batches[bucket] = batch
					}
				}
				batch.items = append(batch.items, item)
				batch.records = append(batch.records, Record{Value: item.row, Change: Append, Offset: -1})
				batch.bytes += item.size
				if len(batch.items) >= w.config.MaxBatchRecords || batch.bytes >= w.config.MaxBatchBytes || w.config.Linger == 0 {
					_ = flushBucket(bucket)
					if w.effectiveAssignment() == AssignmentSticky {
						w.advanceSticky()
					}
				} else {
					armTimer()
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

func (w *LogWriter) encodeBatch(batch *bucketBatch, sequence int32) ([]byte, int, error) {
	if len(batch.items) == 1 && batch.items[0].arrow != nil {
		item := batch.items[0]
		encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
			Magic: 0, BaseOffset: -1, SchemaID: int16(w.table.SchemaID), WriterID: w.writerID,
			BatchSequence: sequence, Record: item.arrow, Changes: item.changes,
		}, ArrowCompressionNone, memory.DefaultAllocator)
		return encoded, len(item.changes), err
	}
	encoded, err := (LogBatch{
		Magic: 0, BaseOffset: -1, SchemaID: int16(w.table.SchemaID), AppendOnly: true,
		WriterID: w.writerID, BatchSequence: sequence, Records: batch.records,
	}).EncodeRows(w.table.Schema, true)
	return encoded, len(batch.records), err
}

func (w *LogWriter) completeBatch(batch *bucketBatch, bucket int32, baseOffset int64, err error) {
	offset := baseOffset
	for _, item := range batch.items {
		count := 1
		if item.arrow != nil {
			count = len(item.changes)
		}
		item.future.complete(WriteResult{Bucket: bucket, BaseOffset: offset, Records: count, Err: err})
		offset += int64(count)
	}
}

func (w *LogWriter) effectiveAssignment() BucketAssignment {
	if len(w.table.Schema.BucketKey) != 0 {
		return AssignmentAuto
	}
	if w.config.Assignment == AssignmentAuto {
		return AssignmentSticky
	}
	return w.config.Assignment
}

func (w *LogWriter) assignBucket(item *pendingWrite) (int32, error) {
	if item.bucket != nil {
		return *item.bucket, nil
	}
	if len(w.table.Schema.BucketKey) != 0 {
		values := make(PrimaryKey, len(w.table.Schema.BucketKey))
		columns := w.table.Schema.columnsByName()
		positions := make(map[string]int, len(w.table.Schema.Columns))
		for index, column := range w.table.Schema.Columns {
			positions[column.Name] = index
		}
		for index, name := range w.table.Schema.BucketKey {
			_, ok := columns[name]
			position, found := positions[name]
			if !ok || !found || item.row[position] == nil {
				return 0, fmt.Errorf("%w: invalid bucket key column %q", ErrInvalidRow, name)
			}
			values[index] = item.row[position]
		}
		key, err := encodeKeyColumns(w.table.Schema, w.table.Schema.BucketKey, values)
		if err != nil {
			return 0, err
		}
		bucket, err := flussBucket(key, len(w.buckets))
		if err != nil {
			return 0, err
		}
		return w.buckets[int(bucket)], nil
	}
	if w.effectiveAssignment() == AssignmentRoundRobin {
		bucket := w.buckets[w.roundRobin%len(w.buckets)]
		w.roundRobin++
		return bucket, nil
	}
	if w.stickyIndex < 0 {
		w.stickyIndex = 0
	}
	return w.buckets[w.stickyIndex], nil
}

func (w *LogWriter) advanceSticky() {
	if len(w.buckets) > 1 {
		w.stickyIndex = (w.stickyIndex + 1) % len(w.buckets)
	}
}

func (w *LogWriter) hasBucket(bucket int32) bool {
	for _, candidate := range w.buckets {
		if candidate == bucket {
			return true
		}
	}
	return false
}
