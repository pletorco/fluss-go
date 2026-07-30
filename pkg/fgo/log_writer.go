package fgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
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

type LogWriteFormat string

const (
	LogWriteFormatAuto      LogWriteFormat = "auto"
	LogWriteFormatArrow     LogWriteFormat = "arrow"
	LogWriteFormatIndexed   LogWriteFormat = "indexed"
	LogWriteFormatCompacted LogWriteFormat = "compacted"
)

type LogWriterConfig struct {
	MaxBatchBytes    int
	MaxBatchRecords  int
	MaxBuffered      int
	Linger           time.Duration
	Timeout          time.Duration
	Acks             int32
	Assignment       BucketAssignment
	Partition        string
	Format           LogWriteFormat
	ArrowCompression ArrowCompression

	arrowCompressionSet bool
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

// WithLogWriteFormat selects the server-supported encoding used by this writer.
func WithLogWriteFormat(format LogWriteFormat) LogWriterOption {
	return func(config *LogWriterConfig) error {
		switch format {
		case LogWriteFormatAuto, LogWriteFormatArrow, LogWriteFormatIndexed, LogWriteFormatCompacted:
			config.Format = format
			return nil
		default:
			return fmt.Errorf("%w: unsupported log write format %q", ErrInvalidConfig, format)
		}
	}
}

// WithLogArrowCompression selects the compression for Arrow log batches.
func WithLogArrowCompression(compression ArrowCompression) LogWriterOption {
	return func(config *LogWriterConfig) error {
		switch compression {
		case ArrowCompressionNone, ArrowCompressionLZ4, ArrowCompressionZSTD:
			config.ArrowCompression = compression
			config.arrowCompressionSet = true
			return nil
		default:
			return fmt.Errorf("%w: unsupported Arrow compression %d", ErrInvalidConfig, compression)
		}
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
	produce(context.Context, logProduceRequest) (int64, error)
}

type clientLogWriterBackend struct{ client *Client }

type logProduceRequest struct {
	path        PhysicalTablePath
	bucket      int32
	tableID     int64
	partitionID int64
	records     []byte
	timeout     time.Duration
	acks        int32
}

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
	input logProduceRequest,
) (int64, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyProduceLog, 0)
	if err != nil {
		return 0, err
	}
	if input.timeout/time.Millisecond > math.MaxInt32 {
		return 0, fmt.Errorf("%w: log timeout exceeds protocol range", ErrInvalidConfig)
	}
	message := request.Message().(*fmsg.ProduceLogRequest)
	message.Acks = proto.Int32(input.acks)
	message.TableId = proto.Int64(input.tableID)
	message.TimeoutMs = proto.Int32(int32(input.timeout / time.Millisecond))
	bucketRequest := &fmsg.PbProduceLogReqForBucket{BucketId: proto.Int32(input.bucket), Records: input.records}
	if input.path.Partition != "" {
		bucketRequest.PartitionId = proto.Int64(input.partitionID)
	}
	message.BucketsReq = []*fmsg.PbProduceLogReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, input.path, input.bucket, request)
	if err != nil {
		return 0, err
	}
	produced, ok := response.Message().(*fmsg.ProduceLogResponse)
	if !ok {
		return 0, fmt.Errorf("fgo: produce log: unexpected response %T", response.Message())
	}
	if len(produced.GetBucketsResp()) != 1 || produced.GetBucketsResp()[0].GetBucketId() != input.bucket {
		return 0, fmt.Errorf("%w: produce response omitted bucket %d", ErrValidation, input.bucket)
	}
	result := produced.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyProduceLog); err != nil {
		return 0, err
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

type logWriterLoop struct {
	writer    *LogWriter
	batches   map[int32]*bucketBatch
	sequences map[int32]int32
	poisoned  map[int32]error
	timer     *time.Timer
	timerC    <-chan time.Time
}

func (c *Client) NewLogWriter(ctx context.Context, table Table, options ...LogWriterOption) (*LogWriter, error) {
	return newLogWriter(ctx, clientLogWriterBackend{client: c}, table, options...)
}

func newLogWriter(ctx context.Context, backend logWriterBackend, table Table, options ...LogWriterOption) (*LogWriter, error) {
	if err := validateLogWriterTable(table); err != nil {
		return nil, err
	}
	config, err := logWriterConfig(options)
	if err != nil {
		return nil, err
	}
	if config.arrowCompressionSet &&
		config.Format != LogWriteFormatAuto && config.Format != LogWriteFormatArrow {
		return nil, fmt.Errorf("%w: Arrow compression requires Arrow or auto format", ErrInvalidConfig)
	}
	if configured := strings.TrimSpace(table.Properties["table.log.format"]); configured != "" &&
		config.Format != LogWriteFormatAuto && !strings.EqualFold(configured, string(config.Format)) {
		return nil, fmt.Errorf(
			"%w: writer format %s does not match table.log.format %s",
			ErrInvalidConfig, config.Format, configured,
		)
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

func validateLogWriterTable(table Table) error {
	if err := table.RequireLog(); err != nil {
		return err
	}
	if err := table.Schema.Validate(); err != nil {
		return err
	}
	if table.SchemaID < 0 || table.SchemaID > math.MaxInt16 {
		return fmt.Errorf("%w: schema ID exceeds log batch range", ErrInvalidSchema)
	}
	return nil
}

func logWriterConfig(options []LogWriterOption) (LogWriterConfig, error) {
	config := LogWriterConfig{
		MaxBatchBytes: 1 << 20, MaxBatchRecords: 1000, MaxBuffered: 10_000,
		Linger: 5 * time.Millisecond, Timeout: 30 * time.Second, Acks: -1,
		Assignment: AssignmentAuto, Format: LogWriteFormatAuto,
	}
	for _, option := range options {
		if option == nil {
			return LogWriterConfig{}, fmt.Errorf("%w: nil log writer option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return LogWriterConfig{}, err
		}
	}
	return config, nil
}

// Append queues one row. The returned future completes exactly once after acknowledgment or
// failure. The writer copies row bytes while encoding, so the caller may reuse the row afterward.
func (w *LogWriter) Append(ctx context.Context, row Row) *WriteFuture {
	future := newWriteFuture()
	if ctx == nil {
		future.complete(WriteResult{Err: fmt.Errorf("%w: nil context", ErrInvalidConfig)})
		return future
	}
	if w.config.Format == LogWriteFormatArrow {
		future.complete(WriteResult{
			Err: fmt.Errorf("%w: Arrow format requires AppendArrow", ErrInvalidConfig),
		})
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
	if w.config.Format == LogWriteFormatIndexed || w.config.Format == LogWriteFormatCompacted {
		future.complete(WriteResult{
			Err: fmt.Errorf("%w: %s format does not accept Arrow batches", ErrInvalidConfig, w.config.Format),
		})
		return future
	}
	expected, err := w.table.Schema.ArrowSchema()
	if err != nil {
		future.complete(WriteResult{Err: err})
		return future
	}
	if !expected.Equal(batch.Schema()) {
		future.complete(WriteResult{Err: fmt.Errorf("%w: Arrow batch schema does not match table", ErrInvalidSchema)})
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
	timer := time.NewTimer(w.config.Linger)
	if !timer.Stop() {
		<-timer.C
	}
	loop := &logWriterLoop{
		writer: w, batches: make(map[int32]*bucketBatch), sequences: make(map[int32]int32),
		poisoned: make(map[int32]error), timer: timer,
	}
	loop.run()
}

func (l *logWriterLoop) run() {
	for {
		select {
		case command := <-l.writer.commands:
			switch {
			case command.item != nil:
				l.add(command.item)
			case command.flush != nil:
				command.flush <- l.flushAll()
			case command.close != nil:
				command.close <- l.flushAll()
				l.timer.Stop()
				return
			}
		case <-l.timerC:
			l.timerC = nil
			_ = l.flushAll()
		}
	}
}

func (l *logWriterLoop) add(item *pendingWrite) {
	if err := item.ctx.Err(); err != nil {
		item.future.complete(WriteResult{Err: err})
		return
	}
	bucket, err := l.writer.assignBucket(item)
	if err != nil {
		item.future.complete(WriteResult{Err: err})
		return
	}
	if item.arrow != nil {
		_ = l.flushBucket(bucket)
		l.batches[bucket] = &bucketBatch{items: []*pendingWrite{item}}
		_ = l.flushBucket(bucket)
		return
	}
	bucket, batch, ok := l.availableBatch(bucket, item)
	if !ok {
		return
	}
	batch.items = append(batch.items, item)
	batch.records = append(batch.records, Record{Value: item.row, Change: Append, Offset: -1})
	batch.bytes += item.size
	if l.batchFull(batch) || l.writer.config.Linger == 0 {
		_ = l.flushBucket(bucket)
		if l.writer.effectiveAssignment() == AssignmentSticky {
			l.writer.advanceSticky()
		}
		return
	}
	l.armTimer()
}

func (l *logWriterLoop) availableBatch(bucket int32, item *pendingWrite) (int32, *bucketBatch, bool) {
	batch := l.batch(bucket)
	if len(batch.items) == 0 || !l.batchFullWith(batch, item) {
		return bucket, batch, true
	}
	_ = l.flushBucket(bucket)
	if l.writer.effectiveAssignment() != AssignmentSticky {
		return bucket, l.batch(bucket), true
	}
	l.writer.advanceSticky()
	next, err := l.writer.assignBucket(item)
	if err != nil {
		item.future.complete(WriteResult{Err: err})
		return 0, nil, false
	}
	return next, l.batch(next), true
}

func (l *logWriterLoop) batch(bucket int32) *bucketBatch {
	batch := l.batches[bucket]
	if batch == nil {
		batch = &bucketBatch{}
		l.batches[bucket] = batch
	}
	return batch
}

func (l *logWriterLoop) batchFull(batch *bucketBatch) bool {
	return len(batch.items) >= l.writer.config.MaxBatchRecords || batch.bytes >= l.writer.config.MaxBatchBytes
}

func (l *logWriterLoop) batchFullWith(batch *bucketBatch, item *pendingWrite) bool {
	return len(batch.items) >= l.writer.config.MaxBatchRecords ||
		batch.bytes+item.size > l.writer.config.MaxBatchBytes
}

func (l *logWriterLoop) flushBucket(bucket int32) error {
	batch := l.batches[bucket]
	if batch == nil || len(batch.items) == 0 {
		return nil
	}
	delete(l.batches, bucket)
	if err := l.poisoned[bucket]; err != nil {
		l.writer.completeBatch(batch, bucket, 0, fmt.Errorf("%w: bucket %d: %v", ErrWriterState, bucket, err))
		return err
	}
	encoded, _, err := l.writer.encodeBatch(batch, l.sequences[bucket])
	var baseOffset int64
	if err == nil {
		baseOffset, err = l.writer.backend.produce(context.Background(), logProduceRequest{
			path: l.writer.path, bucket: bucket, tableID: l.writer.tableID, partitionID: l.writer.partitionID,
			records: encoded, timeout: l.writer.config.Timeout, acks: l.writer.config.Acks,
		})
	}
	if err != nil {
		l.poisoned[bucket] = err
		l.writer.completeBatch(batch, bucket, 0, err)
		return err
	}
	l.sequences[bucket]++
	l.writer.completeBatch(batch, bucket, baseOffset, nil)
	return nil
}

func (l *logWriterLoop) flushAll() error {
	l.stopTimer()
	var joined error
	for _, bucket := range l.writer.buckets {
		joined = errors.Join(joined, l.flushBucket(bucket))
	}
	return joined
}

func (l *logWriterLoop) armTimer() {
	if l.timerC == nil && l.writer.config.Linger > 0 {
		l.timer.Reset(l.writer.config.Linger)
		l.timerC = l.timer.C
	}
}

func (l *logWriterLoop) stopTimer() {
	if l.timerC != nil && !l.timer.Stop() {
		select {
		case <-l.timer.C:
		default:
		}
	}
	l.timerC = nil
}

func (w *LogWriter) encodeBatch(batch *bucketBatch, sequence int32) ([]byte, int, error) {
	if len(batch.items) == 1 && batch.items[0].arrow != nil {
		item := batch.items[0]
		encoded, err := EncodeArrowLogBatch(ArrowLogBatch{
			Magic: 0, BaseOffset: -1, SchemaID: int16(w.table.SchemaID), WriterID: w.writerID,
			BatchSequence: sequence, Record: item.arrow, Changes: item.changes,
		}, w.config.ArrowCompression, memory.DefaultAllocator)
		return encoded, len(item.changes), err
	}
	encoded, err := (LogBatch{
		Magic: 0, BaseOffset: -1, SchemaID: int16(w.table.SchemaID), AppendOnly: true,
		WriterID: w.writerID, BatchSequence: sequence, Records: batch.records,
	}).EncodeRows(w.table.Schema, w.config.Format != LogWriteFormatIndexed)
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
		values, err := w.bucketKeyValues(item.row)
		if err != nil {
			return 0, err
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

func (w *LogWriter) bucketKeyValues(row Row) (PrimaryKey, error) {
	values := make(PrimaryKey, len(w.table.Schema.BucketKey))
	positions := make(map[string]int, len(w.table.Schema.Columns))
	for index, column := range w.table.Schema.Columns {
		positions[column.Name] = index
	}
	for index, name := range w.table.Schema.BucketKey {
		position, found := positions[name]
		if !found || row[position] == nil {
			return nil, fmt.Errorf("%w: invalid bucket key column %q", ErrInvalidRow, name)
		}
		values[index] = row[position]
	}
	return values, nil
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
