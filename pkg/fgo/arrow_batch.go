package fgo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ArrowCompressionType identifies the Arrow IPC compression codec.
type ArrowCompressionType uint8

// Arrow compression codecs supported by Apache Fluss 1.0.
const (
	ArrowCompressionNone ArrowCompressionType = iota
	ArrowCompressionLZ4
	ArrowCompressionZSTD
)

// ArrowLogBatch is the Arrow-backed variant of a Fluss log batch. EncodeArrowLogBatch only borrows
// Record for the duration of the call. DecodeArrowLogBatch returns an owned Record that remains
// valid until Release is called.
type ArrowLogBatch struct {
	// Magic selects the Fluss v0, v1, or v2 batch header.
	Magic byte
	// BaseOffset is the first row offset.
	BaseOffset int64
	// CommitTime is the server commit time in Unix milliseconds.
	CommitTime int64
	// LeaderEpoch is present only for magic 2.
	LeaderEpoch int32
	// SchemaID identifies the Arrow schema.
	SchemaID int16
	// AppendOnly omits per-row change bytes when true.
	AppendOnly bool
	// WriterID identifies the idempotent writer session.
	WriterID int64
	// BatchSequence is monotonic within WriterID and bucket.
	BatchSequence int32
	// Record is borrowed during encoding and owned after decoding.
	Record arrow.RecordBatch
	// Changes has one entry per row unless AppendOnly is true.
	Changes []ChangeType

	owned   bool
	release *sync.Once
}

// Release releases a record owned by a decoded batch. It is safe to call more than once.
func (b *ArrowLogBatch) Release() {
	if b == nil || !b.owned {
		return
	}
	b.release.Do(func() {
		if b.Record != nil {
			b.Record.Release()
			b.Record = nil
		}
	})
}

// EncodeArrowLogBatch encodes an Arrow-backed Fluss log batch.
// The function borrows batch.Record only for the duration of the call.
func EncodeArrowLogBatch(batch ArrowLogBatch, compression ArrowCompressionType, allocator memory.Allocator) ([]byte, error) {
	layout, ok := logLayout(batch.Magic)
	if !ok {
		return nil, fmt.Errorf("%w: unsupported log magic %d", ErrMalformedRecordBatch, batch.Magic)
	}
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	count, err := arrowRecordCount(batch.Record)
	if err != nil {
		return nil, err
	}
	changes, err := arrowChanges(batch, count)
	if err != nil {
		return nil, err
	}
	payload, err := encodeArrowPayload(batch.Record, compression, allocator)
	if err != nil {
		return nil, err
	}
	if count == 0 && len(payload) != 0 {
		return nil, fmt.Errorf("%w: empty Arrow batch has payload", ErrMalformedRecordBatch)
	}
	headerSize := layout.size
	if len(payload) > maxRowBytes-headerSize-len(changes) {
		return nil, fmt.Errorf("%w: Arrow batch exceeds limit", ErrMalformedRecordBatch)
	}
	encoded := make([]byte, headerSize, headerSize+len(changes)+len(payload))
	writeLogBatchHeader(encoded, layout, LogBatch{
		Magic: batch.Magic, BaseOffset: batch.BaseOffset, CommitTime: batch.CommitTime,
		LeaderEpoch: batch.LeaderEpoch, SchemaID: batch.SchemaID, AppendOnly: batch.AppendOnly,
		WriterID: batch.WriterID, BatchSequence: batch.BatchSequence,
	}, count)
	encoded = append(encoded, changes...)
	encoded = append(encoded, payload...)
	finishLogBatch(encoded, layout)
	return encoded, nil
}

func arrowRecordCount(record arrow.RecordBatch) (int, error) {
	if record == nil {
		return 0, nil
	}
	if record.NumRows() > int64(^uint32(0)>>1) {
		return 0, fmt.Errorf("%w: too many Arrow rows", ErrMalformedRecordBatch)
	}
	return int(record.NumRows()), nil
}

// DecodeArrowLogBatch decodes a Fluss Arrow log batch.
// The caller owns the returned batch and must call [ArrowLogBatch.Release].
func DecodeArrowLogBatch(schema *arrow.Schema, encoded []byte, allocator memory.Allocator) (*ArrowLogBatch, error) {
	if schema == nil {
		return nil, fmt.Errorf("%w: nil Arrow schema", ErrInvalidSchema)
	}
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	magic, recordsOffset, schemaOffset, err := logBatchHeader(encoded)
	if err != nil {
		return nil, err
	}
	lastOffset := schemaOffset + 3
	rawCount := binary.LittleEndian.Uint32(encoded[lastOffset+16:])
	if rawCount > maxRowBytes {
		return nil, fmt.Errorf("%w: invalid Arrow record count", ErrMalformedRecordBatch)
	}
	count := int(rawCount)
	appendOnly := encoded[schemaOffset+2]&1 != 0
	changeBytes := 0
	if !appendOnly {
		changeBytes = count
	}
	if changeBytes > len(encoded)-recordsOffset {
		return nil, fmt.Errorf("%w: invalid Arrow record count", ErrMalformedRecordBatch)
	}
	changes, position, err := decodeArrowChanges(encoded, recordsOffset, count, appendOnly)
	if err != nil {
		return nil, err
	}
	record, err := decodeArrowRecord(schema, encoded[position:], count, allocator)
	if err != nil {
		return nil, err
	}
	batch := &ArrowLogBatch{
		Magic: magic, BaseOffset: int64(binary.LittleEndian.Uint64(encoded)),
		CommitTime: int64(binary.LittleEndian.Uint64(encoded[13:])),
		SchemaID:   int16(binary.LittleEndian.Uint16(encoded[schemaOffset:])), AppendOnly: appendOnly,
		WriterID:      int64(binary.LittleEndian.Uint64(encoded[lastOffset+4:])),
		BatchSequence: int32(binary.LittleEndian.Uint32(encoded[lastOffset+12:])),
		Record:        record, Changes: changes, owned: record != nil, release: &sync.Once{},
	}
	if magic == 2 {
		batch.LeaderEpoch = int32(binary.LittleEndian.Uint32(encoded[logBatchEpochOffset:]))
	}
	return batch, nil
}

func decodeArrowChanges(encoded []byte, position, count int, appendOnly bool) ([]ChangeType, int, error) {
	changes := make([]ChangeType, count)
	if appendOnly {
		for index := range changes {
			changes[index] = Append
		}
		return changes, position, nil
	}
	for index := range changes {
		changes[index] = ChangeType(encoded[position+index])
		if err := changes[index].Validate(); err != nil {
			return nil, position, fmt.Errorf("%w: invalid Arrow change type", ErrMalformedRecordBatch)
		}
	}
	return changes, position + count, nil
}

func decodeArrowRecord(
	schema *arrow.Schema,
	payload []byte,
	count int,
	allocator memory.Allocator,
) (arrow.RecordBatch, error) {
	if count == 0 {
		if len(payload) != 0 {
			return nil, fmt.Errorf("%w: empty Arrow batch has trailing bytes", ErrMalformedRecordBatch)
		}
		return nil, nil
	}
	record, err := decodeArrowPayload(schema, payload, allocator)
	if err != nil {
		return nil, err
	}
	if record.NumRows() != int64(count) {
		record.Release()
		return nil, fmt.Errorf("%w: Arrow row count mismatch", ErrMalformedRecordBatch)
	}
	return record, nil
}

func arrowChanges(batch ArrowLogBatch, count int) ([]byte, error) {
	if batch.AppendOnly {
		if len(batch.Changes) != 0 && len(batch.Changes) != count {
			return nil, fmt.Errorf("%w: Arrow change count mismatch", ErrMalformedRecordBatch)
		}
		for _, change := range batch.Changes {
			if change != Append {
				return nil, fmt.Errorf("%w: append-only Arrow batch contains %d", ErrInvalidRow, change)
			}
		}
		return nil, nil
	}
	if len(batch.Changes) != count {
		return nil, fmt.Errorf("%w: Arrow change count mismatch", ErrMalformedRecordBatch)
	}
	changes := make([]byte, count)
	for i, change := range batch.Changes {
		if err := change.Validate(); err != nil {
			return nil, err
		}
		changes[i] = byte(change)
	}
	return changes, nil
}

type arrowPayloadCapture struct {
	calls   int
	payload bytes.Buffer
}

func (*arrowPayloadCapture) Start() error { return nil }
func (*arrowPayloadCapture) Close() error { return nil }
func (w *arrowPayloadCapture) WritePayload(payload ipc.Payload) error {
	w.calls++
	if w.calls == 1 {
		return nil
	}
	if w.calls > 2 {
		return errors.New("fgo: Arrow dictionaries are not supported")
	}
	_, err := payload.WritePayload(&w.payload)
	return err
}

func encodeArrowPayload(record arrow.RecordBatch, compression ArrowCompressionType, allocator memory.Allocator) ([]byte, error) {
	if record == nil || record.NumRows() == 0 {
		return nil, nil
	}
	capture := &arrowPayloadCapture{}
	options := []ipc.Option{ipc.WithAllocator(allocator), ipc.WithSchema(record.Schema())}
	switch compression {
	case ArrowCompressionNone:
	case ArrowCompressionLZ4:
		options = append(options, ipc.WithLZ4())
	case ArrowCompressionZSTD:
		options = append(options, ipc.WithZstd())
	default:
		return nil, fmt.Errorf("%w: unsupported Arrow compression %d", ErrInvalidConfig, compression)
	}
	writer := ipc.NewWriterWithPayloadWriter(capture, options...)
	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("%w: encode Arrow record: %v", ErrMalformedRecordBatch, err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("%w: close Arrow writer: %v", ErrMalformedRecordBatch, err)
	}
	if capture.calls != 2 {
		return nil, fmt.Errorf("%w: missing Arrow record payload", ErrMalformedRecordBatch)
	}
	return append([]byte(nil), capture.payload.Bytes()...), nil
}

func decodeArrowPayload(schema *arrow.Schema, payload []byte, allocator memory.Allocator) (arrow.RecordBatch, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("%w: missing Arrow payload", ErrMalformedRecordBatch)
	}
	var stream bytes.Buffer
	writer := ipc.NewWriter(&stream, ipc.WithAllocator(allocator), ipc.WithSchema(schema))
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("%w: encode Arrow schema: %v", ErrMalformedRecordBatch, err)
	}
	const eosBytes = 8
	if stream.Len() < eosBytes {
		return nil, fmt.Errorf("%w: invalid Arrow schema stream", ErrMalformedRecordBatch)
	}
	schemaPrefix := stream.Bytes()[:stream.Len()-eosBytes]
	combined := make([]byte, 0, len(schemaPrefix)+len(payload)+eosBytes)
	combined = append(combined, schemaPrefix...)
	combined = append(combined, payload...)
	combined = append(combined, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0)
	reader, err := ipc.NewReader(bytes.NewReader(combined), ipc.WithAllocator(allocator))
	if err != nil {
		return nil, fmt.Errorf("%w: decode Arrow stream: %v", ErrMalformedRecordBatch, err)
	}
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("%w: decode Arrow record: %v", ErrMalformedRecordBatch, err)
		}
		return nil, fmt.Errorf("%w: Arrow stream has no record", ErrMalformedRecordBatch)
	}
	record := reader.RecordBatch()
	record.Retain()
	if reader.Next() {
		record.Release()
		return nil, fmt.Errorf("%w: Arrow stream has multiple records", ErrMalformedRecordBatch)
	}
	if err := reader.Err(); err != nil {
		record.Release()
		return nil, fmt.Errorf("%w: decode Arrow record: %v", ErrMalformedRecordBatch, err)
	}
	return record, nil
}
