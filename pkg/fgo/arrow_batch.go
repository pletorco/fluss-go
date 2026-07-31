package fgo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ArrowCompression identifies the Arrow IPC compression codec.
type ArrowCompression uint8

// Arrow compression codecs supported by Apache Fluss 0.9.1.
const (
	ArrowCompressionNone ArrowCompression = iota
	ArrowCompressionLZ4
	ArrowCompressionZSTD
)

// ArrowLogBatch is the Arrow-backed variant of a Fluss log batch. EncodeArrowLogBatch only borrows
// Record for the duration of the call. DecodeArrowLogBatch returns an owned Record that remains
// valid until Release is called.
type ArrowLogBatch struct {
	// Magic selects the Fluss v0 or v1 batch header.
	Magic byte
	// BaseOffset is the first row offset.
	BaseOffset int64
	// CommitTime is the server commit time in Unix milliseconds.
	CommitTime int64
	// LeaderEpoch is present only for magic 1.
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
func EncodeArrowLogBatch(batch ArrowLogBatch, compression ArrowCompression, allocator memory.Allocator) ([]byte, error) {
	if batch.Magic != 0 && batch.Magic != 1 {
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
	headerSize := arrowHeaderSize(batch.Magic)
	if len(payload) > maxRowBytes-headerSize-len(changes) {
		return nil, fmt.Errorf("%w: Arrow batch exceeds limit", ErrMalformedRecordBatch)
	}
	encoded := make([]byte, headerSize, headerSize+len(changes)+len(payload))
	crcOffset, schemaOffset := writeArrowHeader(encoded, batch, count)
	encoded = append(encoded, changes...)
	encoded = append(encoded, payload...)
	binary.LittleEndian.PutUint32(encoded[8:], uint32(len(encoded)-12))
	binary.LittleEndian.PutUint32(encoded[crcOffset:], crc32.Checksum(encoded[schemaOffset:], crc32.MakeTable(crc32.Castagnoli)))
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

func arrowHeaderSize(magic byte) int {
	if magic == 1 {
		return logBatchV1HeaderSize
	}
	return logBatchV0HeaderSize
}

func writeArrowHeader(encoded []byte, batch ArrowLogBatch, count int) (int, int) {
	// Arrow batches share the row-oriented log v0/v1 header documented in
	// record_batch.go. The header is followed by one change byte per row for
	// non-append-only batches and then by the Arrow IPC payload. Batch length
	// excludes the first 12 bytes; CRC32C starts at schemaOffset and therefore
	// covers the common tail, changes, and payload.
	//
	// The layout is pinned to Apache Fluss 0.9.1 commit
	// 6bf969f71af8d6f9cc37383ab89ae46a58b0e227 and byte-locked by
	// TestArrowLogBatchDecodesJava091Fixture.
	binary.LittleEndian.PutUint64(encoded, uint64(batch.BaseOffset))
	encoded[12] = batch.Magic
	binary.LittleEndian.PutUint64(encoded[13:], uint64(batch.CommitTime))
	crcOffset, schemaOffset := 21, 25
	if batch.Magic == 1 {
		binary.LittleEndian.PutUint32(encoded[21:], uint32(batch.LeaderEpoch))
		crcOffset, schemaOffset = 25, 29
	}
	binary.LittleEndian.PutUint16(encoded[schemaOffset:], uint16(batch.SchemaID))
	if batch.AppendOnly {
		encoded[schemaOffset+2] = 1
	}
	lastOffset := schemaOffset + 3
	if count > 0 {
		binary.LittleEndian.PutUint32(encoded[lastOffset:], uint32(count-1))
	}
	binary.LittleEndian.PutUint64(encoded[lastOffset+4:], uint64(batch.WriterID))
	binary.LittleEndian.PutUint32(encoded[lastOffset+12:], uint32(batch.BatchSequence))
	binary.LittleEndian.PutUint32(encoded[lastOffset+16:], uint32(count))
	return crcOffset, schemaOffset
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
	magic, headerSize, crcOffset, schemaOffset, err := arrowHeader(encoded)
	if err != nil {
		return nil, err
	}
	if int(binary.LittleEndian.Uint32(encoded[8:])) != len(encoded)-12 ||
		binary.LittleEndian.Uint32(encoded[crcOffset:]) != crc32.Checksum(encoded[schemaOffset:], crc32.MakeTable(crc32.Castagnoli)) {
		return nil, fmt.Errorf("%w: invalid Arrow batch length or checksum", ErrMalformedRecordBatch)
	}
	lastOffset := schemaOffset + 3
	count := int(binary.LittleEndian.Uint32(encoded[lastOffset+16:]))
	appendOnly := encoded[schemaOffset+2]&1 != 0
	changeBytes := 0
	if !appendOnly {
		changeBytes = count
	}
	if count > maxRowBytes || changeBytes > len(encoded)-headerSize {
		return nil, fmt.Errorf("%w: invalid Arrow record count", ErrMalformedRecordBatch)
	}
	changes, position, err := decodeArrowChanges(encoded, headerSize, count, appendOnly)
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
	if magic == 1 {
		batch.LeaderEpoch = int32(binary.LittleEndian.Uint32(encoded[21:]))
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

func arrowHeader(encoded []byte) (byte, int, int, int, error) {
	if len(encoded) < 13 || len(encoded) > maxRowBytes {
		return 0, 0, 0, 0, fmt.Errorf("%w: invalid Arrow batch length", ErrMalformedRecordBatch)
	}
	switch encoded[12] {
	case 0:
		if len(encoded) < logBatchV0HeaderSize {
			break
		}
		return 0, logBatchV0HeaderSize, 21, 25, nil
	case 1:
		if len(encoded) < logBatchV1HeaderSize {
			break
		}
		return 1, logBatchV1HeaderSize, 25, 29, nil
	}
	return 0, 0, 0, 0, fmt.Errorf("%w: invalid Arrow batch header", ErrMalformedRecordBatch)
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

func encodeArrowPayload(record arrow.RecordBatch, compression ArrowCompression, allocator memory.Allocator) ([]byte, error) {
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
