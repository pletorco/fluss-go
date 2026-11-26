package fgo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	kvBatchHeaderSize    = 28
	logBatchV0HeaderSize = 48
	logBatchV1HeaderSize = 52
	logBatchV2HeaderSize = 56
	logBatchEpochOffset  = 21
)

// KV batch bytes are [length:4][magic:1][CRC32C:4][schema:2][reserved:1]
// [writer ID:8][sequence:4][record count:4], followed by length-delimited
// records. The length excludes its own four bytes and the checksum covers from
// schema through the final record.
//
// Log v0 bytes are [base offset:8][length:4][magic:1][commit time:8]
// [CRC32C:4] plus the common schema-and-writer tail. Fluss 1.0 log v1 adds
// a four-byte statistics length and optional statistics before the records.
// V2 also inserts the leader epoch before the checksum. Lengths exclude the
// first 12 bytes, and checksums cover schema through the final record.

// ErrMalformedRecordBatch reports an invalid KV, row, or Arrow batch encoding.
var ErrMalformedRecordBatch = errors.New("fgo: malformed record batch")

// KVRecord is one raw primary-key record. A nil Value represents a delete. Buffers returned by a
// decoder are owned by the caller.
type KVRecord struct {
	// Key contains an encoded non-empty primary key.
	Key []byte
	// Value contains an encoded row; nil represents a delete.
	Value []byte
}

// KVBatch carries the writer state required for idempotent KV writes.
type KVBatch struct {
	// SchemaID identifies the row schema used by Records.
	SchemaID int16
	// WriterID identifies the idempotent writer session.
	WriterID int64
	// BatchSequence is monotonic within WriterID and bucket.
	BatchSequence int32
	// Records are encoded in request order.
	Records []KVRecord
}

// Encode serializes the KV batch using the Apache Fluss 1.0 layout.
func (b KVBatch) Encode() ([]byte, error) {
	if len(b.Records) > int(^uint32(0)>>1) {
		return nil, fmt.Errorf("%w: too many KV records", ErrMalformedRecordBatch)
	}
	encoded := make([]byte, kvBatchHeaderSize)
	encoded[4] = 0
	binary.LittleEndian.PutUint16(encoded[9:], uint16(b.SchemaID))
	binary.LittleEndian.PutUint64(encoded[12:], uint64(b.WriterID))
	binary.LittleEndian.PutUint32(encoded[20:], uint32(b.BatchSequence))
	binary.LittleEndian.PutUint32(encoded[24:], uint32(len(b.Records)))
	for _, record := range b.Records {
		if len(record.Key) > maxRowBytes || len(record.Value) > maxRowBytes {
			return nil, fmt.Errorf("%w: KV record exceeds limit", ErrMalformedRecordBatch)
		}
		body := appendVar32(nil, uint32(len(record.Key)))
		body = append(body, record.Key...)
		body = append(body, record.Value...)
		if len(body) > maxRowBytes || len(encoded)+4+len(body) > maxRowBytes {
			return nil, fmt.Errorf("%w: KV batch exceeds limit", ErrMalformedRecordBatch)
		}
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(len(body)))
		encoded = append(encoded, length[:]...)
		encoded = append(encoded, body...)
	}
	binary.LittleEndian.PutUint32(encoded, uint32(len(encoded)-4))
	binary.LittleEndian.PutUint32(encoded[5:], crc32.Checksum(encoded[9:], crc32.MakeTable(crc32.Castagnoli)))
	return encoded, nil
}

// DecodeKVBatch validates and decodes a KV batch.
// Returned key and value buffers are owned by the caller.
func DecodeKVBatch(encoded []byte) (KVBatch, error) {
	if len(encoded) < kvBatchHeaderSize || len(encoded) > maxRowBytes || encoded[4] != 0 {
		return KVBatch{}, fmt.Errorf("%w: invalid KV batch header", ErrMalformedRecordBatch)
	}
	if int(binary.LittleEndian.Uint32(encoded)) != len(encoded)-4 || binary.LittleEndian.Uint32(encoded[5:]) != crc32.Checksum(encoded[9:], crc32.MakeTable(crc32.Castagnoli)) {
		return KVBatch{}, fmt.Errorf("%w: invalid KV batch length or checksum", ErrMalformedRecordBatch)
	}
	count := int(binary.LittleEndian.Uint32(encoded[24:]))
	if count > len(encoded)/5 {
		return KVBatch{}, fmt.Errorf("%w: invalid KV record count", ErrMalformedRecordBatch)
	}
	batch := KVBatch{SchemaID: int16(binary.LittleEndian.Uint16(encoded[9:])), WriterID: int64(binary.LittleEndian.Uint64(encoded[12:])), BatchSequence: int32(binary.LittleEndian.Uint32(encoded[20:])), Records: make([]KVRecord, 0, count)}
	position := kvBatchHeaderSize
	for range count {
		record, next, err := decodeKVRecord(encoded, position)
		if err != nil {
			return KVBatch{}, err
		}
		batch.Records = append(batch.Records, record)
		position = next
	}
	if position != len(encoded) {
		return KVBatch{}, fmt.Errorf("%w: trailing KV bytes", ErrMalformedRecordBatch)
	}
	return batch, nil
}

func decodeKVRecord(encoded []byte, position int) (KVRecord, int, error) {
	if len(encoded)-position < 4 {
		return KVRecord{}, position, fmt.Errorf("%w: truncated KV record", ErrMalformedRecordBatch)
	}
	length := int(binary.LittleEndian.Uint32(encoded[position:]))
	position += 4
	if length < 1 || length > len(encoded)-position {
		return KVRecord{}, position, fmt.Errorf("%w: invalid KV record length", ErrMalformedRecordBatch)
	}
	bodyEnd := position + length
	keyLength, keyStart, err := readVar(encoded[:bodyEnd], position, 5)
	if err != nil || keyLength > uint64(bodyEnd-keyStart) {
		return KVRecord{}, position, fmt.Errorf("%w: invalid KV key length", ErrMalformedRecordBatch)
	}
	keyEnd := keyStart + int(keyLength)
	record := KVRecord{Key: append([]byte(nil), encoded[keyStart:keyEnd]...)}
	if keyEnd != bodyEnd {
		record.Value = append([]byte(nil), encoded[keyEnd:bodyEnd]...)
	}
	return record, bodyEnd, nil
}

// LogBatch encodes compacted or indexed row records using the Fluss 1.0 layouts.
type LogBatch struct {
	// Magic selects the Fluss v0, v1, or v2 batch header.
	Magic byte
	// BaseOffset is the first record offset.
	BaseOffset int64
	// CommitTime is the server commit time in Unix milliseconds.
	CommitTime int64
	// LeaderEpoch is present only for magic 2.
	LeaderEpoch int32
	// SchemaID identifies the row schema used by Records.
	SchemaID int16
	// AppendOnly omits per-record change bytes when true.
	AppendOnly bool
	// WriterID identifies the idempotent writer session.
	WriterID int64
	// BatchSequence is monotonic within WriterID and bucket.
	BatchSequence int32
	// Records are ordered by offset.
	Records []Record
}

// EncodeRows serializes row records using compacted or indexed encoding.
func (b LogBatch) EncodeRows(schema Schema, compacted bool) ([]byte, error) {
	return b.encodeRowRecords(len(b.Records), func(index int) ([]byte, ChangeType, error) {
		record := b.Records[index]
		if err := record.Change.Validate(); err != nil {
			return nil, record.Change, err
		}
		encoding := compactedEncoding
		if !compacted {
			encoding = indexedEncoding
		}
		row, err := encodeRow(schema, record.Value, encoding)
		return row, record.Change, err
	})
}

// encodeRowRecords shares the batch framing between public row encoding and
// append writers that already own validated, encoded row snapshots.
func (b LogBatch) encodeRowRecords(count int, recordAt func(int) ([]byte, ChangeType, error)) ([]byte, error) {
	layout, ok := logLayout(b.Magic)
	if !ok {
		return nil, fmt.Errorf("%w: unsupported log magic %d", ErrMalformedRecordBatch, b.Magic)
	}
	encoded := make([]byte, layout.size)
	writeLogBatchHeader(encoded, layout, b, count)
	for index := range count {
		row, change, err := recordAt(index)
		if err != nil {
			return nil, err
		}
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(1+len(row)))
		encoded = append(encoded, length[:]...)
		encoded = append(encoded, byte(change))
		encoded = append(encoded, row...)
	}
	finishLogBatch(encoded, layout)
	return encoded, nil
}

// DecodeLogBatchRows decodes a single Fluss v0, v1, or v2 row-oriented log batch.
// It validates the CRC before allocating records and skips optional statistics.
func DecodeLogBatchRows(schema Schema, encoded []byte, compacted bool) (LogBatch, error) {
	magic, recordsOffset, schemaOffset, err := logBatchHeader(encoded)
	if err != nil {
		return LogBatch{}, err
	}
	lastOffset := schemaOffset + 3
	rawCount := binary.LittleEndian.Uint32(encoded[lastOffset+16:])
	if rawCount > uint32((len(encoded)-recordsOffset)/5) {
		return LogBatch{}, fmt.Errorf("%w: invalid log record count", ErrMalformedRecordBatch)
	}
	count := int(rawCount)
	batch := LogBatch{
		Magic: magic, BaseOffset: int64(binary.LittleEndian.Uint64(encoded)), CommitTime: int64(binary.LittleEndian.Uint64(encoded[13:])),
		SchemaID: int16(binary.LittleEndian.Uint16(encoded[schemaOffset:])), AppendOnly: encoded[schemaOffset+2]&1 != 0,
		WriterID: int64(binary.LittleEndian.Uint64(encoded[lastOffset+4:])), BatchSequence: int32(binary.LittleEndian.Uint32(encoded[lastOffset+12:])), Records: make([]Record, 0, count),
	}
	if magic == 2 {
		batch.LeaderEpoch = int32(binary.LittleEndian.Uint32(encoded[logBatchEpochOffset:]))
	}
	position := recordsOffset
	for index := 0; index < count; index++ {
		record, next, err := decodeLogRecord(schema, batch, encoded, position, index, compacted)
		if err != nil {
			return LogBatch{}, err
		}
		batch.Records = append(batch.Records, record)
		position = next
	}
	if position != len(encoded) {
		return LogBatch{}, fmt.Errorf("%w: trailing log bytes", ErrMalformedRecordBatch)
	}
	return batch, nil
}

type logBatchLayout struct {
	size         int
	schemaOffset int // The CRC is the four bytes immediately before the schema ID.
}

func logLayout(magic byte) (logBatchLayout, bool) {
	switch magic {
	case 0:
		return logBatchLayout{logBatchV0HeaderSize, 25}, true
	case 1:
		return logBatchLayout{logBatchV1HeaderSize, 25}, true
	case 2:
		return logBatchLayout{logBatchV2HeaderSize, 29}, true
	default:
		return logBatchLayout{}, false
	}
}

func writeLogBatchHeader(encoded []byte, layout logBatchLayout, b LogBatch, count int) {
	binary.LittleEndian.PutUint64(encoded, uint64(b.BaseOffset))
	encoded[12] = b.Magic
	binary.LittleEndian.PutUint64(encoded[13:], uint64(b.CommitTime))
	if b.Magic == 2 {
		binary.LittleEndian.PutUint32(encoded[logBatchEpochOffset:], uint32(b.LeaderEpoch))
	}
	tail := encoded[layout.schemaOffset:]
	binary.LittleEndian.PutUint16(tail, uint16(b.SchemaID))
	if b.AppendOnly {
		tail[2] = 1
	}
	if count > 0 {
		binary.LittleEndian.PutUint32(tail[3:], uint32(count-1))
	}
	binary.LittleEndian.PutUint64(tail[7:], uint64(b.WriterID))
	binary.LittleEndian.PutUint32(tail[15:], uint32(b.BatchSequence))
	binary.LittleEndian.PutUint32(tail[19:], uint32(count))
	// The zero-initialized V1/V2 statistics length means no statistics.
}

func finishLogBatch(encoded []byte, layout logBatchLayout) {
	binary.LittleEndian.PutUint32(encoded[8:], uint32(len(encoded)-12))
	binary.LittleEndian.PutUint32(encoded[layout.schemaOffset-4:], logBatchCRC(encoded[layout.schemaOffset:]))
}

func logBatchCRC(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}

func logBatchHeader(encoded []byte) (byte, int, int, error) {
	if len(encoded) < 13 || len(encoded) > maxRowBytes {
		return 0, 0, 0, fmt.Errorf("%w: invalid log batch length", ErrMalformedRecordBatch)
	}
	magic := encoded[12]
	layout, ok := logLayout(magic)
	if !ok {
		return 0, 0, 0, fmt.Errorf("%w: unsupported log magic %d", ErrMalformedRecordBatch, magic)
	}
	if len(encoded) < layout.size || binary.LittleEndian.Uint32(encoded[8:]) != uint32(len(encoded)-12) {
		return 0, 0, 0, fmt.Errorf("%w: invalid log batch size", ErrMalformedRecordBatch)
	}
	schemaOffset := layout.schemaOffset
	if binary.LittleEndian.Uint32(encoded[schemaOffset-4:]) != logBatchCRC(encoded[schemaOffset:]) {
		return 0, 0, 0, fmt.Errorf("%w: invalid log batch checksum", ErrMalformedRecordBatch)
	}
	recordsOffset := layout.size
	if magic != 0 {
		statsLength := binary.LittleEndian.Uint32(encoded[layout.size-4:])
		if statsLength > uint32(len(encoded)-recordsOffset) {
			return 0, 0, 0, fmt.Errorf("%w: invalid log statistics length", ErrMalformedRecordBatch)
		}
		recordsOffset += int(statsLength)
	}
	return magic, recordsOffset, schemaOffset, nil
}

func decodeLogRecord(
	schema Schema,
	batch LogBatch,
	encoded []byte,
	position, index int,
	compacted bool,
) (Record, int, error) {
	if len(encoded)-position < 5 {
		return Record{}, position, fmt.Errorf("%w: truncated log record", ErrMalformedRecordBatch)
	}
	length := int(binary.LittleEndian.Uint32(encoded[position:]))
	position += 4
	if length < 1 || length > len(encoded)-position {
		return Record{}, position, fmt.Errorf("%w: invalid log record length", ErrMalformedRecordBatch)
	}
	change := ChangeType(encoded[position])
	if err := change.Validate(); err != nil {
		return Record{}, position, fmt.Errorf("%w: invalid log change type", ErrMalformedRecordBatch)
	}
	bodyEnd := position + length
	var row Row
	var err error
	if compacted {
		row, err = DecodeCompactedRow(schema, encoded[position+1:bodyEnd])
	} else {
		row, err = DecodeIndexedRow(schema, encoded[position+1:bodyEnd])
	}
	record := Record{
		Value: row, Change: change, Offset: batch.BaseOffset + int64(index),
		Timestamp: time.UnixMilli(batch.CommitTime),
	}
	return record, bodyEnd, err
}
