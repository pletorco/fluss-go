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
)

var ErrMalformedRecordBatch = errors.New("fgo: malformed record batch")

// KVRecord is one raw primary-key record. A nil Value represents a delete. Buffers returned by a
// decoder are owned by the caller.
type KVRecord struct {
	Key   []byte
	Value []byte
}

// KVBatch carries the writer state required for idempotent KV writes.
type KVBatch struct {
	SchemaID      int16
	WriterID      int64
	BatchSequence int32
	Records       []KVRecord
}

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

// LogBatch encodes compacted or indexed row records. Magic 0 and 1 use the Fluss 0.9.1 layouts.
type LogBatch struct {
	Magic         byte
	BaseOffset    int64
	CommitTime    int64
	LeaderEpoch   int32
	SchemaID      int16
	AppendOnly    bool
	WriterID      int64
	BatchSequence int32
	Records       []Record
}

func (b LogBatch) EncodeRows(schema Schema, compacted bool) ([]byte, error) {
	if b.Magic != 0 && b.Magic != 1 {
		return nil, fmt.Errorf("%w: unsupported log magic %d", ErrMalformedRecordBatch, b.Magic)
	}
	headerSize := logBatchV0HeaderSize
	if b.Magic == 1 {
		headerSize = logBatchV1HeaderSize
	}
	encoded := make([]byte, headerSize)
	binary.LittleEndian.PutUint64(encoded, uint64(b.BaseOffset))
	encoded[12] = b.Magic
	binary.LittleEndian.PutUint64(encoded[13:], uint64(b.CommitTime))
	crcOffset, schemaOffset := 21, 25
	if b.Magic == 1 {
		binary.LittleEndian.PutUint32(encoded[21:], uint32(b.LeaderEpoch))
		crcOffset, schemaOffset = 25, 29
	}
	binary.LittleEndian.PutUint16(encoded[schemaOffset:], uint16(b.SchemaID))
	if b.AppendOnly {
		encoded[schemaOffset+2] = 1
	}
	lastOffset := schemaOffset + 3
	if len(b.Records) > 0 {
		binary.LittleEndian.PutUint32(encoded[lastOffset:], uint32(len(b.Records)-1))
	}
	binary.LittleEndian.PutUint64(encoded[lastOffset+4:], uint64(b.WriterID))
	binary.LittleEndian.PutUint32(encoded[lastOffset+12:], uint32(b.BatchSequence))
	binary.LittleEndian.PutUint32(encoded[lastOffset+16:], uint32(len(b.Records)))
	for _, record := range b.Records {
		if err := record.Change.Validate(); err != nil {
			return nil, err
		}
		var row []byte
		var err error
		if compacted {
			row, err = EncodeCompactedRow(schema, record.Value)
		} else {
			row, err = EncodeIndexedRow(schema, record.Value)
		}
		if err != nil {
			return nil, err
		}
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(1+len(row)))
		encoded = append(encoded, length[:]...)
		encoded = append(encoded, byte(record.Change))
		encoded = append(encoded, row...)
	}
	binary.LittleEndian.PutUint32(encoded[8:], uint32(len(encoded)-12))
	binary.LittleEndian.PutUint32(encoded[crcOffset:], crc32.Checksum(encoded[schemaOffset:], crc32.MakeTable(crc32.Castagnoli)))
	return encoded, nil
}

// DecodeLogBatchRows decodes a single Fluss v0 or v1 row-oriented log batch. It validates the
// CRC before allocating records and returns the schema ID advertised by the batch.
func DecodeLogBatchRows(schema Schema, encoded []byte, compacted bool) (LogBatch, error) {
	magic, headerSize, schemaOffset, err := logBatchHeader(encoded)
	if err != nil {
		return LogBatch{}, err
	}
	lastOffset := schemaOffset + 3
	count := int(binary.LittleEndian.Uint32(encoded[lastOffset+16:]))
	if count > (len(encoded)-headerSize)/5 {
		return LogBatch{}, fmt.Errorf("%w: invalid log record count", ErrMalformedRecordBatch)
	}
	batch := LogBatch{
		Magic: magic, BaseOffset: int64(binary.LittleEndian.Uint64(encoded)), CommitTime: int64(binary.LittleEndian.Uint64(encoded[13:])),
		SchemaID: int16(binary.LittleEndian.Uint16(encoded[schemaOffset:])), AppendOnly: encoded[schemaOffset+2]&1 != 0,
		WriterID: int64(binary.LittleEndian.Uint64(encoded[lastOffset+4:])), BatchSequence: int32(binary.LittleEndian.Uint32(encoded[lastOffset+12:])), Records: make([]Record, 0, count),
	}
	if magic == 1 {
		batch.LeaderEpoch = int32(binary.LittleEndian.Uint32(encoded[21:]))
	}
	position := headerSize
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

func logBatchHeader(encoded []byte) (byte, int, int, error) {
	if len(encoded) < 13 || len(encoded) > maxRowBytes {
		return 0, 0, 0, fmt.Errorf("%w: invalid log batch length", ErrMalformedRecordBatch)
	}
	magic := encoded[12]
	headerSize, crcOffset, schemaOffset := logBatchV0HeaderSize, 21, 25
	if magic == 1 {
		headerSize, crcOffset, schemaOffset = logBatchV1HeaderSize, 25, 29
	} else if magic != 0 {
		return 0, 0, 0, fmt.Errorf("%w: unsupported log magic %d", ErrMalformedRecordBatch, magic)
	}
	if len(encoded) < headerSize || int(binary.LittleEndian.Uint32(encoded[8:])) != len(encoded)-12 {
		return 0, 0, 0, fmt.Errorf("%w: invalid log batch size", ErrMalformedRecordBatch)
	}
	checksum := crc32.Checksum(encoded[schemaOffset:], crc32.MakeTable(crc32.Castagnoli))
	if binary.LittleEndian.Uint32(encoded[crcOffset:]) != checksum {
		return 0, 0, 0, fmt.Errorf("%w: invalid log batch checksum", ErrMalformedRecordBatch)
	}
	return magic, headerSize, schemaOffset, nil
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
