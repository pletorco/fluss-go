package fgo

import (
	"cmp"
	"math"
	"slices"
	"sync"
)

type scanSegment struct {
	offset int64
	row    int
	arrow  int
}

type boundedScan struct {
	bucket    int32
	stop      int64
	remaining int64
	delivered int64
	next      int64
	truncated bool
	rows      []ScanRecord
	arrows    []ScanArrowBatch
}

func (s *LogScanner) applyBounds(
	bucket int32,
	rows []ScanRecord,
	arrows []ScanArrowBatch,
	decodedNext int64,
	delivered int64,
) ([]ScanRecord, []ScanArrowBatch, int64, int64) {
	if s.config.RowLimit == 0 && s.config.StoppingOffsets == nil {
		return rows, arrows, scanResultRows(rows, arrows), decodedNext
	}
	remaining, stop := s.scanBounds(bucket, delivered)
	segments := orderedScanSegments(rows, arrows)
	bounded := boundedScan{
		bucket: bucket, stop: stop, remaining: remaining,
		rows: make([]ScanRecord, 0, len(rows)), arrows: make([]ScanArrowBatch, 0, len(arrows)),
	}
	for _, segment := range segments {
		if segment.row >= 0 {
			bounded.appendRow(rows[segment.row])
			continue
		}
		bounded.appendArrow(segment, &arrows[segment.arrow])
	}
	// The fetch may have advanced past filtered batches after its last
	// record. Keep that progress unless the row limit withheld eligible data.
	next := min(decodedNext, stop)
	if bounded.truncated {
		next = bounded.next
	}
	return bounded.rows, bounded.arrows, bounded.delivered, next
}

func scanResultRows(rows []ScanRecord, arrows []ScanArrowBatch) int64 {
	delivered := int64(len(rows))
	for index := range arrows {
		delivered += arrows[index].Batch.Record.NumRows()
	}
	return delivered
}

func (s *LogScanner) scanBounds(bucket int32, delivered int64) (int64, int64) {
	remaining := int64(math.MaxInt64)
	if s.config.RowLimit > 0 {
		remaining = s.config.RowLimit - delivered
	}
	stop := int64(math.MaxInt64)
	if configured, ok := s.config.StoppingOffsets[bucket]; ok {
		stop = configured
	}
	return remaining, stop
}

func orderedScanSegments(rows []ScanRecord, arrows []ScanArrowBatch) []scanSegment {
	segments := make([]scanSegment, 0, len(rows)+len(arrows))
	for index := range rows {
		segments = append(segments, scanSegment{offset: rows[index].Record.Offset, row: index, arrow: -1})
	}
	for index := range arrows {
		segments = append(segments, scanSegment{offset: arrows[index].Batch.BaseOffset, row: -1, arrow: index})
	}
	slices.SortStableFunc(segments, func(a, b scanSegment) int { return cmp.Compare(a.offset, b.offset) })
	return segments
}

func (b *boundedScan) appendRow(row ScanRecord) {
	if row.Record.Offset >= b.stop {
		return
	}
	if b.remaining <= 0 {
		b.truncated = true
		return
	}
	b.rows = append(b.rows, row)
	b.remaining--
	b.delivered++
	b.next = row.Record.Offset + 1
}

func (b *boundedScan) appendArrow(segment scanSegment, item *ScanArrowBatch) {
	batch := &item.Batch
	count := batch.Record.NumRows()
	eligible := min(count, b.stop-segment.offset)
	if eligible > b.remaining {
		b.truncated = true
	}
	allowed := min(eligible, b.remaining)
	if allowed <= 0 {
		batch.Release()
		return
	}
	if allowed == count {
		b.arrows = append(b.arrows, *item)
	} else {
		b.arrows = append(b.arrows, ScanArrowBatch{
			Bucket: b.bucket,
			Batch:  sliceArrowLogBatch(batch, 0, allowed),
		})
		batch.Release()
	}
	b.remaining -= allowed
	b.delivered += allowed
	b.next = segment.offset + allowed
}

func sliceArrowLogBatch(batch *ArrowLogBatch, start, end int64) ArrowLogBatch {
	sliced := *batch
	sliced.BaseOffset += start
	sliced.Record = batch.Record.NewSlice(start, end)
	sliced.Changes = append([]ChangeType(nil), batch.Changes[start:end]...)
	sliced.owned = true
	sliced.release = &sync.Once{}
	return sliced
}
