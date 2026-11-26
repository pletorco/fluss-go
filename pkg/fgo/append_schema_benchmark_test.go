package fgo

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

func appendBenchmarkInput(shape string) (Table, Row) {
	table := appendWriterTable()
	row := Row{int32(1), "value"}
	switch shape {
	case "wide32":
		for index := 2; index < 32; index++ {
			table.Schema.Columns = append(table.Schema.Columns, Column{Name: fmt.Sprintf("value%d", index), Type: BigIntType})
			row = append(row, int64(index))
		}
	case "bytes4k":
		table.Schema.Columns[1].Type = BytesType
		row[1] = bytes.Repeat([]byte{'x'}, 4096)
	case "nested":
		table.Schema, row, _ = mutableAppendInput()
	}
	table.Schema.BucketKey = []string{"id"}
	return table, row
}

func BenchmarkAppendWriterSchema(b *testing.B) {
	for _, format := range []LogFormat{LogFormatCompacted, LogFormatIndexed} {
		for _, shape := range []string{"small", "wide32", "bytes4k", "nested"} {
			for _, records := range []int{1, 16, 64} {
				b.Run(fmt.Sprintf("%s/%s/%d", format, shape, records), func(b *testing.B) {
					table, row := appendBenchmarkInput(shape)
					table.Properties["table.log.format"] = string(format)
					writer, err := newAppendWriter(context.Background(), newSchedulingLogBackend(1), table,
						WithAppendLogFormat(format), WithAppendBatchLimits(1<<20, records),
						WithAppendBuffer(records), WithAppendBatchTimeout(time.Hour))
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { _ = writer.Close(context.Background()) })
					b.ReportAllocs()
					b.ResetTimer()
					for written := 0; written < b.N; {
						count := min(records, b.N-written)
						futures := make([]*WriteFuture, count)
						for index := range count {
							futures[index] = writer.Append(context.Background(), row)
						}
						if err := writer.Flush(context.Background()); err != nil {
							b.Fatal(err)
						}
						for _, future := range futures {
							if result := future.Await(context.Background()); result.Err != nil {
								b.Fatal(result.Err)
							}
						}
						written += count
					}
				})
			}
		}
	}
}

// Run with -benchtime=1x. Heap samples are process-wide diagnostics, not
// deterministic assertions or a replacement for queue/ownership regression tests.
func BenchmarkAppendWriterRetention(b *testing.B) {
	for _, format := range []LogFormat{LogFormatCompacted, LogFormatIndexed} {
		for _, shape := range []string{"wide32", "bytes4k", "nested"} {
			b.Run(string(format)+"/"+shape, func(b *testing.B) {
				for range b.N {
					benchmarkAppendRetention(b, format, shape)
				}
			})
		}
	}
}

func benchmarkAppendRetention(b *testing.B, format LogFormat, shape string) {
	table, row := appendBenchmarkInput(shape)
	table.Properties["table.log.format"] = string(format)
	runtime.GC()
	var baseline, sample runtime.MemStats
	runtime.ReadMemStats(&baseline)
	backend := &barrierBackend{requests: make(chan barrierRequest, 1), stop: make(chan struct{})}
	writer, err := newAppendWriter(context.Background(), barrierAppendBackend{logBackend(0), backend}, table,
		WithAppendLogFormat(format), WithAppendBatchLimits(1<<20, 64),
		WithAppendBuffer(64), WithAppendBatchTimeout(time.Hour))
	if err != nil {
		b.Fatal(err)
	}
	defer writer.Close(context.Background())
	peak, pending, drainedFirst, drainedLast := baseline.HeapAlloc, uint64(0), uint64(0), uint64(0)
	for round := range 32 {
		futures := make([]*WriteFuture, 64)
		for index := range futures {
			futures[index] = writer.Append(context.Background(), row)
		}
		request := <-backend.requests
		runtime.ReadMemStats(&sample)
		peak = max(peak, sample.HeapAlloc)
		if round == 0 {
			runtime.GC()
			runtime.ReadMemStats(&sample)
			pending = sample.HeapAlloc
		}
		request.result <- nil
		if err := writer.Flush(context.Background()); err != nil {
			b.Fatal(err)
		}
		for _, future := range futures {
			if result := future.Await(context.Background()); result.Err != nil {
				b.Fatal(result.Err)
			}
		}
		if round == 0 || round == 31 {
			runtime.GC()
			runtime.ReadMemStats(&sample)
			if round == 0 {
				drainedFirst = sample.HeapAlloc
			}
			drainedLast = sample.HeapAlloc
		}
	}
	runtime.KeepAlive(writer)
	runtime.KeepAlive(table) // Include the snapshot cost while the caller retains metadata.
	delta := func(value, base uint64) float64 {
		if value < base {
			return 0
		}
		return float64(value - base)
	}
	b.ReportMetric(delta(pending, baseline.HeapAlloc), "pending-heap-B")
	b.ReportMetric(delta(drainedFirst, baseline.HeapAlloc), "drained-1-heap-B")
	b.ReportMetric(delta(drainedLast, baseline.HeapAlloc), "drained-32-heap-B")
	b.ReportMetric(delta(peak, baseline.HeapAlloc), "sampled-peak-B")
}
