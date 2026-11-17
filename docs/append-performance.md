# Append schema preparation

Status: accepted for Apache Fluss 1.0 after the input-ownership and lifecycle fixes.

## Decision and validation boundaries

`AppendWriter` validates its schema at construction, takes an owned snapshot
of columns, nested logical types, key lists, and properties, and computes bucket-key
column positions once. Each admission still validates row length, nullability,
scalar types, and recursively nested values. The accepted row is encoded once
in its configured compacted/indexed format, and routing and wire batching keep
the accepted row/key bytes from the input-ownership fix.

The full-row validator iterates ordered columns directly rather than allocating
column-name lists and maps. Public codecs and `Schema.ValidateRow` still validate
the caller's schema on every invocation. Nested codecs keep their existing
validation; no recursively compiled serializer is introduced. Decimal precision,
scale, fixed-width bounds, row/key size limits, and framing still use the same
codec implementations and errors. Arrow retention, retries, sequences, queue
limits, barriers, and cancellation are unchanged.

Build-vs-buy classification: mandatory reuse. The existing row/key codecs and
standard `slices.Clone`/`maps.Clone` are reused; the small logical-type tree walk
only detaches the writer's metadata. A generic deep-copy library would add a
dependency or reflection without removing schema-specific ownership rules.
JSON round-tripping would add serialization work and alter optional schema
metadata. Global schema caches are rejected because public schemas are mutable.
An allocator or a second serializer is unnecessary for this measured bottleneck.
Maintenance remains with the existing `fgo` writer/codec owners, with no new
dependency, license, protocol, or platform support surface.

## Measurement setup

Baseline: `origin/main` commit `8b5d5cf40b266ecf54a284906e9dd4d800fe752b`,
which already includes encoded-input ownership (#211), retained batch errors
(#212), and cancelable frame admission (#213). These measurements do not claim
credit for #211's removal of asynchronous re-encoding.

Environment: Linux/amd64, Go 1.26.11, AMD Ryzen 7 7735HS, 16 logical CPUs,
2026-11-17. Identical benchmark sources were compiled before and after the
production change, then their binaries were run sequentially on the same machine.
No race instrumentation, live cluster, or other verification jobs ran during
the final measured sequence. Numbers are medians of three 200 ms samples,
including append, batch flushing, and future completion. They describe a
synthetic non-retaining mock backend, not production throughput guarantees.

`BenchmarkWriterBatching` retains its original two-column, unkeyed workload.
`BenchmarkAppendWriterSchema` uses an integer bucket key and four shapes:

- `small`: integer and short string;
- `wide32`: 32 columns (integer, string, and 30 int64 values);
- `bytes4k`: integer plus mutable 4 KiB payload;
- `nested`: bytes, mutable decimal, arrays of rows, ordered and Go maps, and a row.

The timed nested input is stable while being encoded; separate race regressions
mutate caller values after `Append` and metadata after writer construction.

## Reproduction

Keep the benchmark files identical on both revisions and compile the two test
binaries before running measurements. Run them sequentially, without CI or
integration jobs competing for CPU or heap:

```sh
go test -run '^$' -bench '^(BenchmarkWriterBatching|BenchmarkAppendWriterSchema)$' \
  -benchmem -benchtime=200ms -count=3 ./pkg/fgo
go test -run '^$' -bench '^BenchmarkAppendWriterRetention$' \
  -benchtime=1x -count=3 ./pkg/fgo
go test -run '^$' -bench '^BenchmarkWriterBatching$' -benchtime=1s \
  -cpuprofile=/tmp/append.cpu -memprofile=/tmp/append.mem \
  -o=/tmp/append.test ./pkg/fgo
go tool pprof -top -cum /tmp/append.test /tmp/append.cpu
go tool pprof -top -alloc_space /tmp/append.test /tmp/append.mem
```

For process peak RSS, compile once and run the test binary under `/usr/bin/time -v`
with `-test.run '^$' -test.bench '^BenchmarkAppendWriterRetention$'
-test.benchtime=1x -test.count=3`; measuring `go test` instead would include
compiler/tool-driver memory. Heap diagnostics use `runtime.ReadMemStats` and
forced GC, so they are not timed-path performance measurements.
On the measurement host `/usr/bin/time` was unavailable; a Python standard-library
`subprocess.run` wrapper read the direct child's `resource.getrusage(RUSAGE_CHILDREN).ru_maxrss`
instead (Linux reports KiB). The benchmark itself remains Go-only.

## Timing and allocation results

All numbers below are per record. Cells are **before → after** (median).
The matrix has 24 keyed cases plus the three original unkeyed batch cases.

| Format / shape | Batch records | ns/record | B/record | allocs/record |
| --- | ---: | ---: | ---: | ---: |
| compacted/small | 1 | 6398 → 4724 | 3552 → 1088 | 28 → 21 |
| compacted/small | 16 | 2761 → 1055 | 3011 → 547 | 17 → 10 |
| compacted/small | 64 | 2587 → 916.7 | 2983 → 519 | 16 → 9 |
| compacted/wide32 | 1 | 25297 → 6513 | 47744 → 1440 | 84 → 54 |
| compacted/wide32 | 16 | 21161 → 2817 | 47320 → 1015 | 73 → 43 |
| compacted/wide32 | 64 | 20177 → 2457 | 47310 → 1005 | 72 → 42 |
| compacted/bytes4k | 1 | 10496 → 8187 | 18144 → 15680 | 31 → 24 |
| compacted/bytes4k | 16 | 8083 → 5927 | 32030 → 29566 | 19 → 12 |
| compacted/bytes4k | 64 | 8128 → 7665 | 32993 → 30528 | 18 → 11 |
| compacted/nested | 1 | 13444 → 13092 | 7206 → 4468 | 87 → 76 |
| compacted/nested | 16 | 8749 → 7293 | 6871 → 4132 | 75 → 64 |
| compacted/nested | 64 | 8000 → 5576 | 6921 → 4182 | 74 → 63 |
| indexed/small | 1 | 6447 → 5861 | 3560 → 1096 | 28 → 21 |
| indexed/small | 16 | 2854 → 1405 | 3019 → 555 | 17 → 10 |
| indexed/small | 64 | 2712 → 1271 | 3019 → 555 | 16 → 9 |
| indexed/wide32 | 1 | 27020 → 12636 | 48952 → 2648 | 87 → 57 |
| indexed/wide32 | 16 | 22270 → 5998 | 49204 → 2899 | 75 → 45 |
| indexed/wide32 | 64 | 22348 → 7052 | 49357 → 3052 | 74 → 44 |
| indexed/bytes4k | 1 | 10520 → 12267 | 17376 → 14912 | 30 → 23 |
| indexed/bytes4k | 16 | 8600 → 9180 | 31262 → 28798 | 18 → 11 |
| indexed/bytes4k | 64 | 8973 → 8406 | 32224 → 29760 | 17 → 10 |
| indexed/nested | 1 | 13255 → 10715 | 7359 → 4620 | 83 → 72 |
| indexed/nested | 16 | 8889 → 6181 | 7257 → 4518 | 71 → 60 |
| indexed/nested | 64 | 8613 → 4989 | 7236 → 4497 | 70 → 59 |
| unkeyed | 1 | 5833 → 4861 | 2703 → 1071 | 25 → 20 |
| unkeyed | 16 | 2114 → 1044 | 2163 → 531 | 15 → 10 |
| unkeyed | 64 | 1886 → 838.5 | 2135 → 503 | 14 → 9 |

The original unkeyed 64-record case reduced median time from 1,886 to
838.5 ns/record (about 56%), allocated bytes from 2,135 to 503 (about 76%),
and allocations from 14 to 9. Wide keyed rows benefit more because repeated
whole-schema maps dominated their cost. Payload copying and batch buffer growth
still dominate the 4 KiB cases; schema preparation does not eliminate those costs.

The initial indexed 4 KiB measurements at batch sizes 1 and 16 were slower despite
lower allocation. Do not generalize the small-row improvement to payload-heavy
traffic; the targeted longer follow-up below records that uncertainty explicitly.

## CPU and allocation attribution

Separate one-second-per-case profiles of the original `BenchmarkWriterBatching`
workload attributed 70.60% of sampled allocated bytes to `Schema.validateColumns`
before this change. It no longer appeared in the after profile. `Schema.ValidateRow`
accounted for 24.04% cumulative sampled CPU before; it was absent from the after
CPU profile. The new value-only `validateRowValues` accounted for 1.41% cumulative
sampled CPU in that after profile and remains on the admission path;
no row validation is skipped. Afterward, futures, accepted-write objects,
row/batch buffers, scheduling, and request contexts account for most remaining work.

These profile percentages describe aggregate time-bounded workloads with different
operation counts, not equal-work totals. Sampling is approximate; overlapping
cumulative stacks must not be added together. Nested codec validation still exists
and was not exercised by this original two-column profile.

## Longer payload follow-up

The indexed 4 KiB cases were rerun sequentially with five 500 ms samples per
revision. Medians improved, but the broad timing ranges (especially before)
confirm that short payload-heavy timings are sensitive to GC and scheduling.
The initial matrix is retained above rather than replaced by the better run.
Allocation reductions were consistent; no universal latency or peak-memory
improvement is claimed for large payloads.

| Batch records | Before median (range), ns/record | After median (range), ns/record | B/record before → after | allocs/record before → after |
| --- | ---: | ---: | ---: | ---: |
| 1 | 10933 (10553–17731) | 8805 (8744–8942) | 17376 → 14912 | 30 → 23 |
| 16 | 12046 (8558–14509) | 6446 (6227–6806) | 31262 → 28799 | 18 → 11 |
| 64 | 8470 (8254–8662) | 7755 (7670–8308) | 32226 → 29761 | 17 → 10 |

## Retention and peak memory

The retention diagnostic keeps the caller's original table alive, fills the
64-record admission limit, holds its backend request, samples heap use, releases
it, and repeats 32 batches. Forced-GC samples after batches 1 and 32 distinguish
retained objects from temporary allocation. Baseline production files were
restored through a Go build overlay for this diagnostic so the final benchmark
harness (including metadata liveness) was identical on both revisions.

The table reports median heap deltas from the process's GC baseline in KiB,
**before → after**. Pending samples are taken while 64 records are accepted;
drained samples retain the live writer and the caller's table. Sampled peak is
the highest `HeapAlloc` observed at batch boundaries, not a continuous heap peak.

| Format / shape | Pending | Drained after 1 batch | Drained after 32 batches | Sampled peak |
| --- | ---: | ---: | ---: | ---: |
| compacted/wide32 | 38.0 → 41.7 | 33.9 → 37.6 | 41.4 → 39.4 | 3001.2 → 1980.3 |
| compacted/bytes4k | 632.3 → 632.2 | 336.4 → 336.2 | 338.4 → 340.2 | 2928.0 → 2773.6 |
| compacted/nested | 53.4 → 55.3 | 41.4 → 43.5 | 46.5 → 46.4 | 2927.2 → 2772.5 |
| indexed/wide32 | 82.8 → 85.6 | 61.5 → 63.9 | 62.5 → 66.3 | 2934.8 → 2961.4 |
| indexed/bytes4k | 632.3 → 633.1 | 335.9 → 336.7 | 339.3 → 341.1 | 2879.3 → 2729.8 |
| indexed/nested | 60.2 → 62.1 | 45.3 → 47.5 | 48.5 → 51.5 | 2891.5 → 2777.3 |

The process peak RSS for all six retention cases repeated three times was
20,492 KiB before and 19,816 KiB after. This is one sequential process run per
revision, not a statistically significant RSS reduction claim. Heap/RSS include
runtime caches, GC scheduling, goroutine stacks, and benchmark scaffolding.
Some retained samples and the indexed wide-row sampled peak increased slightly;
copying metadata is an intentional bounded per-writer cost, not free memory.

Most importantly, history is not accumulated per append: one owned schema tree
and one integer position per bucket-key column survive with the writer. Accepted
row/key bytes, batch buffers, and Arrow ownership remain bounded by the existing
row-size and writer queue/batch limits. Drained heap stayed in the same range
between batches 1 and 32 (2,048 records); it did not grow with record history.
The mock backend does not retain sent frames. This is evidence for the scoped
workload, not proof about arbitrary schemas, payloads, concurrent caller counts,
remote server behavior, or production peak memory. No global cache or byte pool
is added, and the existing rule to size buffers from maximum row size still applies.
