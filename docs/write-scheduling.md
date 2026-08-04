# Write scheduling decision

Status: accepted for the Apache Fluss 0.9.1 client.

## Current ownership

The client shares one negotiated connection per server identity through
`connectionManager`. Log and upsert writers own their scheduling state:

- each writer has one bounded FIFO command queue with `MaxBuffered` capacity;
- each writer has one scheduler and at most `MaxConcurrentRequests` active
  server calls for distinct buckets;
- pending batches fill dynamically until the batch timeout expires or the fixed
  `MaxBatchRecords` or `MaxBatchBytes` cap is reached;
- `Flush` and `Close` are barriers in that writer's queue;
- an ambiguous write failure poisons only that writer's affected bucket.

There is no hidden connection-wide record queue or memory pool. Connection-wide
buffer use is therefore the sum of explicitly configured writer queues plus
each writer's active per-bucket batches. Applications with many writers must
budget that sum rather than treating `MaxBuffered` as a client-global limit.

## Invariants

- **Memory:** an admission semaphore counts queued, batched, and in-flight
  records, so one writer has at most `MaxBuffered` accepted records awaiting
  completion. One writer cannot consume another writer's queue budget. Total
  records still grow additively across writers.
- **Queue:** accepted commands retain FIFO order within a writer. No ordering is
  promised across writers. Only one request per bucket is active, preserving
  batch sequence and write order within that bucket.
- **Fairness:** writers independently submit requests to the shared transport.
  A blocked bucket cannot prevent another bucket in the same writer from making
  progress while concurrency is available, and a blocked or poisoned writer
  cannot prevent another writer from enqueueing, flushing, or closing. The
  remote server and TCP connection may still affect request latency; strict
  cross-writer fairness is not promised.
- **Failure:** explicitly enabled idempotent retries preserve the writer ID,
  bucket sequence, and encoded bytes. An unrecoverable ambiguous response
  poisons only that bucket, so one writer's uncertain state is not inherited
  by another.
- **Shutdown:** `Close` drains commands accepted by that writer and stops its
  worker. Writers should be closed before the parent client. Closing one writer
  neither flushes nor closes another writer. The context passed to `Close`
  bounds how long that caller waits; it does not shorten the timeout of an
  already accepted batch. `AppendWriterConfig.RequestTimeout` and
  `UpsertWriterConfig.RequestTimeout` independently bound both the server operation and
  the client-side network call, so a responsive backend cannot keep the worker
  alive indefinitely. Once shutdown finishes, concurrent and repeated `Close`
  calls return the same terminal flush result. A caller that stopped waiting
  because its own context expired may call `Close` again to observe that result.

The scheduling tests cover saturation, ambiguous failures, slow-bucket
isolation, per-bucket sequence order, and shutdown for both log and upsert writers.
The package race test covers the same paths under the race detector.

## Decision

A connection-owned shared accumulator/sender is not introduced. The current
design already shares network connections, while keeping queue capacity,
failure state, flush, and close ownership local to each writer. Centralizing
those queues would save one scheduler and timer per writer, but would require a
global admission policy and fair scheduling and would add a head-of-line
blocking risk to independent writers.

The decision can be revisited if production profiles show writer goroutines or
additive queue memory are a material cost. Any replacement must preserve the
invariants above and add an explicit client-wide byte budget rather than an
unbounded shared queue.

Configurable same-bucket in-flight requests are also not introduced. The
transport can multiplex requests, and the saturation benchmark shows the
throughput available when eight independent buckets can make progress. Doing
the same for one bucket would require allocating sequences before completion,
committing responses in sequence order, retaining later successful responses
behind a failed earlier request, and implementing the Fluss 0.9.1 writer-ID
reset rules. The synthetic upper bound does not justify that state machine for
the initial Go client. One active request per bucket remains the explicit
ordering and idempotence boundary.

Adaptive batch limits are not introduced. The existing writer already fills
each batch according to arrival rate and batch timeout while fixed record and byte
caps provide predictable request sizes. Measurements show that configuring
those caps for the workload captures the material allocation and throughput
benefit without an adaptive controller.

A page allocator or additional byte admission pool is rejected for now. Rows
are bounded to 16 MiB, accepted records are bounded by `MaxBuffered`, and
encoded batches are bounded by `MaxBatchBytes`, so memory has finite limits.
Charging retained Go values accurately before encoding would require copying
caller-owned strings, byte slices, and Arrow buffers at admission, changing
ownership and allocation behavior. Applications should set `MaxBuffered` and
`MaxBatchBytes` from their maximum row size. A future byte pool must account
for retained and encoded storage separately and demonstrate a lower peak heap
on a representative workload before replacing these bounds.

## Reproduce measurements

```sh
go test -run '^$' -bench 'BenchmarkWriter(Scheduling|Batching)$' -benchmem ./pkg/fgo
```

The benchmark covers single-bucket saturation, independent-bucket concurrency,
16 writers, slow and failing servers, 64-writer lifecycle cost, and fixed batch
sizes. A short comparison run on Linux/amd64, Go 1.26.5, AMD Ryzen 7 7735HS
with `-benchtime=200ms` produced:

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| One writer, one slow bucket | 939,303 | 3,964 | 34 |
| One writer, eight slow buckets | 213,772 | 3,934 | 36 |
| 16 writers, eight buckets | 1,870 | 3,553 | 33 |
| 16 writers, slow server | 51,809 | 3,918 | 36 |
| 64-writer lifecycle | 300,382 | 325,472 | 1,680 |
| One-record batches | 7,376 | 3,679 | 34 |
| 16-record batches | 3,202 | 3,196 | 23 |
| 64-record batches | 2,626 | 3,174 | 22 |

The eight-bucket case establishes the concurrency ceiling but does not model
same-bucket response reordering. Moving from one to 64 records per batch cut
time per record by about 64% and allocations by about 35%, supporting
configurable fixed caps. Benchmark values are comparative rather than release
guarantees and vary by machine and Go version.
