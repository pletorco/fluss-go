# Write scheduling decision

Status: accepted for the Apache Fluss 0.9.1 client.

## Current ownership

The client shares one negotiated connection per server identity through
`connectionManager`. Log and KV writers own their scheduling state:

- each writer has one bounded FIFO command queue with `MaxBuffered` capacity;
- each writer has one scheduler and at most `MaxConcurrentRequests` active
  server calls for distinct buckets;
- pending batches are bounded per bucket by `MaxBatchRecords` and
  `MaxBatchBytes`;
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
- **Failure:** Fluss 0.9.1 writes are not automatically retried after an
  ambiguous response. Bucket poisoning remains writer-local, so one writer's
  uncertain state is not inherited by another.
- **Shutdown:** `Close` drains commands accepted by that writer and stops its
  worker. Writers should be closed before the parent client. Closing one writer
  neither flushes nor closes another writer. The context passed to `Close`
  bounds how long that caller waits; it does not shorten the timeout of an
  already accepted batch. `LogWriterConfig.Timeout` and
  `KVWriterConfig.Timeout` independently bound both the server operation and
  the client-side network call, so a responsive backend cannot keep the worker
  alive indefinitely. Once shutdown finishes, concurrent and repeated `Close`
  calls return the same terminal flush result. A caller that stopped waiting
  because its own context expired may call `Close` again to observe that result.

The scheduling tests cover saturation, ambiguous failures, slow-bucket
isolation, per-bucket sequence order, and shutdown for both log and KV writers.
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

## Reproduce measurements

```sh
go test -run '^$' -bench BenchmarkWriterScheduling -benchmem ./pkg/fgo
```

The benchmark covers 16 writers over 8 buckets with an immediate server, the
same topology with a 100 microsecond server delay, and repeated writer
lifecycles against a failing server. Reference measurements and environment are
recorded in GitHub issue 43 because benchmark numbers vary by machine and Go
version.
