# Lookup scheduling decision

Status: accepted for the Apache Fluss 0.9.1 client.

## Design

Each `LookupClient` owns one bounded scheduler for point and prefix requests.
Compatible keys share a batch only when they target the same physical table,
bucket, lookup mode, and insert-if-not-exists policy. The client configuration
bounds every scheduling dimension:

- `MaxQueuedKeys` counts queued, grouped, and in-flight keys;
- `BatchDelay` bounds how long a partial group waits;
- `MaxBatchKeys` bounds keys in one protocol request;
- `MaxConcurrent` fixes the worker count and active request limit;
- `Timeout` bounds each backend request;
- `Retry.MaxAttempts` and its context-aware backoff bound safe read retries.

Callers retain their own result ordering and context. Canceling one caller
stops only that caller's wait and does not cancel a shared request needed by
other callers. Tasks canceled before dispatch are omitted. `Close` rejects new
keys, cancels active requests, completes queued tasks, joins the fixed workers,
and can be called repeatedly.

Read-only point and prefix requests retry only typed retriable server failures
and connection failures. Insert-if-not-exists clients reject retry policies
with more than one attempt. Although insertion is atomic, an ambiguous retry
could repeat server-side auto-increment or accounting effects, so the client
does not infer mutation safety from the lookup response shape.

## Build versus buy

A small client-local scheduler is retained. Generic batching packages do not
model Fluss bucket compatibility, partial result association, independent
caller cancellation, mutation-aware retries, or deterministic close. Combining
a generic batcher, semaphore, and retry package would retain the same lifecycle
state while spreading it across dependencies. The implementation uses standard
channels, contexts, timers, and a fixed worker set.

The scheduler is intentionally per lookup client rather than connection-wide.
One client has one table, partition, schema resolver, and insertion policy, so
its compatibility key stays small and close ownership remains clear. A shared
connection-wide scheduler would need cross-client fairness and schema lifetime
rules without reducing protocol calls beyond compatible groups.

## Measurement

Reproduce the comparison with:

```sh
go test -run '^$' -bench BenchmarkLookupCrossCallBatching -benchmem ./pkg/fgo
```

On Linux/amd64 with Go 1.26.5 and an AMD Ryzen 7 7735HS, a short run against a
backend with 100 microseconds of request latency produced:

| Mode | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Immediate one-key requests | 99,462 | 5,416 | 45 |
| Up to 64 keys, 100 microsecond delay | 91,405 | 4,661 | 33 |

The bounded batch reduced allocation count by about 27% and improved throughput
under concurrent latency. The delay is configurable because an immediate local
backend can favor one-key dispatch. These values are comparative and vary by
machine and Go version.

Unit and race tests cover cross-call point and prefix batching, input
association, cancellation isolation, queue saturation, timeout, safe retries,
insert retry rejection, concurrency, and queued/in-flight close.
