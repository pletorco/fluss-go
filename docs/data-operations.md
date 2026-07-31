# Data operations and advanced options

This guide covers the Apache Fluss 0.9.1 data features that build on an opened
`fgo.Client` and authoritative `fgo.Table`. All resources return validation or
classified server errors; they do not silently repair invalid input.

## Dynamic partitions

Dynamic partition creation is opt-in at client construction. Concurrent writers
requesting the same missing partition share one creation attempt, and metadata
refresh is bounded by `MetadataAttempts`.

<!-- go-source: internal/docexamples/snippets_test.go dynamicPartitions -->
```go
client, err := fgo.Open(
	ctx,
	fgo.WithSeedBrokers("coordinator:9123"),
	fgo.WithDynamicPartitionCreation(fgo.DynamicPartitionCreationConfig{
		MetadataAttempts: 3,
		RetryBackoff:     25 * time.Millisecond,
	}),
)
if err != nil {
	return err
}

partition := fgo.PartitionSpec{"day": "2026-07-30", "region": "kr"}
writer, err := client.NewKVWriter(
	ctx,
	table,
	fgo.WithKVPartitionSpec(table.Schema, partition),
)
```

`Schema.PartitionName` always follows the schema's partition-key order.
`PartitionNames` validates, deduplicates, and sorts multiple specs. Automatic
creation applies only to log and KV writers configured with a partition; reads
never create server state.

## Writer concurrency and shutdown

Log and KV writers default to four active requests across distinct buckets.
Use `WithLogConcurrency` or `WithKVConcurrency` to choose a value from 1
through 64. Requests for one bucket remain strictly ordered, so concurrency
does not change per-bucket sequence or mutation order. A slow bucket does not
prevent another bucket in the same writer from making progress while a request
slot is available.

`WithLogBuffer` and `WithKVBuffer` bound every accepted record that has not
reached a terminal result, including queued, batched, and in-flight records.
`WithLogRequest` and `WithKVRequest` set one timeout for both the server
operation and its client-side network call. The timeout is therefore also the
upper bound for an accepted request when the backend stays responsive at the
socket level.

`Close` stops admission, flushes accepted work, and retains the terminal flush
result. Concurrent and repeated callers observe the same final error. A
caller's context only bounds how long that caller waits; it does not shorten
the configured timeout of work the writer already accepted. A caller whose
context expires may call `Close` again to observe the final result. Ambiguous
write failures are not retried and poison only the affected bucket. See
[error handling](error-handling.md) and
[write scheduling](write-scheduling.md) for recovery and ownership details.

## Log formats and bounded scans

Select an explicit format only when it agrees with the table's advertised
`table.log.format` property. Auto mode derives the compatible format.

<!-- go-source: internal/docexamples/snippets_test.go logFormat -->
```go
writer, err := client.NewLogWriter(
	ctx,
	table,
	fgo.WithLogWriteFormat(fgo.LogWriteFormatCompacted),
)
```

A log scanner can terminate after a total row limit or at exclusive offsets for
every initial bucket. Stopping-offset maps must contain exactly those buckets.

<!-- go-source: internal/docexamples/snippets_test.go boundedScan -->
```go
scanner, err := client.NewLogScanner(
	ctx,
	table,
	fgo.Earliest(),
	fgo.WithScanRowLimit(1_000),
	fgo.WithScanStoppingOffsets(map[int32]int64{
		0: 10_000,
		1: 12_000,
	}),
)
if err != nil {
	return err
}
defer scanner.Close()
```

`Wakeup` interrupts an active poll, or the next poll when none is active. The
poll returns `fgo.ErrWakeup`; the scanner remains usable. Row and Arrow results
preserve per-bucket failures rather than replacing a partial result with a
global success.

## Schema evolution on reads

Readers resolve the writer schema ID carried by historical values and batches,
then map rows to the `fgo.Table` schema supplied when the reader was created.
Point and prefix lookups, current-state batches, and row and Arrow log batches
share the same bounded client schema cache.

Stable field IDs preserve renamed columns. Dropped writer columns are ignored,
and a column added as nullable is returned as `nil` for older rows. Required
column additions, incompatible type changes, and null values mapped to a
required column return `fgo.ErrInvalidSchema`; the client does not invent
defaults or coerce values.

After altering a table, call `OpenTable` until it returns the new schema ID and
construct new writers and readers from that refreshed `Table`. Existing
readers intentionally retain the result shape they were created with. See the
[schema-evolution guide](schema-evolution.md) for the complete compatibility,
projection, cache, and error contract.

## KV merge and insert-if-missing

`MergeModeDefault` applies the table's configured merge engine.
`MergeModeOverwrite` bypasses it and writes the supplied values as replacements.
One writer uses one mode for every mutation it accepts.

<!-- go-source: internal/docexamples/snippets_test.go kvMerge -->
```go
writer, err := client.NewKVWriter(
	ctx,
	table,
	fgo.WithKVMergeMode(fgo.MergeModeOverwrite),
)
```

Lookup clients can request Fluss to atomically insert a missing primary-key row
before returning it. Fluss fills auto-increment columns and sets nullable
non-key columns to null.

<!-- go-source: internal/docexamples/snippets_test.go lookupInsert -->
```go
lookup, err := client.NewLookupClient(
	ctx,
	table,
	fgo.WithLookupInsertIfNotExists(5*time.Second, -1),
)
```

The option is rejected when the schema has required non-key values that Fluss
cannot synthesize.

## Current-state and snapshot scans

Resolve a stable, bucket-ID-ordered metadata snapshot before opening one scanner
per bucket:

<!-- go-source: internal/docexamples/snippets_test.go batchScan -->
```go
buckets, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{
	TablePath: table.Path,
})
if err != nil {
	return err
}

scanner, err := client.NewBatchScanner(
	ctx,
	table,
	buckets[0],
	fgo.WithBatchLimit(1_000),
	fgo.WithBatchProjection("id", "name"),
)
if err != nil {
	return err
}
defer scanner.Close()

result, err := scanner.Poll(ctx)
if err != nil {
	return err
}
defer result.Release()
```

Current-state scans use LIMIT_SCAN once. Snapshot scans use
`NewSnapshotBatchScanner` and require a `SnapshotBatchProvider` configured when
the client is opened. A snapshot provider may return its final rows and
`io.EOF` together; `Poll` then returns those rows with `Done` set. Process the
result before checking whether another poll is needed. A later poll returns an
empty completed result. `BatchResult.Release` releases owned Arrow batches.

## Metrics

Register one observer when opening the client:

<!-- go-source: internal/docexamples/snippets_test.go metricsObserver -->
```go
observer := fgo.MetricsObserverFunc(func(event fgo.MetricEvent) {
	log.Printf("kind=%d operation=%d duration=%s", event.Kind, event.Operation, event.Duration)
})

client, err := fgo.Open(
	ctx,
	fgo.WithSeedBrokers("coordinator:9123"),
	fgo.WithMetricsObserver(observer),
)
```

Events have bounded-cardinality fields and omit endpoints, table paths, bucket
IDs, payloads, credentials, token bytes, and server messages. An observer panic
is isolated from the client operation, but observers should still return
quickly and move expensive export work to their own bounded queue.

## Administrative server discovery

The administrative client reuses the same connections and returns the current
coordinator followed by deterministically sorted live tablet servers:

<!-- go-source: internal/docexamples/snippets_test.go serverDiscovery -->
```go
admin, err := fadm.New(client)
if err != nil {
	return err
}
nodes, err := admin.ServerNodes(ctx)
```

Close writers, scanners, and lookup clients before closing the shared
`fgo.Client`.
