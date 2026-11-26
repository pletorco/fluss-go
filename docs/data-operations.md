# Data operations and advanced options

This guide covers the Apache Fluss 1.0 data features that build on an opened
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
	fgo.WithBootstrapServers("coordinator:9123"),
	fgo.WithDynamicPartitionCreation(fgo.DynamicPartitionCreationConfig{
		MetadataAttempts: 3,
		RetryBackoff:     25 * time.Millisecond,
	}),
)
if err != nil {
	return err
}

partition := fgo.PartitionSpec{"day": "2026-07-30", "region": "kr"}
writer, err := client.NewUpsertWriter(
	ctx,
	table,
	fgo.WithUpsertPartitionSpec(table.Schema, partition),
)
```

`Schema.PartitionName` always follows the schema's partition-key order.
`PartitionNames` validates, deduplicates, and sorts multiple specs. Automatic
creation applies only to log and upsert writers configured with a partition; reads
never create server state.

Resolved partition buckets carry the parent table ID from the same metadata
response. If a table is dropped and recreated at the same path, use a fresh
`GetTable` result to open a writer, log scanner, or lookuper. Using the old
table with the recreated partition fails with `ErrMetadata`.

Append routing uses the selected partition's own bucket count. Changing the
table's default count affects only future partitions; existing partitions do
not need to match the default to open an append writer. If a leader is missing,
writer setup returns `ErrNoBucketLeader` rather than routing to a subset.

Primary-key lookups and upserts hash keys using the routing bucket count of
the selected table or partition. A bucket without a leader is never replaced
with a different bucket. If the bucket layout changes after a lookuper or
upsert writer opens, refresh the table metadata and reopen that resource when
Fluss rejects requests for the previous layout.

## Writer concurrency and shutdown

Log and upsert writers default to four active requests across distinct buckets.
Use `WithAppendConcurrency` or `WithUpsertConcurrency` to choose a value from 1
through 64. Requests for one bucket remain strictly ordered, so concurrency
does not change per-bucket sequence or mutation order. A slow bucket does not
prevent another bucket in the same writer from making progress while a request
slot is available.

`WithAppendBuffer` and `WithUpsertBuffer` bound every accepted record that has not
reached a terminal result, including queued, batched, and in-flight records.
Append writers snapshot table schema metadata (including nested types, key
lists, and properties) at construction. Later changes to the caller's table
do not change an existing writer; create a new writer for a changed schema.
Admission still validates every row's shape, nullability, and value types.
`Append` snapshots the row and its bucket key before returning, including mutable
byte slices, nested arrays/maps/rows, and decimal values. Inputs may be reused
after that return. `AppendArrow` rejects actual nulls in required fields,
including nested fields under valid parent values. The caller must retain and
keep the record batch unchanged until its future completes.
`WithAppendRequest` and `WithUpsertRequest` set one timeout for both the server
operation and its client-side network call. The timeout is therefore also the
upper bound for an accepted request when the backend stays responsive at the
socket level.

`Flush` waits for previously accepted work and returns the retained original
cause of each unrecoverable batch failure, even if its future completed before
the call. Failures are cumulative for the writer's lifetime: repeated flushes
and later successful writes do not clear them. Individual row validation and
pre-dispatch cancellation errors remain on their futures and do not poison a
bucket or enter this batch-failure history.

Context cancellation and deadlines also bound waits for buffer capacity,
serialized command admission, and a full command queue. Canceling a call before
it submits a command releases its buffer slot, if any, without interrupting
accepted work or stopping the writer. No helper goroutine outlives that wait.

`Close` stops admission, flushes accepted work, and retains the terminal flush
result. Concurrent and repeated callers observe the same final error. A
caller's context only bounds how long that caller waits; it does not shorten
the configured timeout of work the writer already accepted. A caller whose
context expires may call `Close` again to observe the final result.
If cancellation prevents the close command from being submitted, the writer
remains open; once the command is accepted, cancellation only stops that
caller's wait and shutdown continues. Ambiguous
write failures are not retried and poison only the affected bucket. See
[error handling](error-handling.md) and
[write scheduling](write-scheduling.md) for recovery and ownership details.

## Log formats and bounded scans

Select an explicit format only when it agrees with the table's advertised
`table.log.format` property. Auto mode uses the advertised format: call
`Append` for COMPACTED or INDEXED and `AppendArrow` for ARROW. If metadata
omits the property, the Fluss 1.0 default is ARROW; row appends return
`fgo.ErrInvalidConfig` before queuing anything. To use `Append` or the typed
row writer, create the table with `table.log.format=COMPACTED` or `INDEXED`.
A writer option cannot override the table's format, and an unrecognized
advertised format fails when opening the writer. This check cannot repair
incompatible batches already written to a table.

For a partitioned Arrow log, every row in a batch must match the partition
selected by `WithAppendPartition`. A mismatched or null partition value
rejects the entire batch with `fgo.ErrInvalidRow` before it is queued.

<!-- go-source: internal/docexamples/snippets_test.go logFormat -->
```go
writer, err := client.NewAppendWriter(
	ctx,
	table,
	fgo.WithAppendLogFormat(fgo.LogFormatCompacted),
)
```

A log scanner can terminate after a total row limit or at exclusive offsets for
every initial bucket. Stopping-offset maps must contain exactly those buckets.
`NewLogScanner` and `ResolveTableBuckets` return `ErrNoBucketLeader` if any
bucket of the selected table or partition is missing a leader. Retry setup
after leader election; neither method succeeds with a partial bucket list.
For already open log scanners, an unavailable leader remains a bucket-local
error, and the next route attempt can refresh that bucket's metadata.

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

### Server-side scan filters

`fgo.WithScanFilter` pushes a predicate to Fluss so that log batches that cannot
match are skipped before they are sent. Build predicates with `fgo.Col`
(`Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`, `IsNull`, `IsNotNull`, `In`, `NotIn`,
`StartsWith`, `EndsWith`, `Contains`) and combine them with `fgo.And` and
`fgo.Or`.

<!-- go-source: internal/docexamples/snippets_test.go scanFilter -->
```go
scanner, err := client.NewLogScanner(
	ctx,
	table,
	fgo.Earliest(),
	fgo.WithScanFilter(fgo.And(
		fgo.Col("amount").Ge(100),
		fgo.Or(
			fgo.Col("region").In("eu", "us"),
			fgo.Col("name").StartsWith("vip-"),
		),
	)),
)
if err != nil {
	return err
}
defer scanner.Close()
```

A filter is an optimization, not a guarantee:

- The server drops whole batches using batch statistics, so the scan still
  returns a superset of the matching rows. Apply the same predicate to the
  returned rows when exact results are required.
- Filters require a table that uses the Arrow log format and are rejected at
  scanner creation for other formats. Remote log segments are read unfiltered.
- The predicate refers to the full table schema and is evaluated before
  projection, so it may name columns that `WithScanProjection` omits. It is
  bound to the table's current schema ID when the scanner is created.
- Literals must match the column exactly. Integer literals convert to any
  wider or equal integer, floating-point, or decimal column when the value is
  exact; `*big.Rat` is used for decimals, `time.Time` for date, time, and
  timestamp columns, and `[]byte` for binary columns. Overflow, inexact
  conversions, unknown columns, nested (array, map, row) columns, and string
  matching on non-string columns are reported as `fgo.ErrInvalidConfig` when
  the scanner is created.
- `nil` is accepted only inside `In` and `NotIn`; use `IsNull` to match nulls.
- Batch and snapshot scanners do not accept filters.

`Wakeup` interrupts an active poll, or the next poll when none is active. The
poll returns `fgo.ErrWakeup`; the scanner remains usable. Row and Arrow results
preserve per-bucket failures rather than replacing a partial result with a
global success.

Log and batch scanners process one poll at a time. A queued poll observes
its own deadline and cancellation while waiting for the active poll.

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

After altering a table, call `GetTable` until it returns the new schema ID and
construct new writers and readers from that refreshed `Table`. Existing
readers intentionally retain the result shape they were created with. See the
[schema-evolution guide](schema-evolution.md) for the complete compatibility,
projection, cache, and error contract.

## Partitioned KV keys

Select a partition with `WithUpsertPartition` or `WithLookupPartition`.
Full and partial upserts, deletes, and point lookups still accept the logical
primary key, including partition columns. Any supplied partition key value
that does not match the selected partition returns `fgo.ErrInvalidRow`.
Only the remaining primary-key columns, in their original order, form the
stored key. Without an explicit bucket key, these same physical key columns
determine the bucket.

Prefix lookups still take a leading part of the logical primary key. After
partition columns are removed, the prefix must cover the bucket key, and the
bucket key must be a leading part of the physical primary key. For example,
given primary key `(region, tenant, session)`, partition key `region`, and
bucket key `tenant`, select partition `apac` and pass prefix `("apac", 7)`.
Fluss receives only the prefix for tenant `7` and a separate partition ID.

### KV key compatibility

For `TIMESTAMP(p)` and `TIMESTAMP_LTZ(p)`, binary row and key encoders reject
fractional seconds that exceed the declared precision with `ErrInvalidRow`.
Normalize the timestamp deliberately before encoding if that is what the
application intends. Representable values retain their existing wire encoding.

Earlier fluss-go clients included partition columns in stored KV keys. They
also emitted an extra sign byte for wide DECIMAL (precision greater than 18)
at negative signed-width boundaries, such as unscaled `-128` or `-32768`.
For scale 2, those values are `-1.28` and `-327.68`. This can change both the
stored key and bucket routing if an affected decimal participates in them.
Previously stored decimal **row values** still decode.

The corrected client neither reads old keys as a fallback nor rewrites them.
Point lookups can miss old rows, and writing under the corrected key can
create a second entry for the same logical primary key. Deleting via the
corrected client only addresses the corrected key.

Before changing clients on a table that may contain affected keys:

1. Stop writes from old clients and take a recoverable backup. Do not mix old
   and corrected writers against the same table during the transition.
2. Scan the current state of all affected partitions and buckets. Reconcile
   duplicate logical keys using an authoritative source or an explicit conflict
   rule; scan order alone does not identify the winning row.
3. Write the reconciled rows into a fresh table with the corrected client.
   Verify counts and representative point and supported prefix lookups from
   both Go and another Fluss 1.0 client before switching traffic. Keep the
   source table and backup until verification and rollback are no longer needed.

## KV merge and insert-if-missing

Upserts, lookups, and current-state KV scans use the value encoding advertised
by `table.kv.format` (`COMPACTED` by default or `INDEXED`). This setting does not
change physical primary-key encoding or determine the changelog format.

`MergeModeDefault` applies the table's configured merge engine.
`MergeModeOverwrite` bypasses it and writes the supplied values as replacements.
One writer uses one mode for every mutation it accepts.

`PartialUpsert` sends the complete row shape in the table's KV value format
and marks only the selected target columns for application. Omitted nullable
fields are encoded as null placeholders in that payload; the server preserves
their existing stored values. The selection must include every primary-key
column and must not include server-generated `AUTO_INCREMENT` columns. On
Fluss 1.0.0, declare the generated column nullable to use it with a partial
upsert. Fluss still generates its value on insert and preserves it on update.
`PartialUpsert` reports `ErrUnsupportedAPI` for a required generated column:
the default Fluss partial merger rejects the schema before generation runs.
Ordinary omitted non-key fields still have to be nullable.

<!-- go-source: internal/docexamples/snippets_test.go kvMerge -->
```go
writer, err := client.NewUpsertWriter(
	ctx,
	table,
	fgo.WithUpsertMergeMode(fgo.MergeModeOverwrite),
)
```

Lookupers can request Fluss to atomically insert a missing primary-key row
before returning it. Fluss fills auto-increment columns and sets nullable
non-key columns to null.

<!-- go-source: internal/docexamples/snippets_test.go lookupInsert -->
```go
lookup, err := client.NewLookuper(
	ctx,
	table,
	fgo.WithLookupInsertIfNotExists(5*time.Second, -1),
)
```

The option is rejected when the schema has required non-key values that Fluss
cannot synthesize. It is also unavailable on partitioned tables in Fluss 1.0.0;
opening that combination returns `ErrUnsupportedAPI` before sending a lookup.
Ordinary point and prefix lookups on partitioned tables remain supported.

Concurrent point and prefix calls share compatible physical-table and bucket
batches. Queue entries, batch timeout, keys per request, active requests, request
timeout, and read-only retries are independently bounded:

<!-- go-source: internal/docexamples/snippets_test.go lookupScheduling -->
```go
lookup, err := client.NewLookuper(
	ctx,
	table,
	fgo.WithLookupBatchLimits(256, 8),
	fgo.WithLookupQueue(4_096, time.Millisecond),
	fgo.WithLookupRequestTimeout(5*time.Second),
	fgo.WithLookupRetryPolicy(fgo.RetryPolicy{MaxAttempts: 3}),
)
```

Canceling one caller does not cancel work still needed by another caller in the
same protocol request. Close the lookuper to cancel active work and
complete queued callers. See [lookup scheduling](lookup-scheduling.md) for the
invariants and benchmark evidence.

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
	fgo.WithBatchSizeBytes(1<<20),
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

Primary-key current-state scans use the Fluss 1.0 `SCAN_KV` session API. Each
new page advances the server-validated call sequence; `WithBatchSizeBytes`
bounds a response. If a poll is canceled or reaches its deadline while waiting
for a submitted request, a later poll waits for the same response instead of
sending the sequence again. Each request has its own 30-second timeout. If the
request itself fails, subsequent polls return the same wrapped error: the
server may already have advanced, so the scanner neither retries nor silently
opens a new session. `Done` remains false on failure.

If decoding or historical-schema lookup fails, `Poll` returns no rows and
retains that page for local decoding on the next poll, even if it was terminal.
Close the scanner if the error is permanent rather than skipping the page.
`Close` cancels any outstanding request, discards undelivered pages, and
explicitly closes a known unfinished server session. If the opening response
never arrived, there may be no scanner ID available to close; the server
expires any session left behind. An empty bucket may complete without a
scanner ID.

Log-table current-state scans use one bounded `LIMIT_SCAN` request.
Use `ResolveTableBuckets` for a partition so the request carries that partition's
routing bucket count, which may differ from the current table default.
Snapshot scans use
`NewSnapshotBatchScanner` and require a `SnapshotBatchProvider` configured when
the client is opened. A provider can return full rows in table order or apply
a projection that selects fewer columns and return them in requested order.
For full-width results the scanner applies any requested reordering. Each
batch must use one row layout. A snapshot provider may return its final rows and
`io.EOF` together; `Poll` then returns those rows with `Done` set. Process the
result before checking whether another poll is needed. A later poll returns an
empty completed result. `BatchResult.Release` releases owned Arrow batches.

On success a snapshot provider or remote snapshot decoder must return a
non-nil reader. Nil interfaces and typed-nil readers are rejected with
`ErrValidation` during open; an error from the provider or decoder is passed
through unchanged. An empty snapshot should use a reader that returns `io.EOF`.

For snapshot scans, the first `Close` cancels an active poll and waits for it
to return before closing the reader. Providers must make `ReadBatch` respond
to its context, including while waiting on I/O. Reader `Close` is called once
and need not run concurrently with `ReadBatch`.

## Metrics

Register one observer when opening the client:

<!-- go-source: internal/docexamples/snippets_test.go metricsObserver -->
```go
observer := fgo.MetricsObserverFunc(func(event fgo.MetricEvent) {
	log.Printf("kind=%d operation=%d duration=%s", event.Kind, event.Operation, event.Duration)
})

client, err := fgo.Open(
	ctx,
	fgo.WithBootstrapServers("coordinator:9123"),
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
nodes, err := admin.GetServerNodes(ctx)
```

Close writers, scanners, and lookupers before closing the shared
`fgo.Client`.
