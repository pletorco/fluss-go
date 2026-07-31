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
the client is opened. `BatchResult.Release` releases owned Arrow batches.

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
