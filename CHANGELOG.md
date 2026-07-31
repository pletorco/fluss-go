# Changelog

All notable user-visible changes to fluss-go are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Until v1, exported APIs remain experimental and prereleases may contain
breaking changes.

## [Unreleased]

### Added

- Added separately reviewed `fgo`, `fadm`, and generated `fmsg` API baselines
  with a required `apidiff` compatibility gate.
- Added a pinned, fail-closed Trivy dependency-license allowlist with a denied
  license canary across the root and optional adapter modules.
- Added live post-failure append, scan, upsert, lookup, coordinator recovery,
  and cancellation-isolation checks against the three-tablet Fluss cluster.

### Changed

- Split S3, OSS, HDFS, and OpenTelemetry adapters into separately versioned Go
  modules so core-only consumers no longer inherit their optional SDK graphs.

## [v0.1.0-beta.6] - 2026-07-31

### Added

- Added an optional Alibaba Cloud OSS remote-file adapter based on the official
  OSS SDK for Go v2.
- Added an HDFS remote-file adapter boundary for application-owned clients with
  exact range validation, cancellation cleanup, and filesystem-token cloning.

### Fixed

- Preserved the reader-side connection failure when a peer closes immediately
  after receiving a request by avoiding a redundant write-deadline reset.
- Prevented point and prefix lookup calls racing with `LookupClient.Close` from
  being stranded after scheduler shutdown.

## [v0.1.0-beta.5] - 2026-07-31

### Added

- Added bounded, opt-in idempotent retries for log and KV writers. Retries
  preserve writer IDs, batch sequences, and encoded bytes, recognize an
  already-committed duplicate sequence as success, and stop on ambiguous or
  out-of-order sequence failures.
- Added bounded cross-call point and prefix lookup batching with configurable
  queue capacity, batch delay, request size, concurrency, timeout, and
  read-only retry policy.
- Added streaming range reads and ordered prefetch for remote logs and
  snapshots. Concurrent object and byte limits bound active downloads while
  retaining the complete-object reader as a compatibility path.
- Added an optional Amazon S3 remote-file adapter based on the official AWS SDK
  for Go v2.
- Added an optional OpenTelemetry metrics adapter based on the stable official
  OpenTelemetry Go metrics API.
- Added `RenewKVSnapshotLease` for extending an existing Fluss KV snapshot
  lease without reacquiring its snapshots.

### Changed

- Made the Fluss 0.9.1 live integration workflow a required pull-request gate
  and added cancellation-isolation coverage to the live suite.
- Rejected relative local paths and validated exact remote ranges, advertised
  sizes, aggregate limits, and active-stream budgets before unbounded
  allocation.
- Replaced scheduler-dependent test sleeps with deterministic synchronization
  and reduced cognitive complexity without changing public behavior.
- Expanded the build-versus-buy register and package manuals for writer and
  lookup scheduling, remote storage, optional adapters, and observability.

### Fixed

- Prevented an already-canceled request from poisoning an established managed
  connection.
- Discarded and transparently replaced a connection when cancellation
  interrupts a partially written frame, while preserving the request-local
  cancellation error for the original caller.

## [v0.1.0-beta.4] - 2026-07-31

### Added

- Added compatible historical-schema decoding for point and prefix lookups,
  current-state batches, and row and Arrow log batches. Stable field IDs now
  preserve renamed columns, ignore dropped columns, and fill newly nullable
  columns with `nil`.
- Added configurable per-writer request concurrency across distinct buckets
  through `WithLogConcurrency` and `WithKVConcurrency`, while preserving
  strict request order within each bucket.
- Added aggregate byte and file-count limits to remote log and snapshot reads
  through `RemoteFileReadConfig.MaxTotalBytes` and `MaxFiles`.

### Changed

- Made bounded fuzz smoke tests deterministic across supported Go versions by
  using fixed iteration budgets instead of wall-clock deadlines.
- Made socket writes context-aware and completed canceled, failed, or closed
  transport requests exactly once.
- Made log and KV request timeouts bound both the server operation and the
  client-side network call. Writer buffer limits now include queued, batched,
  and in-flight records.
- Coalesced connection, metadata, dynamic-partition, and schema requests with
  independent waiter cancellation and last-waiter operation cancellation.
- Restricted remote-read retries to explicitly temporary, timeout, or
  truncated-read failures and validated aggregate limits before downloading.
- Allowed filesystem-token receivers to close the client without deadlocking
  shutdown.
- Grouped Go Task output in GitHub Actions so successful coverage results are
  no longer rendered as error log lines.

### Fixed

- Preserved a snapshot reader's final non-empty batch when it returns rows and
  `io.EOF` together.
- Preserved the terminal flush error across concurrent and repeated log and KV
  writer `Close` calls, including callers that previously stopped waiting.

## [v0.1.0-beta.3] - 2026-07-31

### Changed

- Added production TLS and SASL PLAIN configuration and authentication-error
  guidance with compile-checked examples.
- Added error classification, safe-retry, ambiguous-writer recovery, and
  partial-result guidance with deterministic examples.
- Added advanced administration guidance for cluster settings, server tags,
  rebalances, producer offsets, snapshot leases, filesystem credentials, lake
  snapshots, and per-bucket statistics with compile-checked lifecycle examples.
- Added deterministic Markdown link and anchor validation plus source-backed,
  compile-checked Go fences to the documentation CI gate.
- Expanded pkg.go.dev contracts for typed APIs, extension interfaces,
  configuration, result ownership, partial failures, and pre-v1 stability.
- Added a CI documentation audit for public packages, declarations, methods,
  and interface contracts in hand-written Go source.
- Expanded pkg.go.dev examples for KV writes, lookups, bounded scans, table
  creation, partial failures, and deterministic codecs.
- Documented row, binary array and map, and record-batch wire layouts against
  the pinned Apache Fluss 0.9.1 compatibility fixtures.

## [v0.1.0-beta.2] - 2026-07-31

### Added

- Added named ACL resource, operation, permission, and principal types with
  Apache Fluss 0.9.1 constants and wildcard helpers.
- Added examples and live SASL authorization coverage for creating, listing,
  enforcing, and dropping ACLs.

### Changed

- Changed `fadm.ACL` and `fadm.ACLFilter` fields from raw `int32` and `string`
  values to the new typed ACL contracts.
- ACL input and server responses now reject unsupported enum values, malformed
  principals, incomplete filters, and invalid wildcard combinations.

## [v0.1.0-beta.1] - 2026-07-31

### Added

- Added dynamic partition creation and routing for partitioned writers.
- Added KV merge modes and atomic insert-if-not-exists lookups.
- Added bounded log scans with row limits, stopping offsets, and explicit
  wakeup support.
- Added current-state and immutable snapshot batch scanners.
- Added row and Arrow log format selection, including indexed and compacted
  formats.
- Added typed generic wrappers for log and KV writers, lookups, log scans, and
  batch scans.
- Added pluggable remote log and snapshot readers with local filesystem
  support.
- Added filesystem security-token acquisition, refresh, publication, and
  revocation hooks.
- Added bounded-cardinality client metrics hooks and admin server discovery.
- Added package manuals, runnable examples, installation guidance, and
  operational documentation.

### Changed

- Required patched Go releases: Go 1.25.12 or newer in the 1.25 series, or Go
  1.26.5 or newer in the 1.26 series.
- Expanded live Apache Fluss 0.9.1 compatibility coverage for the newly added
  data and administration workflows.

### Security

- Pinned a patched Go toolchain and strengthened CI checks for known
  standard-library vulnerabilities.

## [v0.1.0-alpha.1] - 2026-07-30

### Added

- Added reproducible Apache Fluss 0.9.1 protobuf messages, API keys, API
  versions, and protocol error metadata in `fmsg`.
- Added framed transport, request correlation, bootstrap, TLS, SASL PLAIN,
  managed connections, metadata routing, and bounded safe-read retries.
- Added Fluss schemas, logical types, rows, Arrow records, keys, and KV and log
  record-batch codecs with Java-compatible golden fixtures.
- Added log writers and scanners, primary-key writers, point lookups, and
  prefix lookups in `fgo`.
- Added database, table, schema, partition, offset, ACL, configuration,
  rebalance, snapshot, token, lake, and table-statistics administration in
  `fadm`.
- Added live plaintext, SASL PLAIN, catalog, data, and leader-failover
  compatibility tests against the pinned Apache Fluss 0.9.1 image.
- Added Task-based formatting, generation, testing, coverage, Sonar, CVE, and
  repository security gates.
- Added Apache License 2.0 licensing and third-party attribution.

[Unreleased]: https://github.com/pletorco/fluss-go/compare/v0.1.0-beta.6...HEAD
[v0.1.0-beta.6]: https://github.com/pletorco/fluss-go/compare/v0.1.0-beta.5...v0.1.0-beta.6
[v0.1.0-beta.5]: https://github.com/pletorco/fluss-go/compare/v0.1.0-beta.4...v0.1.0-beta.5
[v0.1.0-beta.4]: https://github.com/pletorco/fluss-go/compare/v0.1.0-beta.3...v0.1.0-beta.4
[v0.1.0-beta.3]: https://github.com/pletorco/fluss-go/compare/v0.1.0-beta.2...v0.1.0-beta.3
[v0.1.0-beta.2]: https://github.com/pletorco/fluss-go/compare/v0.1.0-beta.1...v0.1.0-beta.2
[v0.1.0-beta.1]: https://github.com/pletorco/fluss-go/compare/v0.1.0-alpha.1...v0.1.0-beta.1
[v0.1.0-alpha.1]: https://github.com/pletorco/fluss-go/releases/tag/v0.1.0-alpha.1
