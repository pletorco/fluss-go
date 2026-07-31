# Changelog

All notable user-visible changes to fluss-go are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Until v1, exported APIs remain experimental and prereleases may contain
breaking changes.

## [Unreleased]

### Changed

- Added production TLS and SASL PLAIN configuration and authentication-error
  guidance with compile-checked examples.
- Added error classification, safe-retry, ambiguous-writer recovery, and
  partial-result guidance with deterministic examples.
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

[Unreleased]: https://github.com/pletorco/fluss-go/compare/v0.1.0-beta.2...HEAD
[v0.1.0-beta.2]: https://github.com/pletorco/fluss-go/compare/v0.1.0-beta.1...v0.1.0-beta.2
[v0.1.0-beta.1]: https://github.com/pletorco/fluss-go/compare/v0.1.0-alpha.1...v0.1.0-beta.1
[v0.1.0-alpha.1]: https://github.com/pletorco/fluss-go/releases/tag/v0.1.0-alpha.1
