# fluss-go

Experimental Go client for Apache Fluss 0.9.1-incubating.

[![Go Reference](https://pkg.go.dev/badge/github.com/pletorco/fluss-go.svg)](https://pkg.go.dev/github.com/pletorco/fluss-go)

Development and review follow the project
[coding guidelines](CODING_GUIDELINES.md).

The public API will follow the package separation used by `franz-go` while
remaining centered on the Apache Fluss table model:

- `pkg/fmsg`: Fluss wire requests, responses, API keys, and versions.
- `pkg/fgo`: data client for log and primary-key table operations.
- `pkg/fadm`: administrative client for databases, tables, partitions, and clusters.

## Install

```sh
go get github.com/pletorco/fluss-go@latest
```

The API is experimental before v1. Pin a release in production rather than
tracking a branch or an unversioned revision.

Remote-storage and observability adapters are separately versioned Go modules.
Importing only `pkg/fgo`, `pkg/fadm`, or `pkg/fmsg` does not add cloud or
OpenTelemetry SDKs to the root module graph. Install an adapter at the same
release version when it is needed, for example:

```sh
go get github.com/pletorco/fluss-go/adapters/s3@latest
```

## Quick Start

Open one shared client and close it after all writers, scanners, lookup clients,
and administrative clients have stopped:

<!-- go-source: internal/docexamples/snippets_test.go quickStart -->
```go
ctx := context.Background()
client, err := fgo.Open(
	ctx,
	fgo.WithSeedBrokers("localhost:9123"),
	fgo.WithClientIdentity("example", "1.0.0"),
)
if err != nil {
	log.Fatal(err)
}
defer func() {
	if err := client.Close(); err != nil {
		log.Printf("close Fluss client: %v", err)
	}
}()

table, err := client.OpenTable(ctx, fgo.TablePath{
	Database: "fluss",
	Table:    "events",
})
if err != nil {
	log.Fatal(err)
}
log.Printf("opened table %s with %d buckets", table.Path, table.BucketCount)
```

`fgo.Client` owns negotiated coordinator and tablet connections. Create
`fadm.Client`, data writers, scanners, and lookup clients from this shared
client instead of opening a transport for each operation.

## Fluss 0.9.1 Feature Matrix

This matrix is the source of truth for public capability claims. A feature is
only marked complete after its implementation and verification evidence are
updated here. Protocol-message coverage is not end-user feature parity.

| Area | Status | Evidence and scope |
| --- | --- | --- |
| `fmsg` public API registry and protobuf messages | Implemented | Generated from the pinned Fluss 0.9.1 protocol; includes API/version and server-error registries. |
| Protocol framing and request correlation | Implemented | [`internal/transport`](internal/transport) has bounded framing, context-aware writes, exactly-once completion, cancellation, and protocol tests. |
| Client bootstrap, TLS, SASL and connection pooling | Implemented | [`pkg/fgo`](pkg/fgo) negotiates versions, authenticates each managed connection, bounds retries to safe reads, and supports bounded-cardinality `MetricsObserver` events with an optional OpenTelemetry API adapter. |
| Coordinator/tablet metadata and partition routing | Implemented | Metadata refreshes are coalesced, stale leaders are rerouted once, `ResolveTableBuckets` returns stable bucket snapshots, and opt-in dynamic partition creation supports partitioned writers. |
| Table, schema, logical-type and record models | Implemented | [`pkg/fgo/model.go`](pkg/fgo/model.go) and [`pkg/fgo/records.go`](pkg/fgo/records.go) model Fluss 0.9.1 tables, load authoritative schemas through `OpenTable`, and resolve historical record schemas through a bounded client cache. |
| Arrow schema and record batches | Supported | Full Fluss logical schema conversion plus v0/v1 Arrow log batches with NONE, LZ4, and ZSTD IPC compression; decoded records use explicit `Release` ownership. |
| Row, key, KV and log record-batch codecs | Supported | Compacted/indexed rows, nested values, projected rows, v0/v1 lookup keys, KV batches, and row/Arrow log batches are covered by pinned Java 0.9.1 fixtures. |
| Log append writers | Supported | `LogWriter` provides row and Arrow appends; auto, Arrow, indexed, and compacted formats; hash/sticky/round-robin assignment; bounded batching and per-bucket concurrency; idempotent sequences; `Flush`; and deterministic `Close`. |
| Log scanners | Supported | `LogScanner` provides explicit/earliest/latest/timestamp subscriptions, schema-aware projection, row limits, exclusive stopping offsets, remote-log merging, `Wakeup`, partial bucket errors, and row or Arrow polling. |
| Current-state and snapshot batch scans | Supported | `ResolveTableBuckets`, `NewBatchScanner`, and `NewSnapshotBatchScanner` provide bounded current-state and pluggable immutable snapshot reads with projection and explicit result ownership. |
| Primary-key writers | Supported | `KVWriter` provides full and projected upsert, delete, Fluss hash routing, merge-engine or overwrite modes, idempotent per-bucket sequences, bounded batching and per-bucket concurrency, partial results, and deterministic lifecycle operations. |
| Point and prefix lookups | Supported | `LookupClient` validates keys, batches compatible concurrent calls by bucket, preserves caller cancellation and input association, bounds queue/delay/concurrency/retries, resolves historical schemas, supports leading-key prefixes, and can atomically insert missing rows without unsafe retries. |
| Typed data APIs | Supported | Generic wrappers cover log and KV writers, point and prefix lookup, log scans, current-state scans, and snapshot scans through explicit application codecs. |
| Remote storage adapters | Supported | Pluggable complete-object or streaming range readers compose remote logs and snapshots with bounded retries, object/aggregate/active byte limits, ordered prefetch, and cancellation cleanup. Local files are built in; optional adapters cover S3, OSS, and application-owned HDFS clients. |
| Filesystem security-token refresh | Supported | The client acquires, clones, refreshes, revokes, and safely publishes filesystem tokens through optional providers and receivers without exposing token bytes. |
| Core `fadm` catalog client | Supported | `pkg/fadm` shares the `fgo` connection pool and implements database, table, schema, alter, partition, and per-bucket offset operations. |
| Advanced `fadm` operations | Supported | ACL, cluster config, server discovery and tags, rebalance, producer-offset, KV snapshot acquire/renew/release, filesystem token, lake snapshot, and per-bucket table statistics APIs from Fluss 0.9.1. |
| Live Fluss 0.9.1 compatibility | Verified | `task test:integration` runs Java-compatible golden fixtures and live plaintext, SASL PLAIN, multi-tablet, catalog, log, KV, lookup, prefix-lookup, schema-evolution, and leader-failover checks against the digest-pinned official 0.9.1 image. |

## Documentation

- [Release changelog](CHANGELOG.md)
- [Schema evolution](docs/schema-evolution.md)
- API reference:
  [fmsg](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fmsg),
  [fgo](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fgo), and
  [fadm](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fadm)
- [Public architecture and ownership](docs/architecture/v0.1.md)
- [Public API stability and compatibility checks](docs/public-api-stability.md)
- [Go module and optional dependency layout](docs/module-layout.md)
- [Request coalescing and cancellation](docs/architecture/request-coalescing.md)
- [Build-vs-buy decisions](docs/build-vs-buy.md)
- [TLS and SASL secure connections](docs/authentication.md)
- [Error handling and recovery](docs/error-handling.md)
- [Advanced administration](docs/admin-operations.md)
- [Data operations and advanced options](docs/data-operations.md)
- [Typed data API](docs/typed-api.md)
- [Remote storage and filesystem tokens](docs/remote-storage.md)
- [Optional AWS SDK v2 S3 adapter](adapters/s3/README.md)
- [Optional Alibaba Cloud OSS SDK v2 adapter](adapters/oss/README.md)
- [Optional HDFS client adapter](adapters/hdfs/README.md)
- [Observability and optional OpenTelemetry adapter](docs/observability.md)
- [Documentation validation and snippet workflow](docs/documentation-tooling.md)
- [Release process](docs/releasing.md)
- [Write scheduling decision](docs/write-scheduling.md)
- [Lookup scheduling decision](docs/lookup-scheduling.md)
- [Initial 0.9.1 delivery record](docs/roadmap-v0.1.md)
- [Security exception policy](docs/security-exceptions.md)
- [Dependency license policy](docs/dependency-license-policy.md)

## Development

This project uses Task v3.51.1 as its command entry point. Run task --list to
see available checks; task verify runs formatting, generation verification,
static analysis, unit and golden tests, bounded fuzz smoke tests, race tests,
per-file coverage checks, and security gates.

The checked-in `go.work` joins the root module and the four adapter modules for
repository development. Published applications should select only the modules
they import; the workspace file is not part of module resolution for users.

Documentation changes use `task docs:check`. Edit compile-checked snippet
sources under `internal/docexamples` and run `task docs:snippets:sync` to update
their Markdown fences.

Protocol generation requires protoc v3.21.12. Local security scans require
Trivy v0.72.0. Integration tests require Docker, Docker Compose, and OpenSSL;
the task creates and removes isolated clusters. The module recommends Go
1.26.5 so the Go command can automatically avoid known standard-library
vulnerabilities; set `GOTOOLCHAIN=local` only when intentionally testing
another supported, patched Go release.

## Compatibility Matrix

| Component | Supported versions |
| --- | --- |
| Apache Fluss | `0.9.1-incubating` at commit `6bf969f71af8d6f9cc37383ab89ae46a58b0e227` only |
| Go | `1.25.12` or newer in the 1.25 series; `1.26.5` or newer in the 1.26 series; module language baseline `1.25.0` |
| Task | `3.51.1` |
| protoc | `3.21.12` |
| Trivy | `0.72.0` |

Later Fluss versions are unsupported until their protocol inputs, golden
fixtures, and live compatibility suite are explicitly added to this matrix.

Security findings block merges. Exceptions follow the documented,
time-limited approval process.

fluss-go is licensed under the Apache License 2.0. See LICENSE and NOTICE.
