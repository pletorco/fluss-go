# fluss-go

Experimental Go client for Apache Fluss 0.9.1-incubating.

Development and review follow the project
[coding guidelines](CODING_GUIDELINES.md).

The public API will follow the package separation used by `franz-go` while
remaining centered on the Apache Fluss table model:

- `pkg/fmsg`: Fluss wire requests, responses, API keys, and versions.
- `pkg/fgo`: data client for log and primary-key table operations.
- `pkg/fadm`: administrative client for databases, tables, partitions, and clusters.

## Fluss 0.9.1 Feature Matrix

This matrix is the source of truth for public capability claims. A feature is
only marked complete after its implementation and verification evidence are
updated here. Protocol-message coverage is not end-user feature parity.

| Area | Status | Evidence and scope |
| --- | --- | --- |
| `fmsg` public API registry and protobuf messages | Implemented | Generated from the pinned Fluss 0.9.1 protocol; includes API/version and server-error registries. |
| Protocol framing and request correlation | Implemented | [`internal/transport`](internal/transport) has bounded framing, cancellation, and protocol tests. |
| Client bootstrap, TLS, SASL and connection pooling | Implemented | [`pkg/fgo`](pkg/fgo) negotiates versions, authenticates each managed connection, and retries safe reads only. Live authentication coverage is tracked in [#27](https://github.com/pletorco/fluss-go/issues/27). |
| Coordinator/tablet metadata routing | Implemented | [`pkg/fgo/metadata.go`](pkg/fgo/metadata.go) and [`pkg/fgo/metadata_client.go`](pkg/fgo/metadata_client.go) cache table and partition leaders, coalesce refreshes, and reroute once after stale metadata. |
| Table, schema, logical-type and record models | Implemented | [`pkg/fgo/model.go`](pkg/fgo/model.go) and [`pkg/fgo/records.go`](pkg/fgo/records.go) model Fluss 0.9.1 tables and load authoritative schemas through `OpenTable`. |
| Arrow schema and record batches | Supported | Full Fluss logical schema conversion plus v0/v1 Arrow log batches with NONE, LZ4, and ZSTD IPC compression; decoded records use explicit `Release` ownership. |
| Row, key, KV and log record-batch codecs | Supported | Compacted/indexed rows, nested values, projected rows, v0/v1 lookup keys, KV batches, and row/Arrow log batches are covered by pinned Java 0.9.1 fixtures. |
| Log append writers | Supported | `LogWriter` provides schema-aware row and explicit-bucket Arrow appends, Fluss hash/sticky/round-robin assignment, bounded per-bucket batching, idempotent sequences, partial results, `Flush`, and `Close`. |
| Log scanners | Supported | `LogScanner` provides explicit/earliest/latest/timestamp subscriptions, projection pushdown, ordered row and Arrow polling, partial bucket errors, and cancellation-aware lifecycle management. |
| Primary-key writers | Supported | `KVWriter` provides full and projected upsert, delete, Fluss hash routing, v0/v1 `PUT_KV`, idempotent per-bucket sequences, batching, partial results, and bounded lifecycle operations. |
| Point and prefix lookups | Supported | `LookupClient` validates and batches v0/v1 keys by bucket, preserves input association, distinguishes not-found results, bounds concurrency, and supports routable leading-key prefixes. |
| Core `fadm` catalog client | Supported | `pkg/fadm` shares the `fgo` connection pool and implements database, table, schema, alter, partition, and per-bucket offset operations. |
| Advanced `fadm` operations | Supported | ACL, cluster config, server tag, rebalance, producer-offset, KV snapshot lease, filesystem token, lake snapshot, and per-bucket table statistics APIs from Fluss 0.9.1. |
| Live Fluss 0.9.1 compatibility | Not yet verified | Unit, golden, and race tests run in `task verify`; the opt-in live integration harness is tracked in [#27](https://github.com/pletorco/fluss-go/issues/27). Do not infer production compatibility from generated protocol types alone. |

## Development

This project uses Task v3.51.1 as its command entry point. Run task --list to
see available checks; task verify runs formatting, generation verification,
static analysis, unit and golden tests, bounded fuzz smoke tests, race tests,
per-file coverage checks, and security gates.

Local security scans require Trivy v0.72.0. Integration tests require an
explicitly configured Apache Fluss cluster and FLUSS_INTEGRATION=1.

## Compatibility Matrix

| Component | Supported versions |
| --- | --- |
| Apache Fluss | `0.9.1-incubating` at commit `6bf969f71af8d6f9cc37383ab89ae46a58b0e227` only |
| Go | `1.25.x` and `1.26.x`; the module language baseline is `1.25.0` |
| Task | `3.51.1` |
| Trivy | `0.72.0` |

Later Fluss versions are unsupported until their protocol inputs, golden
fixtures, and live compatibility suite are explicitly added to this matrix.

Security findings block merges. The documented, time-limited exception process
is in [docs/security-exceptions.md](docs/security-exceptions.md).

fluss-go is licensed under the Apache License 2.0. See LICENSE and NOTICE.
