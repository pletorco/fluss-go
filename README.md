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
| Row, key, KV and log record-batch codecs | Supported | Compacted/indexed rows, nested values, projected rows, v0/v1 lookup keys, KV batches, and row log batches are covered by pinned Java 0.9.1 fixtures. Arrow batches remain tracked in [#9](https://github.com/pletorco/fluss-go/issues/9). |
| Log writers, scanners, upserts and lookups | Planned | These user operations depend on the record codecs and are tracked in [#10](https://github.com/pletorco/fluss-go/issues/10), [#11](https://github.com/pletorco/fluss-go/issues/11), [#12](https://github.com/pletorco/fluss-go/issues/12), and [#13](https://github.com/pletorco/fluss-go/issues/13). |
| `fadm` administrative client | Planned | No `pkg/fadm` package is published. Core and advanced administrative APIs are tracked in [#14](https://github.com/pletorco/fluss-go/issues/14) and [#15](https://github.com/pletorco/fluss-go/issues/15). |
| Live Fluss 0.9.1 compatibility | Not yet verified | Unit, golden, and race tests run in `task verify`; the opt-in live integration harness is tracked in [#27](https://github.com/pletorco/fluss-go/issues/27). Do not infer production compatibility from generated protocol types alone. |

## Development

This project uses Task v3.51.1 as its command entry point. Run task --list to
see available checks; task verify runs formatting, generation verification,
static analysis, unit tests, race tests, and security gates.

Local security scans require Trivy v0.72.0. Integration tests require an
explicitly configured Apache Fluss cluster and FLUSS_INTEGRATION=1.

Security findings block merges. The documented, time-limited exception process
is in [docs/security-exceptions.md](docs/security-exceptions.md).

fluss-go is licensed under the Apache License 2.0. See LICENSE and NOTICE.
