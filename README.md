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

## Quick Start

Open one shared client and close it after all writers, scanners, lookup clients,
and administrative clients have stopped:

```go
package main

import (
	"context"
	"log"

	"github.com/pletorco/fluss-go/pkg/fgo"
)

func main() {
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
}
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
| Protocol framing and request correlation | Implemented | [`internal/transport`](internal/transport) has bounded framing, cancellation, and protocol tests. |
| Client bootstrap, TLS, SASL and connection pooling | Implemented | [`pkg/fgo`](pkg/fgo) negotiates versions, authenticates each managed connection, bounds retries to safe reads, and supports bounded-cardinality `MetricsObserver` events. |
| Coordinator/tablet metadata and partition routing | Implemented | Metadata refreshes are coalesced, stale leaders are rerouted once, `ResolveTableBuckets` returns stable bucket snapshots, and opt-in dynamic partition creation supports partitioned writers. |
| Table, schema, logical-type and record models | Implemented | [`pkg/fgo/model.go`](pkg/fgo/model.go) and [`pkg/fgo/records.go`](pkg/fgo/records.go) model Fluss 0.9.1 tables and load authoritative schemas through `OpenTable`. |
| Arrow schema and record batches | Supported | Full Fluss logical schema conversion plus v0/v1 Arrow log batches with NONE, LZ4, and ZSTD IPC compression; decoded records use explicit `Release` ownership. |
| Row, key, KV and log record-batch codecs | Supported | Compacted/indexed rows, nested values, projected rows, v0/v1 lookup keys, KV batches, and row/Arrow log batches are covered by pinned Java 0.9.1 fixtures. |
| Log append writers | Supported | `LogWriter` provides row and Arrow appends; auto, Arrow, indexed, and compacted formats; hash/sticky/round-robin assignment; bounded batching; idempotent sequences; `Flush`; and `Close`. |
| Log scanners | Supported | `LogScanner` provides explicit/earliest/latest/timestamp subscriptions, projection pushdown, row limits, exclusive stopping offsets, remote-log merging, `Wakeup`, partial bucket errors, and row or Arrow polling. |
| Current-state and snapshot batch scans | Supported | `ResolveTableBuckets`, `NewBatchScanner`, and `NewSnapshotBatchScanner` provide bounded current-state and pluggable immutable snapshot reads with projection and explicit result ownership. |
| Primary-key writers | Supported | `KVWriter` provides full and projected upsert, delete, Fluss hash routing, merge-engine or overwrite modes, idempotent per-bucket sequences, batching, partial results, and bounded lifecycle operations. |
| Point and prefix lookups | Supported | `LookupClient` validates and batches keys by bucket, preserves input association, distinguishes not-found results, bounds concurrency, supports leading-key prefixes, and can atomically insert missing rows. |
| Typed data APIs | Supported | Generic wrappers cover log and KV writers, point and prefix lookup, log scans, current-state scans, and snapshot scans through explicit application codecs. |
| Remote storage adapters | Supported | Pluggable readers compose remote log segments and snapshot files without mandatory filesystem SDKs; local paths and `file://` URIs are built in. |
| Filesystem security-token refresh | Supported | The client acquires, clones, refreshes, revokes, and safely publishes filesystem tokens through optional providers and receivers without exposing token bytes. |
| Core `fadm` catalog client | Supported | `pkg/fadm` shares the `fgo` connection pool and implements database, table, schema, alter, partition, and per-bucket offset operations. |
| Advanced `fadm` operations | Supported | ACL, cluster config, server discovery and tags, rebalance, producer-offset, KV snapshot lease, filesystem token, lake snapshot, and per-bucket table statistics APIs from Fluss 0.9.1. |
| Live Fluss 0.9.1 compatibility | Verified | `task test:integration` runs Java-compatible golden fixtures and live plaintext, SASL PLAIN, multi-tablet, catalog, log, KV, lookup, prefix-lookup, and leader-failover checks against the digest-pinned official 0.9.1 image. |

## Documentation

- [Release changelog](CHANGELOG.md)
- API reference:
  [fmsg](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fmsg),
  [fgo](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fgo), and
  [fadm](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fadm)
- [Public architecture and ownership](docs/architecture/v0.1.md)
- [TLS and SASL secure connections](docs/authentication.md)
- [Data operations and advanced options](docs/data-operations.md)
- [Typed data API](docs/typed-api.md)
- [Remote storage and filesystem tokens](docs/remote-storage.md)
- [Write scheduling decision](docs/write-scheduling.md)
- [Initial 0.9.1 delivery record](docs/roadmap-v0.1.md)
- [Security exception policy](docs/security-exceptions.md)

## Development

This project uses Task v3.51.1 as its command entry point. Run task --list to
see available checks; task verify runs formatting, generation verification,
static analysis, unit and golden tests, bounded fuzz smoke tests, race tests,
per-file coverage checks, and security gates.

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
