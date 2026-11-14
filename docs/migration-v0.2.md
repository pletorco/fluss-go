# Upgrade to v0.2.0-beta.3

The fluss-go v0.2 line, introduced by `v0.2.0-beta.1`, targets Apache Fluss 1.0.
It deliberately drops Apache Fluss 0.9.1 compatibility instead of maintaining a
mixed protocol baseline. `v0.2.0-beta.3` is the current release of that line.

| Apache Fluss cluster | fluss-go version |
| --- | --- |
| `0.9.1-incubating` | Pin `v0.1.0-beta.11` |
| `1.0.0` | Use `v0.2.0-beta.3` |

Upgrade the cluster to Fluss 1.0 before deploying an application built with
fluss-go v0.2. No runtime switch or compatibility mode selects the old
protocol. Root and adapter modules should use the same fluss-go version.

```sh
go get github.com/pletorco/fluss-go@v0.2.0-beta.3
go get github.com/pletorco/fluss-go/adapters/s3@v0.2.0-beta.3
go mod tidy
```

## Upgrading from v0.2.0-beta.2

`v0.2.0-beta.3` removes no exported API and does not change Fluss wire
behavior. Review these maintenance requirements:

- Go 1.26.11 is now the module, workspace, and compatibility-test baseline.
  Go 1.25 is no longer supported.
- `github.com/klauspost/compress` 1.20.2 fixes `CVE-2026-116591`
  (`GO-2026-6865`), and Go 1.26.11 contains the required standard-library
  fixes for `CVE-2026-111943` and `CVE-2026-97031`.
- Goldmark is updated to 1.8.7. The optional adapters use Alibaba Cloud OSS
  SDK 1.6.1, OpenTelemetry 1.47.0, AWS SDK core 1.48.0, and AWS S3 1.114.10.
- Root and adapter modules must all use `v0.2.0-beta.3`; mixing beta.2 adapter
  requirements with the beta.3 root module is not a supported release set.

The [native lake-format decision](lake-formats.md) only removes an obsolete Go
version mismatch from its Iceberg evaluation; the no-implementation decision
and application-owned planning boundary are unchanged. README, adapter manuals,
build-vs-buy records, and release instructions otherwise only align supported
versions and maintainer procedures, with no additional application migration.

## Upgrading from v0.2.0-beta.1

`v0.2.0-beta.2` removes no exported API. Review these changes:

- `fgo.WithScanFilter`, `fgo.Col`, `fgo.And`, and `fgo.Or` are new. They let a
  log scanner push a predicate to Fluss, but the server returns a superset of
  the matching rows, so keep applying predicates in the application. See
  [data-operations.md](data-operations.md#server-side-scan-filters).
- `UpsertWriter.Close` no longer waits for a pending KV backpressure delay
  before sending its final batches.
- A connection that fails TLS or transport setup now reports a failure to close
  the partial connection joined to the original error. `errors.Is` and
  `errors.As` still match the original error.
- Writer retry execution returns `ErrInvalidConfig` instead of panicking when
  it is called with an unvalidated retry policy.

## Behavior to review

- `NewBatchScanner` now uses the Fluss 1.0 server-side `SCAN_KV` session for
  primary-key tables. Continue polling until `Done`, release each result, and
  close an unfinished scanner. `WithBatchSizeBytes` bounds each response.
- Metadata exposes leader, replica, ISR, bucket-count epoch, remote-data, and
  per-partition bucket-count details. Routing requests send the authoritative
  routing bucket count and dynamic partition creation waits for a real leader.
- `UpsertWriter` reports server storage pressure in `WriteResult` and applies
  bounded cooperative throttling. `WithUpsertBackpressureMaxThrottle` changes
  the maximum delay.
- `AlterDatabase`, `GetClusterHealth`, `ListRemoteLogManifests`,
  `ListKVSnapshots`, and table bucket-count alteration are new public admin
  operations.
- Log scans honor Fluss 1.0 `filtered_end_offset`, and existing data APIs use
  their Fluss 1.0 negotiated versions.

Historical physical-partition routing for lake workflows is not supported in
this beta; use only current table or partition routing metadata. Log scanners
can push predicates to Fluss with `fgo.WithScanFilter`, but the server returns
a superset of matching rows, so keep applying predicates in the application.
See [data-operations.md](data-operations.md#server-side-scan-filters).

## Runtime and packaging

This release remains a pure-Go implementation. It does not link or wrap the
Fluss Rust core and therefore adds no C ABI, cgo, native-library, or
cross-compilation requirement. A future Rust-core adoption requires a separate
design and packaging review.

Run the application's integration tests against the exact Fluss 1.0 deployment
before rollout. The repository's compatibility target and live evidence are
recorded in the [README compatibility matrix](../README.md#compatibility-matrix)
and [live evidence matrix](live-evidence.md).
