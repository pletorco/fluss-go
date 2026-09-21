# Upgrade to v0.2.0-beta.1

`v0.2.0-beta.1` is the first fluss-go line for Apache Fluss 1.0. It deliberately
drops Apache Fluss 0.9.1 compatibility instead of maintaining a mixed protocol
baseline.

| Apache Fluss cluster | fluss-go version |
| --- | --- |
| `0.9.1-incubating` | Pin `v0.1.0-beta.11` |
| `1.0.0` | Use `v0.2.0-beta.1` |

Upgrade the cluster to Fluss 1.0 before deploying an application built with
fluss-go v0.2. No runtime switch or compatibility mode selects the old
protocol. Root and adapter modules should use the same fluss-go version.

```sh
go get github.com/pletorco/fluss-go@v0.2.0-beta.1
go get github.com/pletorco/fluss-go/adapters/s3@v0.2.0-beta.1
go mod tidy
```

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

Arrow predicate pushdown and historical physical-partition routing for lake
workflows are not supported in this beta. Continue applying predicates in the
application and use only current table or partition routing metadata.

## Runtime and packaging

This release remains a pure-Go implementation. It does not link or wrap the
Fluss Rust core and therefore adds no C ABI, cgo, native-library, or
cross-compilation requirement. A future Rust-core adoption requires a separate
design and packaging review.

Run the application's integration tests against the exact Fluss 1.0 deployment
before rollout. The repository's compatibility target and live evidence are
recorded in the [README compatibility matrix](../README.md#compatibility-matrix)
and [live evidence matrix](live-evidence.md).
