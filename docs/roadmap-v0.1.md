# Initial Apache Fluss 0.9.1 roadmap

Status: completed on 2026-07-30.

The initial fluss-go milestone targets only Apache Fluss
`v0.9.1-incubating` at commit
`6bf969f71af8d6f9cc37383ab89ae46a58b0e227`. It establishes an experimental Go
API and does not promise compatibility with later Fluss releases or a stable
v1 Go API.

## Delivered packages

| Package | Delivered boundary |
| --- | --- |
| `pkg/fmsg` | Reproducible protobuf messages, API keys, API versions, and error metadata for keys 1000 through 1059. |
| `pkg/fgo` | Bootstrap, TLS, SASL PLAIN, managed connections, metrics, metadata and dynamic partition routing, row and Arrow codecs, log append and bounded scan, KV merge modes, point and prefix lookup, insert-if-not-exists, current-state and snapshot scans, typed APIs, remote storage, and filesystem-token refresh. |
| `pkg/fadm` | Database, table, schema, partition, offset, ACL, configuration, server discovery and tags, rebalance, snapshot lease, token, lake, and table-statistics administration. |

The implemented dependency and lifecycle boundaries are documented in
[architecture/v0.1.md](architecture/v0.1.md). The public feature matrix and
supported tool versions are maintained in the repository
[README](../README.md).

Usage and extension contracts are documented in
[data-operations.md](data-operations.md), [typed-api.md](typed-api.md),
[remote-storage.md](remote-storage.md), and
[write-scheduling.md](write-scheduling.md).

## Validation evidence

- `task verify` is the local and CI gate for formatting, deterministic
  generation, vet, unit and golden tests, bounded fuzzing, race tests,
  per-file coverage, govulncheck, and Trivy.
- Every hand-written Go source file has a coverage baseline of at least 80%.
- Sonar analysis enforces at least 80% new-code coverage, zero new issues, and
  no more than 3% new duplicated lines.
- Java 0.9.1-compatible byte fixtures cover protocol frames, keys, rows, KV
  batches, log batches, Arrow batches, and bucket hashing.
- `task test:integration` uses the official Fluss 0.9.1 image pinned by digest
  and verifies plaintext, SASL PLAIN, bootstrap failover, role-aware API
  negotiation, catalog operations, append and scan, KV mutation, point and
  prefix lookup, dynamic partition creation, bounded scans, supported log
  formats, security tokens, and a three-tablet leader change.
- CI covers patched Go 1.25 and 1.26 releases without toolchain auto-switching.
  Weekly workflows rerun security and live compatibility checks.

## Deliberate limits

- Later Fluss releases and APIs introduced after key 1059 are unsupported.
- Exported APIs remain experimental before a separately reviewed v1 contract.
- Live compatibility claims cover the workflows listed above, not every
  possible server configuration, schema evolution path, or failure topology.
- Expanding the supported matrix requires updated protocol inputs, fixtures,
  documentation, and live integration evidence.
