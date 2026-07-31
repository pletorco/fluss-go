# Fluss 0.9.1 live evidence

This matrix records which public client workflows are exercised against the
pinned Apache Fluss `0.9.1-incubating` image. `task test:integration` creates
an isolated cluster, bounds every operation, and removes all volumes. Unit and
golden coverage remains required even when live evidence exists.

## Data APIs

| Public surface | Live evidence | Boundary |
| --- | --- | --- |
| Log writer, row and Arrow writer, log scanner | Covered | Appends, explicit formats, bounded scans, schema evolution, acknowledged offsets, and post-leader-failure order are checked. |
| KV writer, delete, partial upsert, merge modes | Covered | Full upsert/delete, full-schema partial-update payloads, preserved nullable fields, `FIRST_ROW`, overwrite bypass, and post-failure updates are checked. |
| Point and prefix lookup | Covered | Found, missing, deleted, insert-if-missing, concurrent, typed, and post-failure results are checked. |
| Current-state batch scanner | Covered | Row and typed scanners cover every resolved bucket and projections. |
| Typed log and KV writers, log scanner, lookup, batch scanner | Covered | Explicit codecs round-trip application structs through the real row protocol. |
| Snapshot batch scanner and typed snapshot scanner | Provider-only | Fluss returns snapshot object metadata, but decoding RocksDB snapshot objects requires an application-selected storage reader and decoder. Their orchestration and ownership are unit tested; the stock suite has no portable decoder to substitute safely. |
| Remote log and snapshot readers | Adapter/service-specific | Core scheduling is unit, race, and benchmark tested. S3, OSS, and HDFS adapters have separate opt-in service tasks because the Fluss image does not provide those external services. |

## Administration APIs

| Public surface | Live evidence | Boundary |
| --- | --- | --- |
| Database, table, partition, schema, and offset methods | Covered | Create, describe, list, exists, alter, drop, dynamic partitions, historical schema, and earliest/latest offsets are checked with cleanup. |
| ACL methods | Covered | Create, list, authorization denial, and drop run on the isolated SASL cluster. |
| `DescribeClusterConfigs` | Covered | Effective stock-cluster values and non-empty metadata are checked. |
| `AlterClusterConfigs` | Deliberately not mutated | Fluss 0.9.1 does not expose a guaranteed dynamic, reversible no-op key. Request encoding, validation, and server errors remain unit tested to avoid leaving cluster-wide state ambiguous. |
| `ServerNodes` | Covered | Coordinator and all three tablet roles are checked before and after coordinator recovery. |
| Server tag add/remove | Deliberately not mutated | The only valid 0.9.1 tags are operational `PERMANENT_OFFLINE` and `TEMPORARY_OFFLINE`; applying either changes tablet eligibility and invalidates the same suite's failover topology. Enum validation, request bytes, and cleanup behavior are unit tested. |
| Rebalance start/progress/wait/cancel | Deliberately not started | Goal IDs are raw, unnamed 0.9.1 values and a rebalance changes placement used by deterministic failover assertions. Validation, response conversion, wait cancellation, and cancel requests are unit tested. |
| Producer offset register/get/delete | Covered | Exact table/bucket offsets, TTL, readback, and deletion cleanup are checked with a unique producer ID. |
| Latest KV snapshots and metadata | Covered | The suite enables one-second snapshots and verifies availability, IDs, log offsets, and immutable file metadata for buckets containing data. Empty buckets may correctly report no snapshot. |
| Snapshot lease acquire/renew/release/drop | Covered | A unique lease protects every available snapshot, releases one bucket, and drops the remainder with bounded fallback cleanup. |
| Filesystem security token | Covered | Direct and managed-refresh paths verify expiry, redaction, replacement, and cleanup without logging token bytes. |
| Lake snapshot | Expected environment error | The stock image has no configured lake catalog. The request is sent and must return a classified storage or validation error; success would fail the suite. |
| Per-bucket table statistics | Covered | Input order, bucket identity, non-negative row counts, and every partial error slot are checked. |

## Review rule

A new public client method must add deterministic unit coverage and update this
matrix. Add live coverage when the stock pinned cluster can execute the method
without external credentials or persistent operational side effects. When it
cannot, record the exact missing service, unstable identifier, or state-change
risk instead of silently skipping it.
