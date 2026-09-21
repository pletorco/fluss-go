# Fluss 1.0 live evidence

This matrix records which public client workflows are exercised against the
pinned Apache Fluss `1.0.0` image. `task test:integration` creates
an isolated cluster, bounds every operation, and removes all volumes. Unit and
golden coverage remains required even when live evidence exists.

## Transport and authentication

| Public surface | Live evidence | Boundary |
| --- | --- | --- |
| Plaintext and SASL PLAIN | Covered | Negotiation, credential rejection, ACL enforcement, connection reuse, cancellation isolation, and routed tablet requests run against native Fluss 1.0 listeners. |
| TLS-terminated plaintext | Covered | Digest-pinned HAProxy terminates TLS for the coordinator and all three advertised tablets. Verified admin, append, scan, upsert, and lookup operations prove metadata routing cannot bypass TLS. Fluss 1.0 itself has no native TLS listener. |
| TLS-terminated SASL PLAIN | Covered | Authentication, table administration, append, and scan run through TLS termination in front of native Fluss SASL PLAIN listeners. |
| TLS failures and cancellation | Covered | Ephemeral PKI checks preserve unknown-authority, hostname, expiry, record-header, plaintext-to-TLS, and causal cancellation errors through client wrapping and shutdown. |

## Reliability profiles

| Profile surface | Live evidence | Boundary |
| --- | --- | --- |
| PR smoke | Required | Seeded log and KV data plus fairly bounded log, KV, lookup, and scan workers; delayed and truncated bootstrap connections; cancellation bursts; full final correctness; and retained heap/goroutine bounds. |
| Load and soak | Scheduled/manual | Dedicated log, KV, lookup, scan, mixed, and repeated lifecycle profiles accept explicit duration, seed, operation, and worker bounds. |
| Fault injection | Scheduled/manual | A mixed workload injects delayed connections, an identity-preserving truncated read, cancellation bursts, and a real tablet restart. Classified transient and ambiguous outcomes are counted while every acknowledged row remains mandatory. |
| Reports | Covered | Credential-free JSON records operation counts, latency percentiles, throughput, expected errors, final correctness, fault names, and retained resources as Actions artifacts. See [reliability testing](reliability-testing.md). |

## Data APIs

| Public surface | Live evidence | Boundary |
| --- | --- | --- |
| Append writer, row and Arrow append, log scanner | Covered | Appends, explicit formats, bounded scans, schema evolution, acknowledged offsets, and post-leader-failure order are checked. |
| upsert writer, delete, partial upsert, merge modes | Covered | Full upsert/delete, full-schema partial-update payloads, preserved nullable fields, `FIRST_ROW`, overwrite bypass, and post-failure updates are checked. |
| Point and prefix lookup | Covered | Found, missing, deleted, insert-if-missing, concurrent, typed, and post-failure results are checked. |
| Current-state batch scanner | Covered | Row and typed scanners cover every resolved bucket and projections through Fluss 1.0 `SCAN_KV` sessions, including empty-bucket terminal responses. Sequence, changed/missing scanner ID, explicit close, and byte bounds have deterministic unit coverage. |
| Typed log and upsert writers, log scanner, lookup, batch scanner | Covered | Explicit codecs round-trip application structs through the real row protocol. |
| Snapshot batch scanner and typed snapshot scanner | Provider-only | Fluss returns snapshot object metadata, but decoding RocksDB snapshot objects requires an application-selected storage reader and decoder. Their orchestration and ownership are unit tested; the stock suite has no portable decoder to substitute safely. |
| Remote log and snapshot readers | Adapter/service-specific | Core scheduling is unit, race, and benchmark tested. A scheduled/manual workflow runs S3 against digest-pinned MinIO and the application-owned HDFS boundary against digest-pinned Apache Hadoop. Managed S3, mounted HDFS, and Alibaba Cloud OSS remain explicit read-only opt-ins; credentials are unavailable to pull requests. |

## Administration APIs

| Public surface | Live evidence | Boundary |
| --- | --- | --- |
| Database, table, partition, schema, and offset methods | Covered | Create, describe, list, exists, alter database, alter table, drop, dynamic partitions, historical schema, and earliest/latest offsets are checked with cleanup. Bucket-count request encoding and metadata routing are unit tested; live rescaling is not mutated because it changes the topology used by later failover assertions. |
| Cluster health | Covered | Aggregate replica, in-sync replica, leader, active-leader, and status bounds are checked against the live three-tablet cluster. |
| Remote-log manifests | Covered | The live stock cluster accepts table-scoped manifest listing. The result may be empty because the suite does not wait for tiering; response mapping is unit tested with populated entries. |
| Active KV snapshot listing | Covered | Table and partition identities are checked live; preservation of multiple snapshots for one bucket is covered deterministically by unit tests. |
| ACL methods | Covered | Create, list, authorization denial, and drop run on the isolated SASL cluster. |
| `DescribeClusterConfigs` | Covered | Effective stock-cluster values and non-empty metadata are checked. |
| `AlterClusterConfigs` | Deliberately not mutated | Fluss 1.0 does not expose a guaranteed dynamic, reversible no-op key. Request encoding, validation, and server errors remain unit tested to avoid leaving cluster-wide state ambiguous. |
| `GetServerNodes` | Covered | Coordinator and all three tablet roles are checked before and after coordinator recovery. |
| Server tag add/remove | Deliberately not mutated | The valid Fluss 1.0 tags are operational `PERMANENT_OFFLINE` and `TEMPORARY_OFFLINE`; applying either changes tablet eligibility and invalidates the same suite's failover topology. Enum validation, request bytes, and cleanup behavior are unit tested. |
| Rebalance start/progress/wait/cancel | Deliberately not started | Goal IDs are raw, unnamed Fluss 1.0 values and a rebalance changes placement used by deterministic failover assertions. Validation, response conversion, wait cancellation, and cancel requests are unit tested. |
| Producer offset register/get/delete | Covered | Exact table/bucket offsets, TTL, readback, and deletion cleanup are checked with a unique producer ID. |
| Latest KV snapshots and metadata | Covered | The suite enables one-second snapshots and verifies availability, IDs, log offsets, and immutable file metadata for buckets containing data. Empty buckets may correctly report no snapshot. |
| Snapshot lease acquire/renew/release/drop | Covered | A unique lease protects every available snapshot, releases one bucket, and drops the remainder with bounded fallback cleanup. |
| Filesystem security token | Covered | Direct and managed-refresh paths verify expiry, redaction, replacement, and cleanup without logging token bytes. |
| Lake snapshot | Wire contract covered; stock environment error expected | The response model and omitted-partition sentinel are verified against Fluss 1.0. The stock image has no configured lake catalog, so live requests must return a classified storage or validation error. Native Iceberg, Lance, and Paimon providers are deliberately not bundled; see [native lake formats](lake-formats.md). |

## Explicitly deferred Fluss 1.0 capabilities

Arrow predicate pushdown is not exposed in this release; callers must apply
all predicates to returned data. Historical physical-partition routing for
lake workflows is also not exposed. Neither capability receives a hidden
fallback or a feature-parity claim.
| Per-bucket table statistics | Covered | Input order, bucket identity, non-negative row counts, and every partial error slot are checked. |

## Review rule

A new public client method must add deterministic unit coverage and update this
matrix. Add live coverage when the stock pinned cluster can execute the method
without external credentials or persistent operational side effects. When it
cannot, record the exact missing service, unstable identifier, or state-change
risk instead of silently skipping it.
