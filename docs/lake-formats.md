# Native lake-format decision

This document records the Fluss `0.9.1-incubating` contract and the reviewed
no-implementation decision for native Iceberg, Lance, and Paimon reads in the
current fluss-go beta. It does not classify remote object transport or Fluss KV
snapshot orchestration as native lake-format support.

## Ownership layers

| Layer | Current owner | Support |
| --- | --- | --- |
| Remote bytes | `pkg/fgo` plus local, S3, OSS, or application-owned HDFS readers | Supported with bounded ranges, retries, concurrency, cancellation, and size validation. |
| Fluss KV snapshots | `RemoteSnapshotBatchProvider` plus an application resolver and decoder | Orchestration supported. RocksDB discovery and decoding remain application-owned. |
| Native lake tables | A format catalog, manifest planner, filesystem, data-file decoder, delete semantics, and Fluss row adapter | No built-in provider. Applications may integrate a reviewed engine outside fluss-go. |

These layers are not interchangeable. A storage reader can fetch a Parquet or
manifest object but cannot safely discover an Iceberg snapshot, apply Paimon
merge semantics, or interpret a Lance dataset. `RemoteSnapshotBatchProvider`
must not be used to recreate behavior already owned by a native format engine.

## Fluss 0.9.1 contract

`GET_LAKE_SNAPSHOT` returns a table ID, one external snapshot ID, and ordered
bucket entries containing partition ID, partition name, bucket ID, and the
included Fluss log offset. It does not return a catalog URI, namespace mapping,
manifest list, data-file path, object size, filesystem configuration, or
credentials.

The Java 0.9.1 Iceberg source treats the returned snapshot ID as an exact
Iceberg snapshot ID. It loads the logical `database.table` through the selected
catalog, calls the native snapshot planner, and reads native data files. Fluss
reserves these lake columns:

- `__offset`: original Fluss log offset;
- `__timestamp`: original Fluss record timestamp;
- `__bucket`: synthetic bucket partitioning when the table has no bucket key.

A future provider must load the authoritative catalog table, select the exact
snapshot, filter the requested physical partition and bucket, project requested
user columns plus required reserved columns, apply native delete and merge
semantics, and convert values according to the Fluss schema. Missing snapshots,
schema mismatches, cancellation, and close failures must retain their causes.

## Candidate review

The review was performed on 2026-07-31 against the Fluss 0.9.1 source at
`6bf969f71af8d6f9cc37383ab89ae46a58b0e227`.

### Iceberg

[`apache/iceberg-go` v0.6.0](https://github.com/apache/iceberg-go/releases/tag/v0.6.0)
is the only maintained native Go candidate. It is Apache-2.0 licensed and
provides exact-snapshot planning, catalog implementations, filesystem support,
Parquet decoding, delete handling, and Arrow record batches. Those facilities
must be reused if Iceberg support is added; fluss-go must not parse Iceberg
metadata, Avro manifests, Parquet files, or cloud credentials itself.

It is not added in this beta for the following measured reasons:

- its module requires Go 1.25.5 while fluss-go currently declares Go 1.25.0;
- a minimal program importing only `table` resolved 299 modules, and
  `go list -deps` reported 480 packages including the standard library;
- the current project Trivy allowlist rejects transitive
  `GNU-All-permissive-Copying-License`, `ISC`, and `MPL-2.0` findings;
- no lake-enabled Fluss 0.9.1 fixture currently proves tiered data written by
  Fluss can be planned, bucket-filtered, projected, and read end to end.

An optional module would protect core consumers but would still impose this
graph on every adapter consumer. License-policy expansion and a service-backed
Fluss/Iceberg fixture require separate approval before implementation.

### Lance

The maintained [`lance-format/lance`](https://github.com/lance-format/lance)
repository provides Rust, Python, and Java surfaces but no native Go reader.
Shipping Rust through cgo, invoking another runtime, or implementing Lance
metadata and data decoding locally would create a new platform and security
boundary. No Lance provider is approved.

### Paimon

[`apache/paimon`](https://github.com/apache/paimon) provides Java and Python
surfaces but no native Go reader. Paimon snapshot planning, merge-tree and
deletion-vector behavior, filesystem access, and row decoding must stay with a
maintained Paimon engine. No local parser or subprocess bridge is approved.

## Revisit gate

Native support can be reconsidered when all of the following are available:

1. a maintained Go API owns catalog, planning, filesystems, decoding, and
   format-specific delete or merge behavior;
2. its complete module graph passes the approved license and CVE gates without
   weakening existing policy;
3. it remains a separately versioned optional module and does not enter the
   root module graph;
4. a reproducible lake-enabled Fluss 0.9.1 environment writes the fixture read
   by the Go provider;
5. golden and service tests cover exact snapshot selection, partition and
   bucket filtering, projections, reserved columns, supported Fluss types,
   cancellation, cleanup, and malformed metadata.

Until then, `fadm.LakeSnapshot` exposes the server contract and applications
that already operate a reviewed native engine may provide their own
`SnapshotBatchProvider`. fluss-go does not claim native format parity.
