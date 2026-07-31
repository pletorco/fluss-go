# Advanced administration

Package `fadm` exposes the Apache Fluss 0.9.1-incubating coordinator APIs that
are outside the catalog and ACL workflows. Construct one `fadm.Client` from the
application's shared `fgo.Client`; the admin client does not own or close the
underlying connections.

The APIs in this guide use server-assigned IDs and Fluss 0.9.1 protocol status
codes. Do not persist inferred meanings for raw numeric codes across Fluss
versions. Resolve logical paths again after catalog changes and use the IDs
returned by the current cluster.

## Cluster configuration

`DescribeClusterConfigs` returns the effective value and source for each
cluster setting. `AlterClusterConfigs` submits one coordinator request
containing `ConfigSet`, `ConfigDelete`, `ConfigAppend`, or `ConfigSubtract`
changes.

Treat the operation as a cluster-wide mutation:

- Read the current values before constructing a change.
- Pass a value for set, append, and subtract operations. Use a nil value only
  for delete.
- Re-read the configuration after success instead of assuming that a submitted
  value is already effective on every server.
- Do not automatically retry an ambiguous mutation failure. Reconcile by
  describing the configuration first.

See the compile-checked
[`Client.AlterClusterConfigs` example](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fadm#example-Client.AlterClusterConfigs).

## Server tags

`ServerNodes` returns the coordinator followed by alive tablet servers.
`AddServerTag` and `RemoveServerTag` accept server IDs from that current
snapshot and a non-negative tag.

Server membership can change between discovery and mutation. Refresh the node
list after a metadata or not-found failure. When a tag is temporary, remove it
with a bounded cleanup context and report cleanup failures; silently leaving a
placement tag behind can affect later scheduling.

## Rebalance lifecycle

`StartRebalance` requires one or more Fluss 0.9.1 goal IDs and returns a
server-assigned rebalance ID. Preserve that ID for progress queries and
cancellation. Goal IDs and status values are raw protocol codes because Fluss
0.9.1 does not expose a stable named enum through this client.

`RebalanceProgress` performs one query. `WaitRebalance` polls at the requested
positive interval until the top-level status is not zero, where zero means
running. A nonzero status is terminal but is not necessarily success; inspect
the top-level and per-bucket status codes according to the target cluster's
Fluss 0.9.1 operational definitions.

Canceling the wait context stops only local polling. It does not cancel the
server operation. On timeout, shutdown, or another abandoned wait, call
`CancelRebalance` with a fresh bounded context and retain the rebalance ID for
later reconciliation if cancellation cannot be confirmed.

See the compile-checked
[`Client.WaitRebalance` example](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fadm#example-Client.WaitRebalance).

## Producer offsets

Producer-offset registrations protect application progress independently of a
writer session. A registration contains:

- a non-empty application-defined producer ID;
- one or more current physical table IDs;
- bucket offsets with `PartitionID == -1` for unpartitioned tables; and
- a positive TTL controlling server-side expiration.

`RegisterProducerOffsets` creates or replaces the registration. Its boolean
result is true only when the Fluss protocol result code is zero. A nil error
with false must still be handled as a rejected registration. Read the
registration back with `GetProducerOffsets` and inspect `ExpiresAt` when
confirmation matters.

Use a producer ID that is unique to the durable progress owner. Refresh the
registration before its TTL expires, and call `DeleteProducerOffsets` when the
owner is permanently retired. After an ambiguous register or delete failure,
query the ID before mutating it again.

See the compile-checked
[`Client.RegisterProducerOffsets` example](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fadm#example-Client.RegisterProducerOffsets).

## KV snapshots and leases

`LatestKVSnapshots` resolves a logical table path and optional partition name
to current physical IDs and one result per bucket. `KVSnapshot.Available`
distinguishes no snapshot from a valid snapshot whose ID is zero.
`KVSnapshotMetadata` then returns the immutable files and inclusive log offset
for a specific table, partition, bucket, and snapshot ID.

Snapshot IDs are scoped by their table, partition, and bucket. A
`PartitionID` of -1 identifies an unpartitioned table; all other IDs and bucket
numbers must be non-negative.

`AcquireKVSnapshotLease` requires an application-unique lease ID, a positive
duration, and at least one snapshot. Its returned slice contains snapshots
that were **unavailable and therefore not leased**. An empty slice means every
requested snapshot was leased. With a mixed result, preserve successful
leases, exclude every unavailable snapshot from reads, and still clean up the
lease.

Use `RenewKVSnapshotLease` before the lease duration expires when snapshot
processing continues. Renewal sends the existing lease ID and a new positive
duration without reacquiring individual snapshots. Use
`ReleaseKVSnapshotLease` to release selected table buckets early.
`DropKVSnapshotLease` releases everything held by the lease ID. Always arrange
a bounded drop after a successful acquire attempt; server-side expiration is a
last resort rather than the normal cleanup path.

See the compile-checked
[`Client.AcquireKVSnapshotLease` example](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fadm#example-Client.AcquireKVSnapshotLease).

## Filesystem credentials

`FileSystemSecurityToken` obtains temporary filesystem credentials directly
through the admin client. The returned value clones token bytes and redacts
them from `String` and `GoString`, but callers still own sensitive data:

- never log, format, or persist `Token`;
- honor `ExpiresAt`;
- replace credentials atomically in downstream filesystem clients; and
- clear superseded byte slices when the application no longer needs them.

Applications that continuously read remote logs or snapshots should normally
use the client-managed refresh workflow described in
[Remote storage adapters](remote-storage.md) instead of polling this admin
method themselves.

## Lake snapshots

`LakeSnapshot` accepts a logical table path, an optional snapshot ID, and a
readability selector. A nil ID asks the server to select a snapshot; a
non-negative ID requests that exact snapshot. Set `readable` when the caller
needs a snapshot suitable for reading rather than merely querying known state.

The response carries the server-selected table and snapshot IDs plus
partition, bucket, and log-offset entries. As with KV snapshots,
`PartitionID == -1` means the table is unpartitioned. Treat the returned
physical IDs as a coherent point-in-time set and do not combine bucket entries
from separate responses without an application-level consistency policy.

## Per-bucket table statistics

`TableStats` returns exactly one `TableStats` value per requested bucket and
keeps the input order. The method has no aggregate error because routing,
transport, validation, and server failures are recorded independently in each
result's `Err`.

Never stop at the first bucket failure. Preserve every successful `RowCount`,
record every bucket-local error, and decide at the application boundary
whether a partial result is useful. A complete operational snapshot requires
all results to have nil errors.

See the compile-checked
[`Client.TableStats` example](https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fadm#example-Client.TableStats).

## Cancellation and shutdown

All admin calls honor their context for local waiting and transport work.
Context cancellation does not prove that a mutation was rejected by the
server. For configuration changes, tags, registrations, leases, and
rebalances, use a fresh bounded context to query or clean up after an ambiguous
result.

Stop admin workflows before closing the shared `fgo.Client`. After `Close`,
new requests fail with `fgo.ErrClosed`.
