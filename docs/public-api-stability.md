# Public API stability

The exported Go API remains experimental before v1, but every change is
reviewed against the latest approved baseline. Experimental means that a
breaking change may be accepted with migration guidance; it does not mean that
exports may change accidentally.

## Owned surfaces

| Surface | Ownership and stability |
| --- | --- |
| `pkg/fgo` | Application data API, configuration, lifecycle, errors, extension interfaces, and Arrow integration. Review all changes for source compatibility and runtime behavior. |
| `pkg/fadm` | Administrative API over a shared `fgo.Client`. Review names, partial results, identifiers, and server-side side effects. |
| `pkg/fmsg` | Pinned Fluss 0.9.1 wire messages, API metadata, raw request escape hatch, and protocol errors. Generated protobuf changes follow upstream inputs and are reviewed separately from client ergonomics. |
| `adapters/*` | Separately versioned optional modules. Their API baselines, dependencies, and prefixed tags are reviewed independently. |
| `internal/*` and `cmd/*` | Not supported as application APIs. Repository tooling commands may change with the development workflow. |

The intended extension points are `fmsg.Requester`, `fgo.Codec`,
`fgo.KeyCodec`, authentication and metrics interfaces, remote-file interfaces,
filesystem-token providers and receivers, and snapshot resolver/decoder
interfaces. Concrete client, writer, scanner, lookup, result, schema, and row
types are user-facing APIs rather than implementation extension points.

Generated `fmsg` types are public because raw protocol access is supported.
They are not hand-edited, and their names and fields follow the pinned
`FlussApi.proto`. `task verify:generate` proves source reproducibility while the
separate protocol API baseline makes generated surface changes explicit.

## Fluss terminology migration

The post-beta.9 API intentionally removes pre-RC names that came from Kafka,
implementation details, or generic local terminology. No deprecated aliases
are retained. The following table is the migration guide for applications
upgrading from beta.9:

| beta.9 API | Fluss-aligned API |
| --- | --- |
| `fgo.WithSeedBrokers` | `fgo.WithBootstrapServers` |
| `fgo.WithClientIdentity` | `fgo.WithClientSoftware` |
| `fgo.WithDialTimeout` | `fgo.WithConnectTimeout` |
| `fgo.Client.OpenTable` | `fgo.Client.GetTable` |
| `fgo.LogWriter`, `NewLogWriter`, `LogWriterConfig`, `LogWriterOption` | `fgo.AppendWriter`, `NewAppendWriter`, `AppendWriterConfig`, `AppendWriterOption` |
| `fgo.WithLog*` writer options | corresponding `fgo.WithAppend*` options |
| `fgo.WithAppendLinger` | `fgo.WithAppendBatchTimeout` |
| `fgo.BucketAssignment`, `Assignment*`, `WithAppendBucketAssignment` | `fgo.NoKeyAssigner`, `NoKeyAssigner*`, `WithAppendNoKeyAssigner` |
| `fgo.LogWriteFormat*` | `fgo.LogFormat*` |
| `fgo.ArrowCompression` | `fgo.ArrowCompressionType` |
| `fgo.TypedLogWriter`, `NewTypedLogWriter` | `fgo.TypedAppendWriter`, `NewTypedAppendWriter` |
| `fgo.KVWriter`, `NewKVWriter`, `KVWriterConfig`, `KVWriterOption` | `fgo.UpsertWriter`, `NewUpsertWriter`, `UpsertWriterConfig`, `UpsertWriterOption` |
| `fgo.WithKV*` writer options | corresponding `fgo.WithUpsert*` options |
| `fgo.WithUpsertLinger` | `fgo.WithUpsertBatchTimeout` |
| `fgo.TypedKVWriter`, `NewTypedKVWriter` | `fgo.TypedUpsertWriter`, `NewTypedUpsertWriter` |
| `fgo.LookupClient`, `NewLookupClient` | `fgo.Lookuper`, `NewLookuper` |
| `fgo.TypedLookupClient`, `NewTypedLookupClient` | `fgo.TypedLookuper`, `NewTypedLookuper` |
| `fgo.WithLookupBatch`, `WithLookupTimeout` | `fgo.WithLookupBatchLimits`, `WithLookupRequestTimeout` |
| `fgo.WithScanLimits` | `fgo.WithLogFetchLimits` |
| generic writer, lookup, scanner, and token config fields | Fluss `BatchTimeout`, `RequestTimeout`, `NoKeyAssigner`, `Fetch*`, `MaxInFlightRequests`, and `Renewal*` fields |
| `fgo.Node`, `Node.Role` | `fgo.ServerNode`, `ServerNode.ServerType` |
| `fgo.ServerRole`, `UnknownServerRole`, `ErrServerRole` | `fgo.ServerType`, `UnknownServerType`, `ErrServerType` |
| `fgo.MetricEvent.ServerRole`, metric key `fluss.server.role` | `fgo.MetricEvent.ServerType`, metric key `fluss.server.type` |
| `fgo.PlainAuthenticator` | `fgo.SASLPlainAuthenticator` |
| `fadm.Database`, `DatabaseDefinition` | `fadm.DatabaseInfo`, `DatabaseDescriptor` |
| `fadm.TableDefinition`, `Partition` | `fadm.TableDescriptor`, `PartitionInfo` |
| `fadm.ConfigChange`, `ConfigOp`, `ClusterConfig` | `fadm.AlterConfig`, `AlterConfigOpType`, `ConfigEntry` |
| `fadm.ACL`, `ACLFilter`, `ACLResult.ACL` | `fadm.ACLBinding`, `ACLBindingFilter`, `CreateACLResult.Binding` |
| `fadm.LatestKVSnapshot`, `SnapshotLease` | `fadm.KVSnapshots`, `KVSnapshotLease` |
| `ListDatabases`, `DescribeDatabase` | `ListDatabaseSummaries`, `GetDatabaseInfo` |
| `DescribeTable`, `TableSchema`, `ListPartitions` | `GetTableInfo`, `GetTableSchema`, `ListPartitionInfos` |
| `ServerNodes`, `FileSystemSecurityToken`, `TableStats` | `GetServerNodes`, `GetFileSystemSecurityToken`, `GetTableStats` |
| `LatestKVSnapshots`, `KVSnapshotMetadata` | `GetLatestKVSnapshots`, `GetKVSnapshotMetadata` |
| `StartRebalance`, `RebalanceProgress` | `Rebalance`, `ListRebalanceProgress` |
| combined `LakeSnapshot` method | `GetLatestLakeSnapshot`, `GetLakeSnapshot`, or `GetReadableLakeSnapshot` |

Method behavior, wire messages, cancellation, ownership, retries, and lifecycle
semantics are unchanged except that the combined lake-snapshot selector is now
split into the three operations exposed by the Fluss Admin API.

## Compatibility gate

`task api:check` compares all three public packages with binary export-data
baselines under `api/baselines` by using the pinned Go `apidiff` command. The
gate rejects compatible additions as well as incompatible changes so every new
export receives the same naming, documentation, ownership, and dependency
review.

After an approved change, run `task api:update` and inspect the source diff and
`apidiff` output before committing the new baseline. A baseline update alone is
not approval. User-visible changes also require tests, package documentation,
examples where useful, a changelog entry, and migration guidance for renamed,
removed, or behaviorally changed APIs.

Before RC, review the complete surface for accidental exports and settle any
Arrow package changes. Adapter modules are already isolated from the root
module graph and use the same release version with path-prefixed Git tags.
After RC, removal or incompatible
signature changes require an explicit exception for a security or correctness
defect. The v1 policy will replace this prerelease policy before a stable tag.
