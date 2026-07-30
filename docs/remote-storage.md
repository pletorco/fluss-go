# Remote storage adapters

`fluss-go` keeps filesystem SDKs optional. Configure `fgo.WithRemoteFileReader` to read remote log
segments advertised by Fluss 0.9.1. Each request receives a cloned
`FileSystemSecurityToken`; implementations must not include token bytes in errors or logs.

`fgo.LocalRemoteFileReader` supports local paths and `file://` URIs. HDFS, S3, OSS, and other
filesystems implement the same `RemoteFileReader` interface in an application or a separate adapter
module.

Snapshot scans compose the transport with metadata and format adapters:

```go
provider, err := fgo.NewRemoteSnapshotBatchProvider(
    reader,
    fgo.RemoteFileReadConfig{},
    snapshotMetadataResolver,
    snapshotFormatDecoder,
    currentToken,
    metrics,
)
if err != nil {
    return err
}

client, err := fgo.Open(
    ctx,
    fgo.WithSeedBrokers("coordinator:9123"),
    fgo.WithSnapshotBatchProvider(provider),
)
```

The integration contract is covered by `TestRemoteAndLocalLogPayloadsMergeWithoutGaps`,
`TestRemoteSnapshotBatchProviderDownloadsAndDecodes`, and the opt-in Fluss 0.9.1 suite. A
filesystem adapter should additionally run those tests against its lake-enabled environment.
