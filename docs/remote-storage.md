# Remote storage adapters

`fluss-go` keeps filesystem SDKs optional. Configure
`fgo.WithRemoteFileReader` to read remote log segments advertised by Fluss
0.9.1. When the client has a current filesystem security token, each request
receives a clone. Requests may have a nil token when refresh is disabled or no
valid token is available. Implementations must not include token bytes in
errors or logs.

`fgo.LocalRemoteFileReader` supports local paths and `file://` URIs. HDFS, S3,
OSS, and other filesystems implement the same `RemoteFileReader` interface in
an application or a separate adapter module.

## Client-managed tokens

Enable coordinator-backed token acquisition with
`WithFileSystemSecurityTokenRefresh`. A zero config applies the documented
defaults: refresh at 75% of token lifetime, retry after one minute with
exponential backoff capped at one hour, and treat a token as expiring 30
seconds early.

```go
receiver := fgo.FileSystemSecurityTokenReceiverFunc(
	func(token fgo.FileSystemSecurityToken) error {
		// Update an external filesystem client with the cloned token.
		return nil
	},
)

client, err := fgo.Open(
	ctx,
	fgo.WithSeedBrokers("coordinator:9123"),
	fgo.WithFileSystemSecurityTokenRefresh(
		fgo.FileSystemSecurityTokenRefreshConfig{},
		receiver,
	),
)
```

The default provider requests tokens through the client's negotiated
coordinator connection. Applications that obtain credentials elsewhere can
use `WithFileSystemSecurityTokenProvider` with the same refresh policy and
receiver contract.

Published tokens are cloned, and `String` and `GoString` always redact token
bytes. `CurrentFileSystemSecurityToken` does not return expired or revoked
tokens; refresh failures retain only a still-valid current token and are retried
with bounded backoff. Closing the client stops the refresh loop; the last cached
token remains readable only until it expires.

## Snapshot composition

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
	fgo.WithRemoteFileReader(reader, fgo.RemoteFileReadConfig{}),
	fgo.WithSnapshotBatchProvider(provider),
)
```

`RemoteSnapshotResolver` discovers files for one snapshot, while
`RemoteSnapshotDecoder` owns the storage-format-specific decoding step.
`RemoteFileReadConfig` bounds attempts, retry delay, and maximum object size.
The provider validates file metadata and exact downloaded sizes before passing
data to the decoder.

The integration contract is covered by
`TestRemoteAndLocalLogPayloadsMergeWithoutGaps`,
`TestRemoteSnapshotBatchProviderDownloadsAndDecodes`, and the opt-in Fluss
0.9.1 suite. A filesystem adapter should additionally run those tests against
its lake-enabled environment.
