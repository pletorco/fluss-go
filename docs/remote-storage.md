# Remote storage adapters

`fluss-go` keeps filesystem SDKs optional. Configure
`fgo.WithRemoteFileReader` to read remote log segments advertised by Fluss
0.9.1. When the client has a current filesystem security token, each request
receives a clone. Requests may have a nil token when refresh is disabled or no
valid token is available. Implementations must not include token bytes in
errors or logs.

`fgo.LocalRemoteFileReader` supports absolute local paths and local `file://`
URIs. Relative paths are rejected. It implements both the complete-object
`RemoteFileReader` compatibility API and the preferred
`RemoteFileStreamReader` range API. HDFS, S3, OSS, and other filesystems use
the same interfaces from an application or a separate adapter module.

## Client-managed tokens

Enable coordinator-backed token acquisition with
`WithFileSystemSecurityTokenRefresh`. A zero config applies the documented
defaults: refresh at 75% of token lifetime, retry after one minute with
exponential backoff capped at one hour, and treat a token as expiring 30
seconds early.

<!-- go-source: internal/docexamples/snippets_test.go tokenRefresh -->
```go
receiver := fgo.FileSystemSecurityTokenReceiverFunc(
	func(token fgo.FileSystemSecurityToken) error {
		// Update an external filesystem client with the cloned token.
		return nil
	},
)

client, err := fgo.Open(
	context.Background(),
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

Receivers run sequentially in registration order. They may call
`Client.Close`; shutdown cancels the refresh loop without waiting for the
currently executing receiver, which avoids reentrant shutdown deadlocks.
Because the receiver interface has no context, callback code itself cannot be
forcibly stopped and must return promptly. A blocked receiver delays
publication and later receivers, but does not prevent client shutdown.

## Snapshot composition

Snapshot scans compose the transport with metadata and format adapters:

<!-- go-source: internal/docexamples/snippets_test.go snapshotComposition -->
```go
provider, err := fgo.NewRemoteSnapshotBatchProvider(
	reader,
	fgo.RemoteFileReadConfig{},
	resolver,
	decoder,
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
`RemoteFileReadConfig` bounds attempts, retry delay, object size, aggregate
bytes, file count, active streams, and advertised bytes across active streams.
Defaults allow at most 256 MiB per object, 512 MiB per operation, 4096 objects,
four active streams, and 256 MiB across those streams. The provider validates
all advertised metadata with overflow-safe arithmetic before downloading any
object, then verifies exact downloaded sizes before passing data to the
decoder. Snapshot files may complete out of order, but decoder input order is
stable.

Every request includes `MaxBytes`, `ExpectedSize`, `Offset`, and `Length`.
Streaming adapters must validate the object metadata and return exactly the
requested range; the client also rejects short and overlong streams and always
closes the body. The built-in local reader uses `io.SectionReader` over an open
file and checks the actual file size before exposing the range.

Remote logs allocate only their final aggregate result buffer. Prefetch workers
write each range into a disjoint final slice, so a completed object is not
retained in a second `[]byte`. The first segment starts at
`FirstStartPosition`, avoiding download of its unused prefix. Snapshot decoders
still consume complete `RemoteSnapshotFile.Data` buffers, but their downloads
stream directly into those bounded final buffers and may overlap within the
concurrent byte budget.

Existing `RemoteFileReader` implementations continue to work. The compatibility
path downloads the complete object, validates it, and copies a requested range.
Adapters should add `OpenRemoteFile` to implement `RemoteFileStreamReader`;
the client discovers it automatically, so applications do not need to change
`WithRemoteFileReader`. Stream implementations must release SDK response bodies
from `Close`, including after cancellation or a partial read.

## Amazon S3 adapter

The optional [`adapters/s3`](../adapters/s3) package uses the official AWS SDK
for Go v2. It parses only `s3://bucket/key` locations, issues ranged
`GetObject` calls, validates S3 response lengths and content ranges, and
preserves SDK error identity. Core `pkg/fgo` does not import the AWS SDK.

Applications load credentials, region, retry mode, and optional endpoint into
an `aws.Config`, create the reader with `s3.NewFromConfig`, and pass it to
`WithRemoteFileReader`. Opaque Fluss filesystem-token bytes are not interpreted
as AWS credentials. See the [adapter manual](../adapters/s3/README.md) for the
reviewed SDK version, endpoint guidance, and opt-in service-test variables.

## Alibaba Cloud OSS adapter

The optional [`adapters/oss`](../adapters/oss) package uses the official
Alibaba Cloud OSS SDK for Go v2. It accepts `oss://bucket/key` locations,
requests standard exact byte ranges, validates returned lengths and content
ranges, and preserves SDK error identity. Applications own region, endpoint,
credential, retry, and SDK client configuration. Opaque Fluss filesystem-token
bytes are not interpreted as OSS credentials. See the
[adapter manual](../adapters/oss/README.md) for the reviewed SDK version and
opt-in service-test variables.

HDFS remains separate adapter work because no maintained Go client currently
combines context-aware reads, Kerberos, and Hadoop delegation-token injection.

Retries are limited to truncated reads and errors whose type explicitly reports
temporary or timeout behavior. Authentication, permission, not-found,
configuration, validation, and unsupported-operation errors fail immediately.
Filesystem adapters should preserve or wrap their SDK's retry classification
instead of returning an untyped generic error.

The integration contract is covered by
`TestRemoteAndLocalLogPayloadsMergeWithoutGaps`,
`TestRemoteSnapshotBatchProviderDownloadsAndDecodes`, and the opt-in Fluss
0.9.1 suite. A filesystem adapter should additionally run those tests against
its lake-enabled environment.

## Prefetch measurement

Run the in-memory and slow-stream comparisons with:

```sh
go test -run '^$' -bench 'BenchmarkReadRemoteLog(Segments|Streaming)$' -benchmem ./pkg/fgo
```

On Linux/amd64 with Go 1.26.5 and eight 1 KiB objects whose first read takes
100 microseconds, one stream at a time took 8.58 ms/op while four-stream
prefetch took 1.94 ms/op. Both retained about 15 KiB/op. The benchmark is
synthetic evidence for bounded overlap, not a storage-service throughput
guarantee.
