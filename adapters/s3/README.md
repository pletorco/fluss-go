# S3 remote-file adapter

This package adapts the official AWS SDK for Go v2 S3 client to
`fgo.RemoteFileReader` and `fgo.RemoteFileStreamReader`. Core `pkg/fgo` does
not import the SDK; applications compile this package only when they need S3.

This adapter is a separately versioned Go module. Install the adapter version
that matches the root client release:

```sh
go get github.com/pletorco/fluss-go/adapters/s3@v0.2.0-beta.4
```

Create an `aws.Config` with the official `config.LoadDefaultConfig` credential
chain, then pass it to `s3.NewFromConfig`. The reader accepts only
`s3://bucket/key` URIs. Custom S3-compatible endpoints should be configured on
the official client with `BaseEndpoint` and `UsePathStyle`, following the AWS
SDK endpoint guidance.

The adapter:

- issues one ranged `GetObject` for each requested stream;
- validates `Content-Length`, `Content-Range`, and the advertised full size;
- returns the SDK response body without copying and requires the caller to
  close it;
- preserves SDK and body errors for `errors.Is` and `errors.As`;
- uses the context supplied to `GetObject`;
- never formats credentials, tokens, request headers, or signed URLs.

Credentials come from `aws.Config`. The adapter does not parse opaque Fluss
filesystem-token bytes into AWS credentials. SDK retry behavior is configured
on the official S3 client. Core remote-read retries remain limited to errors
whose types report temporary or timeout behavior, so SDK service errors are not
reclassified by this package.

## Dependency review

The initial reviewed `github.com/aws/aws-sdk-go-v2/service/s3` version was
v1.106.2. The current reviewed pins are AWS SDK core v1.48.0 and S3 v1.114.10;
this adapter release requires Go 1.26.11 and uses the Apache-2.0 license. The
dependencies bring the official AWS SDK core, Smithy runtime, event-stream,
endpoint, checksum, signing, and S3 internal modules. `task security` checks
the resulting graph with `govulncheck` and Trivy.

The SDK is preferred over custom S3 signing, endpoint, retry, checksum, and
HTTP protocol code. Dependency version, maintenance, license, CVEs, and
transitive modules must be reviewed on every upgrade.

## Service tests

`task test:s3` starts the official RustFS `1.0.6` image pinned by digest, creates an
ephemeral bucket through the official AWS SDK, and verifies exact ranges,
metadata mismatch rejection, cancellation, close behavior, service-error
identity, retry classification, and credential redaction. The runner removes
the bucket, container, network, and tmpfs-backed data after every run. It needs
Docker and Docker Compose but no cloud credentials.

The image is `rustfs/rustfs:1.0.6` at index digest
`sha256:ee717072290bec9b11abfc0365f8310b00df1cd9b3639943edae47bdef05044a`,
published by [RustFS upstream](https://github.com/rustfs/rustfs/releases/tag/1.0.6)
under Apache-2.0. It replaces the withdrawn public MinIO image. The fixture
runs as UID/GID `10001:10001`, uses a writable owned tmpfs, disables the console,
binds only to loopback, and uses an ephemeral secret. RustFS is test-only:
it is not linked into or distributed with any Go module and is not the Fluss
Rust client core. This suite proves the adapter behavior above, not complete
Amazon S3 feature parity or production RustFS capacity. The container choice
and alternatives are recorded in the [build-vs-buy register](../../docs/build-vs-buy.md).

`task test:s3:managed` reads an existing application-managed object and is
skipped unless
`FLUSS_GO_S3_TEST_URI` and `FLUSS_GO_S3_TEST_SIZE` are set. Optional variables
are `FLUSS_GO_S3_ENDPOINT`, `FLUSS_GO_S3_TEST_SHA256`, `AWS_REGION`,
`AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`. It never writes or deletes
managed data. Emulator evidence and managed-service evidence are reported
separately.
