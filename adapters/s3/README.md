# S3 remote-file adapter

This package adapts the official AWS SDK for Go v2 S3 client to
`fgo.RemoteFileReader` and `fgo.RemoteFileStreamReader`. Core `pkg/fgo` does
not import the SDK; applications compile this package only when they need S3.

This adapter is a separately versioned Go module. Install the adapter version
that matches the root client release:

```sh
go get github.com/pletorco/fluss-go/adapters/s3@v0.1.0-beta.7
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

The initial reviewed version is `github.com/aws/aws-sdk-go-v2/service/s3`
v1.106.2, released on 2026-07-29 with Go 1.24 support and the Apache-2.0
license. It brings the official AWS SDK core, Smithy runtime, event-stream,
endpoint, checksum, signing, and S3 internal modules. `task security` checks
the resulting graph with `govulncheck` and Trivy.

The SDK is preferred over custom S3 signing, endpoint, retry, checksum, and
HTTP protocol code. Dependency version, maintenance, license, CVEs, and
transitive modules must be reviewed on every upgrade.

## Opt-in service test

`task test:s3` reads an existing object and is skipped unless
`FLUSS_GO_S3_TEST_URI` and `FLUSS_GO_S3_TEST_SIZE` are set. Optional variables
are `FLUSS_GO_S3_ENDPOINT`, `FLUSS_GO_S3_TEST_SHA256`, `AWS_REGION`,
`AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`. A custom endpoint uses
path-style addressing, which supports MinIO and similar test services.
