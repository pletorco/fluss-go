# Alibaba Cloud OSS adapter

Package `adapters/oss` adapts the official Alibaba Cloud OSS SDK for Go v2 to
`fgo.RemoteFileStreamReader`. Version `v1.5.3` is the initial reviewed pin. The
SDK is Apache-2.0 licensed, maintained by Alibaba Cloud, supports context-aware
operations, V4 signing, standard range requests, endpoint configuration, and
SDK-owned credentials and retries.

This adapter is a separately versioned Go module. Install the adapter version
that matches the root client release:

```sh
go get github.com/pletorco/fluss-go/adapters/oss@v0.1.0-beta.7
```

Create an `oss.Config` with `oss.LoadDefaultConfig`, pass it to
`oss.NewFromConfig`, and configure the resulting reader with
`fgo.WithRemoteFileReader`. The compiled `ExampleNewFromConfig` on pkg.go.dev
shows the constructor path.

The adapter accepts only `oss://bucket/key` paths. It sends an exact standard
range request, verifies `Content-Length` and `Content-Range`, preserves SDK
errors, and closes invalid response bodies. Applications own SDK credential,
endpoint, retry, and client lifecycle configuration. Fluss filesystem-token
bytes are deliberately not interpreted as OSS credentials.

## Live test

The opt-in test reads an existing object and never writes or deletes data.
Provide SDK credentials through the environment variables documented by the
official SDK, then set:

```sh
export FLUSS_OSS_LIVE=1
export FLUSS_OSS_REGION=cn-hangzhou
export FLUSS_OSS_BUCKET=example-bucket
export FLUSS_OSS_OBJECT=path/to/object
export FLUSS_OSS_EXPECTED_BASE64=ZXhhbXBsZQ==
# Optional for private-cloud or emulator endpoints:
export FLUSS_OSS_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com
task test:oss
```

The expected value is base64 encoded so binary fixtures are supported without
placing object data or credentials on a command line.
