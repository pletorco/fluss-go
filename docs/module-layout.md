# Go module layout

The repository is a Go workspace containing one core module and four optional
adapter modules. The split keeps cloud and observability SDKs out of the module
graph of applications that use only the Fluss client.

| Directory | Module | Dependency boundary |
| --- | --- | --- |
| `/` | `github.com/pletorco/fluss-go` | Protocol, transport, `fgo`, `fadm`, Arrow, and repository tooling. |
| `adapters/s3` | `github.com/pletorco/fluss-go/adapters/s3` | Official AWS SDK v2. |
| `adapters/oss` | `github.com/pletorco/fluss-go/adapters/oss` | Official Alibaba Cloud OSS SDK v2. |
| `adapters/hdfs` | `github.com/pletorco/fluss-go/adapters/hdfs` | Application-owned HDFS client boundary; no selected HDFS SDK. |
| `adapters/otel` | `github.com/pletorco/fluss-go/adapters/otel` | Stable OpenTelemetry metrics API. |

No native Iceberg, Lance, or Paimon module is published. The format-engine
review and conditions for adding a separately versioned optional module are
recorded in [native lake formats](lake-formats.md).

The checked-in `go.work` is for repository development. Each module has its
own `go.mod`, `go.sum`, Dependabot entry, vulnerability scan, tests, race run,
coverage profile, API baseline, and prefixed release tag. Root patterns such as
`go test ./...` intentionally stop at nested module boundaries, so project
tasks enumerate every module rather than relying on that pattern alone.

Adapter modules contain a relative `replace` for the root module so one branch
can test changes atomically. Replacement directives in dependency modules are
ignored by downstream main modules. Published adapters therefore require a
root release that already excludes the old in-root adapter packages. The first
split release is beta.7; later adapters may retain an older compatible minimum
root version but should normally be released with the same version number.

## Dependency impact

At the beta.7 split, `GOWORK=off go list -m all` reports 78 modules for the
root, down from 97 before the split. The adapter graphs are 78 for HDFS, 80 for
OSS, 84 for OpenTelemetry, and 89 for S3 with their local root replacement.
Module graph size is not binary size, but separating requirements also narrows
license, update, and vulnerability review for core-only consumers.

Apache Arrow remains a root dependency. Fluss 0.9.1 uses Arrow record batches,
and exported `fgo` APIs intentionally accept and return Arrow types with
explicit ownership. Moving those APIs now would break the data client while
leaving row/log decoding dependent on Arrow IPC and compression. This is an
approved core tradeoff, not an optional-dependency claim, and must be reviewed
again before v1 if the public Arrow boundary changes.

## Verification and release

Use `task ci` for workspace-wide format, API, unit, race, coverage, fuzz,
vulnerability, and license checks. Direct module verification is available
with `GOWORK=off go -C <module> mod verify`; local replacements deliberately
resolve to the checked-out root.

Nested modules use path-prefixed Git tags such as
`adapters/s3/v0.1.0-beta.7`. The root tag must be published and visible through
the Go proxy before adapter tags whose `go.mod` requires that root version.
The complete sequence and proxy checks are maintained in the release guide.
