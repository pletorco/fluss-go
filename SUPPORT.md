# Support

fluss-go is maintained as an open source prerelease project. Community support
is provided through GitHub on a best-effort basis and does not include a
response-time or production-availability SLA.

## Supported questions

The issue tracker accepts reproducible questions and defects involving:

- the latest tagged fluss-go beta;
- Apache Fluss `0.9.1-incubating` at the pinned compatibility target;
- supported Go versions from the README compatibility matrix;
- the public `fmsg`, `fgo`, and `fadm` packages; and
- the separately versioned HDFS, OSS, OpenTelemetry, and S3 adapters.

Use the [issue chooser](https://github.com/pletorco/fluss-go/issues/new/choose)
and select the form that best matches the request. Search existing issues
first, and keep one independently actionable problem per issue.

## Information to include

For usage questions and bug reports, provide:

- fluss-go, Apache Fluss, and Go versions;
- the package or adapter involved;
- operating system and relevant deployment topology;
- a minimal reproducer or the smallest failing request sequence;
- expected and actual behavior; and
- redacted logs or protocol errors when they are necessary.

Never include passwords, access keys, security tokens, private keys, connection
strings containing credentials, or production records. Follow
[SECURITY.md](SECURITY.md) when the behavior may expose confidentiality,
integrity, authentication, or availability.

## Project boundaries

The following requests may be redirected or closed unless accompanied by a
scoped compatibility proposal and evidence:

- support for an Apache Fluss release outside the documented matrix;
- debugging an application without a minimal reproducer;
- operating or tuning an Apache Fluss cluster;
- implementing application-owned snapshot decoders or lake formats;
- selecting credentials, authorization policy, or an HDFS client for an
  application; and
- guarantees for untagged commits or modified forks.

Feature proposals should explain the user problem, required Fluss protocol
surface, alternatives considered, compatibility impact, and why an existing Go
library or extension boundary cannot satisfy the need.

## Other project requests

- Contribution workflow: [CONTRIBUTING.md](CONTRIBUTING.md)
- Security reports: [SECURITY.md](SECURITY.md)
- Expected community behavior: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- Supported functionality: [README feature matrix](README.md#fluss-091-feature-matrix)
- Error handling and recovery: [docs/error-handling.md](docs/error-handling.md)
