# fluss-go

Experimental Go client for Apache Fluss 0.9.1-incubating.

Development and review follow the project
[coding guidelines](CODING_GUIDELINES.md).

The public API will follow the package separation used by `franz-go` while
remaining centered on the Apache Fluss table model:

- `pkg/fmsg`: Fluss wire requests, responses, API keys, and versions.
- `pkg/fgo`: data client for log and primary-key table operations.
- `pkg/fadm`: administrative client for databases, tables, partitions, and clusters.

Current status: package and public API design. No client implementation has been
committed yet.

## Development

This project uses Task v3.51.1 as its command entry point. Run task --list to
see available checks; task verify runs formatting, generation verification,
static analysis, unit tests, race tests, and security gates.

Local security scans require Trivy v0.72.0. Integration tests require an
explicitly configured Apache Fluss cluster and FLUSS_INTEGRATION=1.

fluss-go is licensed under the Apache License 2.0. See LICENSE and NOTICE.
