# fluss-go

Experimental Go client for Apache Fluss.

Development and review follow the project
[coding guidelines](CODING_GUIDELINES.md).

The public API will follow the package separation used by `franz-go` while
remaining centered on the Apache Fluss table model:

- `pkg/fmsg`: Fluss wire requests, responses, API keys, and versions.
- `pkg/fgo`: data client for log and primary-key table operations.
- `pkg/fadm`: administrative client for databases, tables, partitions, and clusters.

Current status: package and public API design. No client implementation has been
committed yet.
