# Fluss 0.9.1 live compatibility

`task test:integration` starts two isolated clusters from the official
`apache/fluss:0.9.1-incubating` image pinned to digest
`sha256:65f5513b33dde10ace4f8adb3956f17226a2a1e2663f92b3096e4769b0ee1d1c`.
The release tag resolves to upstream commit
`6bf969f71af8d6f9cc37383ab89ae46a58b0e227`.

The plaintext cluster has one coordinator and three tablet servers with a
replication factor of three. The second cluster has SASL PLAIN enabled on its
client listener. The runner generates an ephemeral password and redacts it
from failure diagnostics.

The suite verifies the Java 0.9.1 golden fixtures before running live protocol,
request-cancellation isolation, authentication, routing, catalog, log, KV,
lookup, prefix-lookup, tablet leader-failover data correctness, and coordinator
restart recovery checks. Failover writes cover every bucket and compare
acknowledged offsets with a bounded final scan; KV values are verified before
and after leader movement. Typed wrappers, partial updates, merge modes,
producer offsets, snapshot metadata and leases, and safe advanced admin reads
are also exercised. The complete method matrix and deliberate environment
limits are recorded in [live evidence](../docs/live-evidence.md). Docker, Docker
Compose, OpenSSL, Go, and Task are required. Ports `19123` through `19126`,
`19223`, and `19224` must be free; each can be overridden with its corresponding
`FLUSS_*_PORT` variable.
