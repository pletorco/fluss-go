# Fluss 1.0 live compatibility

`task test:integration` first starts two isolated clusters from the official
`apache/fluss:1.0.0` image pinned to digest
`sha256:ff461b45438033da4fd1c2556d3f978f3603bb3632fe075c2bd57388339a58cb`.
The release tag resolves to upstream commit
`5c07f88e50a8ff41b0ebc214a0458e5dba60be37`.

The plaintext cluster has one coordinator and three tablet servers with a
replication factor of three. The second cluster has SASL PLAIN enabled on its
client listener. A second phase starts equivalent isolated backends behind
HAProxy 3.2.21, pinned by image digest. HAProxy terminates TLS for the
coordinator and every advertised tablet; this verifies a deployment topology,
not a native Fluss TLS listener. The runner generates an ephemeral CA,
certificate, and passwords, removes them on exit, and redacts credentials from
failure diagnostics.

The suite verifies the byte-level compatibility fixtures before running live
protocol, request-cancellation isolation, authentication, routing, catalog, log, KV,
lookup, prefix-lookup, tablet leader-failover data correctness, and coordinator
restart recovery checks. Failover writes cover every bucket and compare
acknowledged offsets with a bounded final scan; KV values are verified before
and after leader movement. Typed wrappers, partial updates, merge modes,
producer offsets, snapshot metadata and leases, safe advanced admin reads,
TLS-routed admin/data calls, TLS with SASL, standard certificate failures,
protocol mismatches, and handshake cancellation are also exercised. The
complete method matrix and deliberate environment
limits are recorded in [live evidence](../docs/live-evidence.md). Docker, Docker
Compose, OpenSSL, Go, and Task are required. Ports `19123` through `19126`,
`19223`, `19224`, `19323` through `19327`, `19423`, `19424`, and `19523` must
be free. The plaintext and native SASL ports can be overridden with their
corresponding `FLUSS_*_PORT` variables.

The same plaintext cluster runs the required eight-second reliability smoke
profile. `task test:reliability` runs one selected longer profile without the
unrelated functional and TLS phases. Profile bounds, fault semantics, JSON
artifacts, and the pinned baseline are documented in
[reliability testing](../docs/reliability-testing.md).
