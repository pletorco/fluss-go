# Build-vs-buy decisions

This register records reviewed exceptions and dependency choices under the
project build-vs-buy rule. It is not a blanket approval for similar future
code. A decision must be revisited when its requirements, available libraries,
supported Fluss version, maintenance state, license, or security assumptions
change.

| Area | Decision | Alternatives and rationale | Maintenance and verification |
| --- | --- | --- | --- |
| Fluss row, key, KV, and log wire codecs | Limited internal implementation using `encoding/binary`, `hash/crc32`, generated protobuf messages, and Arrow-Go | General serialization libraries do not implement the compacted/indexed Fluss 0.9.1 layouts or record headers. Copying Java internals would create a larger derived-code maintenance burden. Arrow IPC and its LZ4/ZSTD support are reused rather than reimplemented. | The scope is pinned to Fluss 0.9.1. Java byte fixtures, round trips, malformed input tests, fuzz targets, and live integration guard compatibility. |
| Fluss bucket hashing | Limited internal Java-compatible adapter | Generic Murmur3 packages implement the common algorithm but do not establish the exact Fluss sequence of seed-42 byte hashing, signed tail expansion, second integer hash, and Java absolute-value edge case. A dependency would still require custom composition and compatibility validation. | [`pkg/fgo/bucket.go`](../pkg/fgo/bucket.go) is isolated and checked against pinned Java 0.9.1 fixtures. It is not exposed as a general hash implementation. |
| SASL PLAIN | Limited internal mechanism adapter; mandatory `crypto/tls` reuse for TLS | A broad SASL framework would add mechanism and transitive surface that the Fluss 0.9.1 client does not use. The adapter only creates the RFC 4616 initial response and clears credential buffers; it does not implement cryptography. | Unit tests cover state, challenge rejection, buffer clearing, and credential validation. The pinned integration cluster verifies SASL PLAIN. Any additional mechanism requires a fresh library evaluation. |
| Protocol generation | Reuse `protoc` for protobuf; limited internal registry generator for pinned Java enums | Protobuf generation uses the official compiler and Go plugin. `ApiKeys.java` and `Errors.java` are upstream enum inputs rather than a standard code-generation schema, so the small generator extracts only the public registry needed by `fmsg`. | Upstream files and SHA-256 values are pinned, expected entry counts are checked, generated Go is formatted, and `task generate:check` verifies deterministic output. |
| Duplicate request coalescing | Small internal generic helper | `golang.org/x/sync/singleflight` coalesces calls but does not provide independent waiter cancellation, last-waiter operation cancellation, group close errors, or unclaimed resource disposal. Wrapping it would retain nearly all required state. | Owner/waiter/all-cancel, replacement, close race, result sharing, disposal, repeated concurrency, and race tests cover the helper. The full context and ownership decision is in [request coalescing](architecture/request-coalescing.md). |

## New decisions

A PR that adds a dependency or custom implementation must add or update a row
when the decision is architectural, security-sensitive, protocol-specific, or
likely to be reused. Small one-off decisions may remain in the PR description
when all evidence required by `CODING_GUIDELINES.md` is present.
