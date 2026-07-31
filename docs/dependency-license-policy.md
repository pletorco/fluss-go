# Dependency license policy

Every root and optional-adapter dependency must pass the repository's pinned
Trivy license gate before merge and release. Run it locally with:

```sh
task security:licenses
```

The reviewed allowlist is deliberately small:

- `Apache-2.0`
- `BSD-2-Clause`
- `BSD-3-Clause`
- `MIT`

`Copyright` is also ignored as a scanner token, not as an approved license.
Trivy 0.72.0 emits it for the words "copyright ownership" in Arrow-Go's
Apache-2.0 `go.mod` header. It is not an SPDX identifier. Unknown or newly
detected license names still fail the gate.

The gate scans all five Go module manifests together and fails at every Trivy
license severity after subtracting the reviewed names. Its denied GPL fixture
is skipped by the repository scan and scanned separately to prove that the
policy fails closed. Trivy and its setup action are version-pinned in the
Taskfile and workflows.

Adding a license requires review of the exact SPDX terms, compatibility with
Apache License 2.0, attribution or source-distribution obligations, affected
module graphs, and maintained alternatives. Record an approved change in this
document, the build-versus-buy register, and the pull request. Do not use a
global exception for an unknown scanner result; first establish whether it is
a real dependency license or a reproducible classifier defect.

Before a release, run `task security`, inspect the resolved dependency changes,
and confirm that `LICENSE`, `NOTICE`, adapter module requirements, and release
notes contain any newly required attribution.
