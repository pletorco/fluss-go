# Contributing to fluss-go

Thank you for helping improve fluss-go. Contributions are welcome for bug
fixes, compatibility evidence, documentation, tests, and carefully reviewed
features within the supported Apache Fluss version.

By participating, you agree to follow the
[Code of Conduct](CODE_OF_CONDUCT.md). Report suspected vulnerabilities through
the private process in [SECURITY.md](SECURITY.md), not through a public issue.

## Before starting

1. Search the [issue tracker](https://github.com/pletorco/fluss-go/issues) for
   related reports and proposals.
2. Open an issue before changing a public API, package boundary, protocol
   input, compatibility policy, security policy, or external dependency.
3. Keep the change scoped to one reviewable problem. Unrelated refactoring
   should use a separate issue and pull request.
4. Read [CODING_GUIDELINES.md](CODING_GUIDELINES.md), especially the
   build-vs-buy and test requirements.

The project currently supports only Apache Fluss `0.9.1-incubating` at the
commit recorded in the [compatibility matrix](README.md#compatibility-matrix).
Support for another Fluss version requires protocol inputs, fixtures,
documentation, and live integration evidence rather than version-only claims.

## Development setup

Install a supported Go release and the pinned tools listed in the
[README](README.md#compatibility-matrix). Task is the command entry point for
repository development.

```sh
git clone https://github.com/pletorco/fluss-go.git
cd fluss-go
go mod download
task tools:check
task test
```

Fork the repository when you do not have branch access. Create every change on
a feature branch based on the latest `origin/main`; do not commit directly to
`main`. Use a descriptive name such as `fix/lookup-cancellation` or
`docs/security-reporting`.

The checked-in `go.work` joins the root module and optional adapter modules for
development. Do not add an optional adapter dependency to the root module when
the integration can remain isolated in its existing adapter module.

## Making a change

- Reuse the standard library, existing project code, or a well-maintained
  library when it satisfies the requirement. Do not copy or reimplement a
  general-purpose library behind a local API.
- Do not hand-edit generated protocol files. Update the pinned source and use
  `task generate` when an approved protocol change requires regeneration.
- Preserve error causes, cancellation, resource ownership, and partial-result
  behavior at public boundaries.
- Align exported `fgo` and `fadm` names, options, and configuration semantics
  with the pinned Fluss public client and configuration terminology. Compare
  the complete affected upstream surface, document intentional Go-specific
  differences in `docs/client-configuration.md`, and do not hand-edit generated
  `pkg/fmsg` names for ergonomic consistency.
- Add tests for changed behavior, important errors, boundary conditions,
  cancellation, and cleanup. Bug fixes should include a regression test.
- Update API baselines, examples, compatibility evidence, and the changelog
  when the corresponding public surface changes.
- Edit compile-checked documentation snippets at their source under
  `internal/docexamples`, then run `task docs:snippets:sync`.

Build-vs-buy decisions must record the alternatives, rejection reasons,
maintenance ownership, license and security impact, and compatibility evidence
required by [docs/build-vs-buy.md](docs/build-vs-buy.md).

## Verification

Run the checks that match the change while developing, then run the complete
pre-PR gate:

```sh
task verify
```

Useful focused commands include:

| Change | Minimum focused verification |
| --- | --- |
| Go behavior | `task test`, plus focused package tests |
| Concurrent lifecycle or networking | `task test:race` |
| Parser, decoder, or wire input | `task test:fuzz` and `task test:golden` |
| Exported API | `task api:check` |
| Documentation | `task docs:check` |
| Dependency or security surface | `task security` |
| Public Fluss workflow | `task test:integration` |
| S3 or HDFS adapter service behavior | `task test:storage` |
| Release preparation | `task ci` and `task sonar` before opening the PR |

Integration tasks require Docker and may take longer than unit checks. If a
required environment is unavailable, explain the missing verification in the
pull request; required evidence must still pass before merge.

`task sonar` is a local release-preparation gate and waits for its Quality
Gate. SonarQube Community Edition displays local analyses under `main`; verify
the scanner's SCM revision against the release branch `HEAD` instead of using
that branch label. Resolve findings before opening the release PR. Sonar is not
part of the post-merge GitHub Actions workflows.

## Commits and pull requests

- Write focused commits with an imperative summary, for example
  `fgo: preserve cancellation cause during lookup`.
- Push the feature branch and open a pull request against `main`.
- Link the issue with `Fixes #<number>` when the merge should close it.
- Complete every applicable section of the pull request template.
- Describe behavior and compatibility impact, not only the files changed.
- Record the exact tests run and any remaining risk or untested environment.
- Do not weaken or delete a valid test to make a check pass.

Maintainers may ask for a smaller scope, additional failure coverage, live
compatibility evidence, or a build-vs-buy review before merging.
