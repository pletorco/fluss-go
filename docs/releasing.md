# Release process

Releases are made only from a reviewed commit on `main`. A feature branch may
prepare the changelog and release notes, but it must not publish a tag or
GitHub Release before merge.

Before opening a release-preparation PR, finish the versioned changelog,
release notes, and affected user manuals together on that branch. The PR is
the review boundary for the complete release documentation, not a placeholder
that is filled after merge. Run `task ci` and `task sonar` on the completed
branch before opening the PR. `task sonar` is a local pre-PR gate: it waits for
the Quality Gate and must pass before review begins. Fix findings on the same
branch rather than discovering them after merge.

SonarQube Community Edition records these local analyses under its single
`main` branch. Ignore that displayed branch label. Validate the SCM revision
reported by the scanner against `git rev-parse HEAD` and use the Quality Gate
result for that revision.

The current prepared prerelease is `v0.1.0-beta.9`, with release notes in
`.github/releases/v0.1.0-beta.9.md`.

## Before publication

1. Merge the release-preparation PR.
2. Confirm the required CI, dependency review, `Fluss 0.9.1 live` integration,
   and security checks passed for the exact merge commit. The integration check
   is a stable branch-protection context and must not be bypassed for runtime,
   protocol, fixture, or dependency changes. Inspect its retained reliability
   smoke report; client scheduling, connection, cancellation, or lifecycle
   changes also require a recent scheduled `mixed`, `soak`, and `fault` run.
3. Update local refs and verify that the worktree is clean, `HEAD` equals
   `origin/main`, and the current branch is `main`.
4. Run `task ci` on that commit. Sonar remains the local pre-PR gate and is not
   rerun as a post-merge GitHub Actions gate.
5. Confirm that `CHANGELOG.md` records the version and publication date and
   that the prepared release notes match it. Update the current release and
   installation examples in `README.md`, and move the supported prerelease in
   `SECURITY.md` to the version being published. Search user-facing manuals for
   the previous tag and review every remaining reference intentionally.
6. Inspect `task security:licenses` after every dependency change and confirm
   that any new attribution obligations are reflected in `LICENSE`, `NOTICE`,
   module documentation, and the release notes.

Do not reuse or move a published version tag. If the intended commit is wrong,
fix `main` through another reviewed PR and release the corrected commit.

## Tag and GitHub prerelease

Create an annotated tag on the verified `main` commit:

```sh
git tag -a v0.1.0-beta.9 -m "fluss-go v0.1.0-beta.9"
git show --no-patch --decorate v0.1.0-beta.9
git push origin v0.1.0-beta.9
```

Then create the GitHub prerelease from the committed notes:

```sh
gh release create v0.1.0-beta.9 \
  --repo pletorco/fluss-go \
  --verify-tag \
  --prerelease \
  --title "fluss-go v0.1.0-beta.9" \
  --notes-file .github/releases/v0.1.0-beta.9.md
```

Verify that the release target SHA equals the peeled annotated tag and the
reviewed `main` commit:

```sh
test "$(git rev-parse v0.1.0-beta.9^{})" = "$(git rev-parse origin/main)"
gh release view v0.1.0-beta.9 --repo pletorco/fluss-go
```

### Adapter module tags

Beginning with `v0.1.0-beta.7`, each adapter is a nested module. Publish the
root tag first, wait until the public Go proxy resolves it, and then publish
path-prefixed adapter tags on the same reviewed commit:

```sh
git tag -a adapters/hdfs/v0.1.0-beta.9 -m "fluss-go HDFS adapter v0.1.0-beta.9"
git tag -a adapters/oss/v0.1.0-beta.9 -m "fluss-go OSS adapter v0.1.0-beta.9"
git tag -a adapters/otel/v0.1.0-beta.9 -m "fluss-go OpenTelemetry adapter v0.1.0-beta.9"
git tag -a adapters/s3/v0.1.0-beta.9 -m "fluss-go S3 adapter v0.1.0-beta.9"
git push origin adapters/hdfs/v0.1.0-beta.9 adapters/oss/v0.1.0-beta.9 \
  adapters/otel/v0.1.0-beta.9 adapters/s3/v0.1.0-beta.9
```

The adapter `go.mod` files use a repository-local `replace` for development.
Go ignores dependency-module replacements for downstream applications; the
versioned root requirement is authoritative after publication. Before tagging,
verify that every adapter requires an already published root version and that
all modules pass with the checked-in workspace. Never publish an adapter tag
before its required root module is available from the public proxy.

## Module and documentation discovery

Ask the public Go module proxy for the exact version; do not validate through a
direct VCS fallback:

```sh
GOPROXY=https://proxy.golang.org GONOSUMDB= \
  go list -m -json github.com/pletorco/fluss-go@v0.1.0-beta.9
```

Confirm that the returned `Version` is `v0.1.0-beta.9` and that its origin hash
matches the released commit. The proxy may need a short propagation interval,
but a failed lookup must not be hidden by `GOPROXY=direct`.

For beta.7 and later, repeat the proxy check for every published adapter:

```sh
GOPROXY=https://proxy.golang.org GONOSUMDB= \
  go list -m -json github.com/pletorco/fluss-go/adapters/s3@v0.1.0-beta.9
```

Open the version-pinned online references and verify their package manuals,
exported contracts, and examples:

- `https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fmsg@v0.1.0-beta.9`
- `https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fgo@v0.1.0-beta.9`
- `https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fadm@v0.1.0-beta.9`
- `https://pkg.go.dev/github.com/pletorco/fluss-go/adapters/hdfs@v0.1.0-beta.9`
- `https://pkg.go.dev/github.com/pletorco/fluss-go/adapters/oss@v0.1.0-beta.9`
- `https://pkg.go.dev/github.com/pletorco/fluss-go/adapters/otel@v0.1.0-beta.9`
- `https://pkg.go.dev/github.com/pletorco/fluss-go/adapters/s3@v0.1.0-beta.9`

Check at minimum the `fgo` TLS/SASL and error examples and the `fadm` advanced
administration examples. Record any pkg.go.dev indexing delay on the release
issue and recheck without publishing another tag.

## After publication

Close the release issue only after the annotated tag, GitHub prerelease, module
proxy version, and all three pkg.go.dev package pages have been verified. Keep
the next `Unreleased` section in `CHANGELOG.md`; new work accumulates there.
