# Release process

Releases are made only from a reviewed commit on `main`. A feature branch may
prepare the changelog and release notes, but it must not publish a tag or
GitHub Release before merge.

Before opening a release-preparation PR, finish the versioned changelog,
release notes, and affected user manuals together on that branch. The PR is
the review boundary for the complete release documentation, not a placeholder
that is filled after merge.

The current prepared prerelease is `v0.1.0-beta.5`, with release notes in
`.github/releases/v0.1.0-beta.5.md`.

## Before publication

1. Merge the release-preparation PR.
2. Confirm the required CI, dependency review, `Fluss 0.9.1 live` integration,
   and security checks passed for the exact merge commit. The integration check
   is a stable branch-protection context and must not be bypassed for runtime,
   protocol, fixture, or dependency changes.
3. Update local refs and verify that the worktree is clean, `HEAD` equals
   `origin/main`, and the current branch is `main`.
4. Run `task ci` on that commit. Run `task sonar` once and confirm the Sonar
   Quality Gate passes.
5. Confirm that `CHANGELOG.md` records the version and publication date and
   that the prepared release notes match it.

Do not reuse or move a published version tag. If the intended commit is wrong,
fix `main` through another reviewed PR and release the corrected commit.

## Tag and GitHub prerelease

Create an annotated tag on the verified `main` commit:

```sh
git tag -a v0.1.0-beta.5 -m "fluss-go v0.1.0-beta.5"
git show --no-patch --decorate v0.1.0-beta.5
git push origin v0.1.0-beta.5
```

Then create the GitHub prerelease from the committed notes:

```sh
gh release create v0.1.0-beta.5 \
  --repo pletorco/fluss-go \
  --verify-tag \
  --prerelease \
  --title "fluss-go v0.1.0-beta.5" \
  --notes-file .github/releases/v0.1.0-beta.5.md
```

Verify that the release target SHA equals the peeled annotated tag and the
reviewed `main` commit:

```sh
test "$(git rev-parse v0.1.0-beta.5^{})" = "$(git rev-parse origin/main)"
gh release view v0.1.0-beta.5 --repo pletorco/fluss-go
```

## Module and documentation discovery

Ask the public Go module proxy for the exact version; do not validate through a
direct VCS fallback:

```sh
GOPROXY=https://proxy.golang.org GONOSUMDB= \
  go list -m -json github.com/pletorco/fluss-go@v0.1.0-beta.5
```

Confirm that the returned `Version` is `v0.1.0-beta.5` and that its origin hash
matches the released commit. The proxy may need a short propagation interval,
but a failed lookup must not be hidden by `GOPROXY=direct`.

Open the version-pinned online references and verify their package manuals,
exported contracts, and examples:

- `https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fmsg@v0.1.0-beta.5`
- `https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fgo@v0.1.0-beta.5`
- `https://pkg.go.dev/github.com/pletorco/fluss-go/pkg/fadm@v0.1.0-beta.5`

Check at minimum the `fgo` TLS/SASL and error examples and the `fadm` advanced
administration examples. Record any pkg.go.dev indexing delay on the release
issue and recheck without publishing another tag.

## After publication

Close the release issue only after the annotated tag, GitHub prerelease, module
proxy version, and all three pkg.go.dev package pages have been verified. Keep
the next `Unreleased` section in `CHANGELOG.md`; new work accumulates there.
