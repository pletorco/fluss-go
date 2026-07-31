# Documentation validation

`task docs:check` is the repository's deterministic documentation gate. It
checks public Go declarations, pkg.go.dev examples, repository-local Markdown
links and anchors, documented Task commands, and every user-facing Go code
fence.

## Tool selection

The project pins
[`github.com/yuin/goldmark`](https://github.com/yuin/goldmark) v1.8.5 as the
Markdown parser used by `cmd/mdcheck`. The dependency was reviewed on
2026-07-31:

- it is actively maintained and implements CommonMark with a GitHub Flavored
  Markdown extension;
- it uses the MIT license, which is compatible with Apache License 2.0;
- it is one direct Go module dependency with no transitive module
  dependencies; and
- parsing runs in-process without executing document content or making network
  requests.

Two standalone link checkers were also evaluated. Lychee v0.24.2 is actively
maintained under Apache License 2.0, but would add a separately installed Rust
binary and its release verification process. `markdown-link-check` is
maintained under the ISC license, but would add a Node.js dependency tree and
is primarily oriented toward live URL checks. Both remain reasonable choices
if the project later adopts an explicitly networked external-link job.

Dependabot reports goldmark updates through `go.mod`. To update it, review the
release and license, run `go get github.com/yuin/goldmark@<version>` followed by
`go mod tidy`, then run `task ci` and the security gates. No separate executable
or checksum is required.

Task names are read structurally from `Taskfile.yml` with `gopkg.in/yaml.v3`
v3.0.1, which was already present in the module graph before the documentation
checker imported it directly. The parser is maintained under MIT and
Apache-2.0 terms, does not execute Task commands, and avoids maintaining a
partial YAML parser. Trivy and govulncheck inspect it with the rest of the Go
module graph.

## Local links

`cmd/mdcheck` parses Markdown with goldmark and validates relative links,
root-relative repository links, directory targets, and heading fragments.
Failures identify the source file, line, and original destination. Heading IDs
follow GitHub-style lowercase slugs and duplicate headings receive `-1`, `-2`,
and later suffixes.

Normal checks never contact external hosts. Absolute URLs, protocol-relative
URLs, and email links are parsed and counted but deliberately skipped after
scheme detection. External availability is therefore not a merge-gate claim.

The scan includes handwritten `*.md` files under the requested roots and has
these reviewed exclusions:

- `vendor` and `third_party` contain dependency-owned content, while `.git` and
  `.tools` contain repository metadata or local tool artifacts;
- a Markdown file carrying both `Code generated` and `DO NOT EDIT` in its first
  2 KiB is generated content; and
- non-Markdown files are outside this check.

The CI command scans the repository root, so a new handwritten Markdown file is
included automatically.

## Task commands

Inline code and `sh`, `bash`, `shell`, or `console` fences are checked for
`task <name>` commands. Each referenced name must be a top-level task in
`Taskfile.yml`; shell comments are ignored. This catches renamed or removed
commands without executing documentation. Free prose and non-shell code are
not interpreted as commands, and Task variables or dynamically included
Taskfiles are outside the current repository policy.

## Go snippets

Every `go` or `golang` fence must have an adjacent source marker:

```text
<!-- go-source: internal/docexamples/snippets_test.go snippetName -->
```

The named source is delimited by matching `doc:snippet` and
`doc:snippet-end` comments in `internal/docexamples/*_test.go`.
`task docs:snippets:compile` compiles that package without executing
live-cluster workflows, and `cmd/mdcheck` requires the rendered fence to match
the compiled source exactly. Source paths outside this dedicated test package
are rejected.

Edit the source first, then synchronize Markdown with:

```sh
task docs:snippets:sync
```

Review the resulting documentation diff and run `task docs:check`. The normal
check is read-only and fails on stale or unmarked snippets.

Pkg.go.dev examples follow a separate two-stage policy.
`docs:examples:compile` compiles every example, including workflows that need a
live Fluss cluster, without running them. `docs:examples:run` executes only
deterministic examples carrying expected `Output` directives and lets the Go
test runner compare their output.
