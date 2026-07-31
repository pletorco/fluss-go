# Public API stability

The exported Go API remains experimental before v1, but every change is
reviewed against the latest approved baseline. Experimental means that a
breaking change may be accepted with migration guidance; it does not mean that
exports may change accidentally.

## Owned surfaces

| Surface | Ownership and stability |
| --- | --- |
| `pkg/fgo` | Application data API, configuration, lifecycle, errors, extension interfaces, and Arrow integration. Review all changes for source compatibility and runtime behavior. |
| `pkg/fadm` | Administrative API over a shared `fgo.Client`. Review names, partial results, identifiers, and server-side side effects. |
| `pkg/fmsg` | Pinned Fluss 0.9.1 wire messages, API metadata, raw request escape hatch, and protocol errors. Generated protobuf changes follow upstream inputs and are reviewed separately from client ergonomics. |
| `adapters/*` | Separately versioned optional modules. Their API baselines, dependencies, and prefixed tags are reviewed independently. |
| `internal/*` and `cmd/*` | Not supported as application APIs. Repository tooling commands may change with the development workflow. |

The intended extension points are `fmsg.Requester`, `fgo.Codec`,
`fgo.KeyCodec`, authentication and metrics interfaces, remote-file interfaces,
filesystem-token providers and receivers, and snapshot resolver/decoder
interfaces. Concrete client, writer, scanner, lookup, result, schema, and row
types are user-facing APIs rather than implementation extension points.

Generated `fmsg` types are public because raw protocol access is supported.
They are not hand-edited, and their names and fields follow the pinned
`FlussApi.proto`. `task verify:generate` proves source reproducibility while the
separate protocol API baseline makes generated surface changes explicit.

## Compatibility gate

`task api:check` compares all three public packages with binary export-data
baselines under `api/baselines` by using the pinned Go `apidiff` command. The
gate rejects compatible additions as well as incompatible changes so every new
export receives the same naming, documentation, ownership, and dependency
review.

After an approved change, run `task api:update` and inspect the source diff and
`apidiff` output before committing the new baseline. A baseline update alone is
not approval. User-visible changes also require tests, package documentation,
examples where useful, a changelog entry, and migration guidance for renamed,
removed, or behaviorally changed APIs.

Before RC, review the complete surface for accidental exports and settle any
Arrow package changes. Adapter modules are already isolated from the root
module graph and use the same release version with path-prefixed Git tags.
After RC, removal or incompatible
signature changes require an explicit exception for a security or correctness
defect. The v1 policy will replace this prerelease policy before a stable tag.
