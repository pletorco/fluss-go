## Summary

-

## Verification

- [ ] task verify
- [ ] New or changed behavior has focused tests.
- [ ] Public API and compatibility impact is documented.
- [ ] Exported API changes are intentional, pass `task api:check`, and include baseline, changelog, and migration updates when applicable.
- [ ] Changed `fgo`/`fadm` names and settings were checked against the complete affected Fluss public API/configuration surface; direct mappings, unsupported settings, and Go-specific differences are documented.
- [ ] Generated `pkg/fmsg` names remain derived from the pinned protocol inputs and were not renamed for client ergonomics.

## Build vs buy

- Decision: N/A / mandatory reuse / new dependency / justified internal implementation
- Alternatives reviewed:
- Rejection reasons and maintenance owner:
- License, security, transitive dependency, compatibility, and size impact:
- Compatibility, golden, integration, or benchmark evidence:

- [ ] Exactly one build-vs-buy classification above applies to this change.
- [ ] The decision follows `CODING_GUIDELINES.md`, or this section explains the approved exception.
- [ ] Library behavior is reused through a thin adapter instead of copied.
- [ ] A new dependency or reusable custom implementation updates `docs/build-vs-buy.md` when required.

## Security and operations

- [ ] No credentials, tokens, or private keys are included.
- [ ] Dependency, license, and generated-artifact changes are explained.
- [ ] Relevant Apache Fluss version assumptions are recorded.
