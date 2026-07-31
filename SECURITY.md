# Security policy

Security is part of the compatibility contract for fluss-go. Please report a
suspected vulnerability privately so maintainers can investigate it before
technical details are made public.

## Supported versions

fluss-go is currently a prerelease client for the pinned Apache Fluss
`0.9.1-incubating` compatibility target. Security fixes are released from the
latest beta line rather than backported to every earlier prerelease.

| Version | Security updates |
| --- | --- |
| `v0.1.0-beta.7` | Supported |
| Earlier prereleases | Not supported; upgrade to the latest beta |
| Untagged branches and commits | Not supported |

This table is updated with each release. A newer release supersedes the beta
listed above unless its release notes state otherwise.

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/pletorco/fluss-go/security/advisories/new)
whenever possible. If that form is unavailable, email `support@pletor.kr` with
the subject `fluss-go security report`.

Do not open a public issue for an unpatched vulnerability. Include only the
information needed to reproduce and assess the report:

- the affected fluss-go, Go, and Apache Fluss versions;
- the affected package, adapter, or protocol operation;
- prerequisites, configuration, and a minimal reproduction;
- expected and observed impact;
- whether credentials, network access, or an authenticated user are required;
- any suggested mitigation; and
- a safe way to contact you about the report.

Remove credentials, security tokens, private keys, production data, and other
secrets. If evidence cannot be shared safely in the initial report, describe
what is available and wait for a private follow-up.

## Response and disclosure

Maintainers aim to acknowledge a report within three business days and provide
an initial triage update within seven business days. These are response goals,
not a support SLA. Complex protocol or upstream dependency issues may require
coordination with Apache Fluss or another maintainer.

After validation, maintainers will coordinate a fix, affected-version range,
release, and disclosure date with the reporter. Please do not publish exploit
details before a fixed release or an agreed disclosure date. Credit is offered
when requested and when doing so does not expose confidential information.

The project does not currently operate a bug bounty program.

## Non-security defects

Crashes that require trusted local input, ordinary correctness bugs, feature
requests, and documentation problems normally belong in the public
[issue tracker](https://github.com/pletorco/fluss-go/issues/new/choose). When
uncertain whether a defect has a security impact, report it privately first.
