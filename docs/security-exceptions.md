# Security Exceptions

Security findings block merges by default. An exception is permitted only when a
documented mitigation exists and the exception is approved by a repository
maintainer.

Each exception must be filed as a GitHub issue and record:

- advisory or finding identifier;
- affected dependency or repository path;
- impact assessment and why the finding cannot be fixed immediately;
- compensating mitigation and owner;
- explicit maintainer approval;
- expiration date no later than 30 days after approval.

The issue must be closed when the dependency is updated, the finding is removed,
or the mitigation is no longer needed. Expired exceptions are not renewable by
silence: they require a new assessment and approval before the expiration date.
