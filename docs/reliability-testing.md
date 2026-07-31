# Reliability testing

The reliability harness runs bounded workloads against the official Apache
Fluss `0.9.1-incubating` image pinned by digest. It is evidence for client
scheduler, connection, memory, cancellation, and shutdown behavior; it is not
a server benchmark or a production capacity estimate.

## Profiles

| Profile | Default duration | Operation cap | Workers | Work |
| --- | ---: | ---: | ---: | --- |
| `smoke` | 8s | 256 | 4 | Fairly divided log, KV, lookup, and scan work plus delayed/truncated bootstrap and cancellation bursts. Required on pull requests. |
| `log` | 2m | 10,000 | 4 | Concurrent acknowledged appends followed by an offset-bounded complete scan. |
| `kv` | 2m | 10,000 | 4 | Concurrent unique-key upserts followed by exact point lookup verification. |
| `lookup` | 2m | 20,000 | 8 | Concurrent reads of seeded primary-key rows. |
| `scan` | 2m | 2,000 | 2 | Repeated bounded scanners over seeded and concurrently visible log rows. |
| `mixed` | 5m | 30,000 | 8 | Fairly divided log, KV, lookup, and scan workers. |
| `soak` | 15m | 5,000 | 2 | Repeated client, table, metadata, lookup, scanner, and writer open/close cycles. |
| `fault` | 10m | 30,000 | 8 | Mixed work with delayed connections, truncated reads, cancellation bursts, and a tablet restart. |

The duration and operation count are independent upper bounds. A run stops
when either bound is reached. Mixed work divides the operation cap among
workers so a low-latency operation cannot starve the other operation types.

Run the default smoke profile with:

```sh
task test:reliability
```

Select a reproducible bounded run through environment variables:

```sh
FLUSS_RELIABILITY_PROFILE=fault \
FLUSS_RELIABILITY_DURATION=10m \
FLUSS_RELIABILITY_SEED=104091 \
FLUSS_RELIABILITY_MAX_OPS=30000 \
FLUSS_RELIABILITY_WORKERS=8 \
task test:reliability
```

Supported durations are 1 second through 30 minutes, operation caps are 16
through 1,000,000, and worker counts are 1 through 32. The default seed is
`104091`. `FLUSS_RELIABILITY_REPORT` can select the report path; the shared
runner otherwise writes `.task/reliability/<profile>.json` from the repository
root.

## Failure policy

Every profile fails on an unexplained operation error, missing or changed
acknowledged data, a duplicate log ID, cleanup failure, retained goroutine
growth greater than 12, or more than 64 MiB of retained heap growth. Heap and goroutine
measurements run after repeated garbage collection and after workload-owned
resources close.

Context errors caused by the configured profile deadline, deliberately
canceled requests, and classified transient errors during the `fault` profile
are counted separately. A timed-out mutation can have reached the server. The
final scan therefore covers every ending offset, requires every acknowledged
row, and reports any additional observed row as `unacknowledged_log_rows`
instead of silently treating an ambiguous result as either success or loss.

The JSON report contains only profile configuration, aggregate counts,
latency percentiles, throughput, resource measurements, fault names, and final
correctness. It contains no credentials, endpoints, database names, rows, or
error payloads from successful runs. Pull-request smoke reports are retained
for 14 days. Scheduled and manual reports are retained for 30 days.

## Pinned baseline

The hard baseline for the pinned Fluss image is deterministic: zero missing or
changed acknowledged rows, zero duplicate IDs, zero unexplained errors,
goroutine growth no greater than 12, and retained heap growth no greater than
64 MiB. Latency and throughput are recorded for trend comparison but do not
have a fixed shared-runner threshold.

The initial local fault reference used seed `104091`, 12 seconds, 128
operations, and four workers. It verified all 30 acknowledged log rows and all
26 acknowledged KV rows after a tablet restart, observed one separately
reported ambiguous log row, retained no additional goroutine, and retained
about 127 KiB of heap. Shared-runner results may differ; a regression must be
compared with the retained report using the same profile, seed, bounds, image,
Go version, and runner class.

The required integration workflow runs `smoke`. The weekly workflow runs
10-minute `mixed`, 15-minute `soak`, and 10-minute `fault` profiles. A manual
workflow accepts all profile bounds explicitly. Every runner has a 35-minute
job timeout, digest verification, failure diagnostics, volume removal, and
process cleanup.
