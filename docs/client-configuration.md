# Client configuration mapping

This document maps the client configuration published by Apache Fluss
`v0.9.1-incubating` to the exported `fgo` API. The source of truth is the
upstream [`ConfigOptions`](https://github.com/apache/fluss/blob/v0.9.1-incubating/fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java)
and generated [configuration reference](https://github.com/apache/fluss/blob/v0.9.1-incubating/website/docs/_configs/_partial_config.mdx).

The mapping is semantic rather than a Java property parser. Go-specific
interfaces use `context.Context`, `crypto/tls`, callbacks, and explicit
resource options instead of JAAS, JMX, Netty, Java memory pages, or string-keyed
configuration. An absent direct mapping means that `fluss-go` does not claim to
implement that Java client tuning control.

## Connection and security

| Fluss 0.9.1 key | `fluss-go` mapping | Notes |
| --- | --- | --- |
| `bootstrap.servers` | `WithBootstrapServers` | Direct semantic mapping. |
| `client.id` | None | `WithClientSoftware` sets the API-versions software name and version; it is not the Java metrics client ID. |
| `client.connect-timeout` | `WithConnectTimeout` | Direct semantic mapping. |
| `client.request-timeout` | `WithAppendRequest`, `WithUpsertRequest`, `WithLookupRequestTimeout`, and call contexts | Go exposes request bounds at the resource or call boundary rather than as one global value. |
| `client.security.protocol` | `WithAuthenticator`; absence selects plaintext | Fluss 0.9.1 provides native `PLAINTEXT` and `SASL`. `WithTLSConfig` is a Go transport extension for externally terminated TLS, not a 0.9.1 security protocol. |
| `client.security.sasl.mechanism` | `SASLPlainAuthenticator` or a custom `AuthenticatorFactory` | `SASL/PLAIN` is the only built-in Fluss 0.9.1 mechanism. |
| `client.security.sasl.username`, `client.security.sasl.password` | `SASLPlainAuthenticator` arguments | Direct credential mapping without a string configuration map. |
| `client.security.sasl.jaas.config` | None | JAAS is Java-specific. |
| `client.metrics.enabled` | `WithMetricsObserver` | Applications choose an observer explicitly; the optional OTel adapter replaces Java's default JMX reporter. |

## Writer

Both `AppendWriter` and `UpsertWriter` use the shared upstream `client.writer.*`
concepts. Their Go options remain resource-specific so independently configured
writers can share one client.

| Fluss 0.9.1 key | `fluss-go` mapping | Notes |
| --- | --- | --- |
| `client.writer.buffer.memory-size` | `WithAppendBuffer`, `WithUpsertBuffer` | Related bound, but Go limits queued records rather than Java memory bytes. |
| `client.writer.buffer.page-size` | None | Java memory-page implementation detail. |
| `client.writer.buffer.per-request-memory-size` | None | Java memory allocator implementation detail. |
| `client.writer.buffer.wait-timeout` | Call contexts and bounded enqueue behavior | No independent Java-style memory wait timeout. |
| `client.writer.batch-size` | `WithAppendBatchLimits`, `WithUpsertBatchLimits` | Go bounds encoded bytes and records. |
| `client.writer.dynamic-batch-size.enabled` | None | Go uses fixed configured bounds. |
| `client.writer.batch-timeout` | `WithAppendBatchTimeout`, `WithUpsertBatchTimeout` | Direct terminology mapping. |
| `client.writer.bucket.no-key-assigner` | `WithAppendNoKeyAssigner` and `NoKeyAssigner` | Supports the upstream `STICKY` and `ROUND_ROBIN` strategies. |
| `client.writer.acks` | `WithAppendRequest`, `WithUpsertRequest` | The same option also sets the operation request timeout. |
| `client.writer.request-max-size` | `WithAppendBatchLimits`, `WithUpsertBatchLimits` | Each Go writer request targets one bucket and is bounded by its encoded batch limit. |
| `client.writer.retries` | `WithAppendRetryPolicy`, `WithUpsertRetryPolicy` | Go uses bounded attempts and a backoff callback. |
| `client.writer.enable-idempotence` | No toggle | Retried batches preserve writer ID and sequence; retries require `acks=-1`. |
| `client.writer.max-inflight-requests-per-bucket` | No direct option | Go preserves strict ordering within each bucket. `WithAppendConcurrency` and `WithUpsertConcurrency` bound work across distinct buckets. |
| `client.writer.dynamic-create-partition.enabled` | `WithDynamicPartitionCreation` | Explicit opt-in in Go rather than enabled by default. |

## Log scanner

| Fluss 0.9.1 key | `fluss-go` mapping | Notes |
| --- | --- | --- |
| `client.scanner.log.check-crc` | Always enabled | Go does not expose a corruption-check bypass. |
| `client.scanner.log.max-poll-records` | None | `WithScanRowLimit` is a total completion bound, not a per-poll limit. |
| `client.scanner.log.fetch.max-bytes` | `WithLogFetchLimits` / `LogScannerConfig.FetchMaxBytes` | Direct semantic mapping. |
| `client.scanner.log.fetch.max-bytes-for-bucket` | `WithLogFetchLimits` / `LogScannerConfig.FetchMaxBytesForBucket` | Direct semantic mapping. |
| `client.scanner.log.fetch.wait-max-time` | `WithLogFetchLimits` / `LogScannerConfig.FetchWaitMaxTime` | Direct semantic mapping. |
| `client.scanner.log.fetch.min-bytes` | `WithLogFetchLimits` / `LogScannerConfig.FetchMinBytes` | Direct semantic mapping. |
| `client.scanner.remote-log.prefetch-num` | None | Remote objects are read through the configured provider without Java's segment-prefetch queue. |
| `client.scanner.io.tmpdir` | None | Go remote readers stream through adapters and do not use the Java scanner temporary directory. |

## Lookup

| Fluss 0.9.1 key | `fluss-go` mapping | Notes |
| --- | --- | --- |
| `client.lookup.queue-size` | `WithLookupQueue` / `LookupConfig.MaxQueuedKeys` | Direct semantic mapping. |
| `client.lookup.max-batch-size` | `WithLookupBatchLimits` / `LookupConfig.MaxBatchKeys` | Direct semantic mapping. |
| `client.lookup.max-inflight-requests` | `WithLookupBatchLimits` / `LookupConfig.MaxInFlightRequests` | Direct semantic mapping. |
| `client.lookup.batch-timeout` | `WithLookupQueue` / `LookupConfig.BatchTimeout` | Direct semantic mapping. |
| `client.lookup.max-retries` | `WithLookupRetryPolicy` | Go uses bounded attempts and a backoff callback. |

## Remote files and filesystem tokens

| Fluss 0.9.1 key | `fluss-go` mapping | Notes |
| --- | --- | --- |
| `client.remote-file.download-thread-num` | `RemoteFileReadConfig.MaxConcurrentReads` | Bounds concurrent object streams rather than Java download threads. |
| `client.filesystem.security.token.renewal.backoff` | `FileSystemSecurityTokenRefreshConfig.RenewalRetryBackoff` | Go adds `MaxRenewalRetryBackoff` for capped exponential retry. |
| `client.filesystem.security.token.renewal.time-ratio` | `FileSystemSecurityTokenRefreshConfig.RenewalTimeRatio` | Direct terminology mapping. |

## Go-specific controls

`WithClientSoftware`, `WithDialContext`, `WithTLSConfig`,
`WithTransportLimits`, `WithRetryPolicy`, partition selectors, snapshot
providers, remote-file safety bounds, token clock skew and jitter, writer record
limits, and typed codecs have no direct Java configuration-key equivalent.
They remain Go APIs because they describe Go transport injection, safety,
resource ownership, or capabilities that are selected per operation.
