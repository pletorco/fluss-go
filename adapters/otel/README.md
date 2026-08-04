# OpenTelemetry metrics adapter

This package maps `fgo.MetricEvent` to the stable OpenTelemetry Go metrics API.
Core `pkg/fgo` remains backend-neutral, and this adapter imports no
OpenTelemetry SDK, reader, OTLP client, Prometheus exporter, or collector
transport.

This adapter is a separately versioned Go module. Install the adapter version
that matches the root client release:

```sh
go get github.com/pletorco/fluss-go/adapters/otel@v0.1.0-beta.9
```

Create `Observer` with an application-owned `metric.MeterProvider`, then pass it
to `fgo.WithMetricsObserver`. The application is responsible for configuring
the SDK, views, resources, readers, export interval, and exporter, and for
flushing and shutting down its provider after all Fluss clients are closed.
The adapter must not shut down a shared provider.

Synchronous `Add` and `Record` calls update the provider's metric aggregation.
Exporter network I/O belongs to the application's reader/export pipeline and is
not performed by the adapter. Instrument panics are recovered so telemetry
cannot fail a client operation.

## Instruments

| Name | Type | Unit | Value |
| --- | --- | --- | --- |
| `fluss.client.events` | Counter | `{event}` | Completed metric events |
| `fluss.client.failures` | Counter | `{failure}` | Failed events |
| `fluss.client.bytes` | Counter | `By` | Positive processed bytes |
| `fluss.client.records` | Counter | `{record}` | Positive processed records |
| `fluss.client.duration` | Histogram | `s` | Positive operation duration |
| `fluss.client.queue.duration` | Histogram | `s` | Positive queue duration |
| `fluss.client.queue.size` | Histogram | `{item}` | Non-negative write-batch queue depth |
| `fluss.client.attempt` | Histogram | `{attempt}` | Positive attempt number |
| `fluss.client.lag` | Histogram | `{record}` | Non-negative scanner-fetch record lag |

Every measurement uses the same bounded attributes:

- `fluss.event.kind`
- `fluss.operation`
- `fluss.error.class`
- `fluss.api.key`
- `fluss.server.type`
- `fluss.failed`

Enum attributes are numeric values from `fgo`. Addresses, table and object
paths, bucket IDs, payloads, credentials, tokens, server messages, and error
text are never attached.

## Dependency review

The initial reviewed API version is OpenTelemetry Go v1.44.0, released on
2026-05-27, requiring Go 1.25 and licensed Apache-2.0. Only
`go.opentelemetry.io/otel` and `go.opentelemetry.io/otel/metric` are imported.
The SDK and exporters remain application choices. `task security` checks the
resulting graph with `govulncheck` and Trivy.
