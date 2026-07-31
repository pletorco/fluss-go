# Observability

Core `fgo.MetricsObserver` emits synchronous, bounded-cardinality events for
RPCs, connection attempts, retries, remote I/O, writes, scans, decoding,
throttling, and lookups. It deliberately excludes addresses, table and object
paths, bucket IDs, payloads, credentials, token bytes, and server error text.
Observer panics are isolated from client operations.

The optional [`adapters/otel`](../adapters/otel) package maps those events to
OpenTelemetry counters and histograms. It depends only on the stable metrics
API. The application provides a `metric.MeterProvider` and owns all SDK,
resource, view, reader, exporter, force-flush, and shutdown decisions.

Close Fluss writers, scanners, lookup clients, and the shared `fgo.Client`
before flushing and shutting down the application provider. The adapter does
not start goroutines and does not perform exporter I/O. Export intervals,
timeouts, queues, and collector availability therefore remain under the
application's OpenTelemetry configuration.

The complete instrument and attribute contract, dependency review, and
compile-checked example are in the
[OpenTelemetry adapter manual](../adapters/otel/README.md). OpenTelemetry's
official Go documentation describes provider and exporter setup; exporter
packages are intentionally not selected by this library.
