# Schema evolution

`fgo.Client` resolves the writer schema identified by each Fluss 0.9.1 record
or batch schema ID. Resolved schemas are shared by all readers on the client,
coalesced while a lookup is in flight, and retained in a 256-entry LRU cache.
Canceling one reader does not cancel a fetch still observed by another reader;
the fetch is canceled when no waiter remains or the client closes.

The following read paths decode historical schema IDs:

- point and prefix lookup values;
- current-state KV value batches;
- row and Arrow log batches used by log and current-state scans.

Rows are mapped to the schema passed when the reader was created. Stable Fluss
field IDs define column identity, so a rename preserves values. Dropped writer
columns are ignored, and nullable columns added after a record was written are
returned as `nil`. A newly required column, a nullable-to-required transition
containing null, or a physical/logical type change returns `ErrInvalidSchema`.
The client does not guess defaults or perform numeric or temporal coercion.

Log projection is applied after schema resolution. This deliberately fetches
the complete batch when historical schemas may be present, so row and Arrow
results have one stable projected shape across schema versions. Arrow batches
retain their writer `SchemaID`, while their record columns conform to the
current result schema.

Missing schema IDs, malformed schema JSON, a server response carrying a
different ID, and incompatible changes are explicit errors. Schema fetches use
`GET_TABLE_SCHEMA` and therefore appear as normal `MetricRequest` observations
with `APIKeyGetTableSchema`.

The unit suite includes Java 0.9.1 schema JSON fixtures and mixed-schema lookup,
KV batch, and log batch regressions. `task test:integration` also writes a log
record, adds a nullable column through `ALTER_TABLE`, writes the new shape, and
reads both schema IDs through one scanner.
