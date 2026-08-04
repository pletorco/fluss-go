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

## Reader lifecycle

The target result schema is the `fgo.Table.Schema` supplied when a lookup or
scanner is created. After `ALTER_TABLE`, call `GetTable` until it returns a
different schema ID, then create new writers and readers from that refreshed
`Table`. Existing readers keep their original result shape and do not silently
change columns while in use.

Historical writer schemas do not require the application to reopen one reader
per schema ID. The client fetches each unknown ID once for concurrent callers,
maps every compatible row to the reader's target schema, and keeps up to 256
resolved schemas across all readers. Applications should still bound the
lifetime of clients used against catalogs with unusually high schema churn.

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
