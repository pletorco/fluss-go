# Schema evolution

`fgo.Client` resolves the writer schema identified by each Fluss 1.0 record
or batch schema ID. Resolved schemas are shared by all readers on the client,
coalesced while a lookup is in flight, and retained in a 256-entry LRU cache.
Canceling one reader does not cancel a fetch still observed by another reader;
the fetch is canceled when no waiter remains or the client closes.

Cached schemas are client-owned read-only snapshots. Cache insertion detaches
input metadata, and each `GetTable` result owns a separate schema tree, including
column slices, nested logical types, and key definitions. Changing one returned
schema does not change the cache or another metadata result, and does not alter
the server table. Ordinary Go copies of the same returned `Table` still share
that result's slices and pointers; keep metadata passed to active readers stable.
Internal cache hits borrow the immutable snapshot rather than cloning it for
each decoded record.

Reader constructors use the supplied table schema as the target result shape.
Writer schemas in the shared cache come from server metadata.

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

A row log batch prepares its column mapping once. A KV value batch reuses
mappings for up to 256 writer schema IDs within the page. The decoder still
checks each row's values against the target column nullability.

To measure schema mapping overhead as part of batch decoding, run:

```sh
go test ./pkg/fgo -run '^$' -bench '^BenchmarkReadBatchSchemaEvolution$' -benchmem -benchtime=250ms -count=3
```

For 128 rows with up to 32 columns, the median row log decode changed from
2.00 ms and 9,859 allocations to 0.49 ms and 842 allocations. A KV page with
alternating schema IDs changed from 1.94 ms and 9,729 allocations to 0.48 ms
and 913 allocations. These local results use Go 1.26.11 on Linux/amd64 with an
AMD Ryzen 7 7735HS and in-memory inputs.

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
KV batch, and log batch regressions, plus cache-input, returned-metadata,
coalesced-fetch, concurrent-reader, and zero-allocation cache-hit ownership checks.
`task test:integration` also writes a log
record, adds a nullable column through `ALTER_TABLE`, writes the new shape, and
reads both schema IDs through one scanner, including after another returned
metadata result has been modified locally.
