# Typed data API

The `fgo` typed API adds compile-time application types without changing the
row-oriented Fluss protocol. It targets Apache Fluss `1.0.0` and
uses explicit codecs as its stable contract:

<!-- go-source: internal/docexamples/snippets_test.go typedCodec -->
```go
type Codec[T any] interface {
	Encode(T) (fgo.Row, error)
	Decode(fgo.Row) (T, error)
}
```

`NewTypedAppendWriter`, `NewTypedUpsertWriter`, `NewTypedLookuper`,
`NewTypedLogScanner`, `NewTypedBatchScanner`, and
`NewTypedSnapshotBatchScanner` delegate batching, context cancellation,
errors, flush, close, and retry behavior to their row-oriented counterparts.
Codec errors are returned without replacement or fallback.

## Mapping rules

- A nullable Fluss value is represented by an untyped `nil` in `Row`. A codec
  should map it to an application representation that can distinguish null
  from the type's zero value, such as a pointer or an explicit option type.
  A typed-nil `*big.Rat` is invalid decimal input, not SQL NULL: row validation,
  encoders, and writers reject it with `ErrInvalidRow`, including inside arrays,
  maps, and nested rows. Use an untyped `nil` for NULL or a non-nil `*big.Rat`
  (including `new(big.Rat)` for zero) for a decimal value.
- A codec must emit the Go value accepted by the matching Fluss logical type.
  Decimal, date, time, timestamp, binary, array, and map conversions remain
  explicit application-code decisions.
- The opened table schema remains authoritative. Encoders must produce values
  in its projected column order. Decoders should tolerate newly added nullable
  or defaulted columns when forward-compatible application behavior is needed.
- Reflection-based struct tags and generated codecs are not part of the stable
  API. They can be layered on `Codec[T]` without changing writers or scanners.
- Typed row scanners reject Arrow batches with `ErrUnsupportedAPI`. Consumers
  that select Arrow transport should use the Arrow APIs or provide an explicit
  Arrow-to-application adapter.

## Scan delivery and codec failures

Typed log, current-state, and snapshot scanners serialize polls through
application decoding. On a codec error, `Poll` returns no partial result and
retains one raw page. A later `Poll` decodes that same page again from the
beginning, without fetching another log batch, issuing another KV sequence,
or advancing the snapshot reader. `Done` stays false until a final page has
been delivered. A repeated error requires correcting the codec's conditions
or closing the scanner; it never skips the page.

Treat decoder input as read-only. Do not retain it, and expect rows from a
failed page to be passed to the codec more than once. Cancellation while
waiting for a poll slot is respected; an active codec call has no context
parameter and cannot be interrupted. If its poll was canceled but the page
was already read, a later poll can still retry it. Closing discards that
page and prevents a running codec from publishing a result after close.
For current-state KV scans, a successfully decoded page also carries
`SnapshotLogOffset` and `SnapshotLogOffsetKnown`. Use the reported per-bucket
offset after consuming the complete snapshot; a codec error does not deliver
either rows or the offset.
Once a row batch scanner has been wrapped, poll and close it through the
typed wrapper. An unsupported Arrow batch stops the typed scanner explicitly;
use a row format or an Arrow scanner to read those results.

## Performance

Run the codec comparison with:

```sh
go test -run '^$' -bench BenchmarkTypedCodec -benchmem ./pkg/fgo
```

The benchmark separates direct `Row` access from encode and decode work. Codec
cost is implementation-dependent; codecs that copy rows allocate, while codecs
that safely reuse immutable values can avoid that allocation. Network,
serialization, routing, and batching behavior are identical to the row API.

Reference result on Linux/amd64, Go 1.26, AMD Ryzen 7 7735HS:

| Operation | Time | Bytes | Allocations |
| --- | ---: | ---: | ---: |
| Direct row access | 3.00 ns/op | 0 B/op | 0 |
| Copying codec encode | 40.29 ns/op | 48 B/op | 1 |
| Copying codec decode | 41.36 ns/op | 48 B/op | 1 |

These numbers are evidence for the wrapper design, not a performance guarantee.
Applications should benchmark their own codec because its conversion and
ownership policy determines most of the overhead.
