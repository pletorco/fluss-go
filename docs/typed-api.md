# Typed data API

The `fgo` typed API adds compile-time application types without changing the
row-oriented Fluss protocol. It targets Apache Fluss `0.9.1-incubating` and
uses explicit codecs as its stable contract:

<!-- go-source: internal/docexamples/snippets_test.go typedCodec -->
```go
type Codec[T any] interface {
	Encode(T) (fgo.Row, error)
	Decode(fgo.Row) (T, error)
}
```

`NewTypedLogWriter`, `NewTypedKVWriter`, `NewTypedLookupClient`,
`NewTypedLogScanner`, `NewTypedBatchScanner`, and
`NewTypedSnapshotBatchScanner` delegate batching, context cancellation,
errors, flush, close, and retry behavior to their row-oriented counterparts.
Codec errors are returned without replacement or fallback.

## Mapping rules

- A nullable Fluss value is represented by an untyped `nil` in `Row`. A codec
  should map it to an application representation that can distinguish null
  from the type's zero value, such as a pointer or an explicit option type.
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
