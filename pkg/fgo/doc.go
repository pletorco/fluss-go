// Package fgo provides the Apache Fluss 0.9.1 data client.
//
// Open one [Client] for an application and derive table handles, writers,
// scanners, lookup clients, and administrative clients from it. A Client owns
// negotiated coordinator and tablet connections and is safe for concurrent use.
// Close child resources before calling [Client.Close].
//
// # Tables and records
//
// [Client.OpenTable] loads the authoritative [Table] and [Schema]. Log tables
// support [LogWriter] and [LogScanner]. Primary-key tables support [KVWriter],
// [LookupClient], [BatchScanner], and snapshot scans. [Row] is the portable
// value representation; the generic typed wrappers adapt application values
// through explicit [Codec] and [KeyCodec] implementations.
//
// # Cancellation and partial results
//
// Blocking operations accept a context. Cancellation stops local waiting but
// cannot prove that an in-flight mutation was rejected by the server. Writers
// report ambiguous state through [ErrWriterState]. Multi-bucket reads preserve
// successful records alongside per-bucket errors.
//
// # Arrow ownership
//
// Arrow-backed APIs use explicit ownership. An input record passed to
// [LogWriter.AppendArrow] must remain valid until its [WriteFuture] completes.
// Call [ScanResult.Release], [BatchResult.Release], or
// [ArrowLogBatch.Release] for decoded records owned by returned results.
//
// # Errors and compatibility
//
// Public errors support errors.Is and errors.As. The package negotiates
// protocol versions but intentionally supports only Apache Fluss 0.9.1.
// Later server versions require explicit protocol, fixture, and integration
// validation before they are supported.
//
// The public Go API is experimental before v1. Applications should pin a
// release and review the project changelog before upgrading.
package fgo
