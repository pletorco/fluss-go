# Error handling and recovery

`fluss-go` preserves Go error chains so callers can make recovery decisions
with `errors.Is` and `errors.As`. Treat error text as diagnostic context, not as
a stable classification API.

| Outcome | Classification | Typical action |
| --- | --- | --- |
| Invalid option, schema, row, or path | `ErrInvalidConfig`, `ErrInvalidSchema`, `ErrInvalidRow` | Correct the input; do not retry unchanged. |
| Missing point-lookup row | `ErrNotFound` in that `LookupResult` | Handle as absent data or use insert-if-not-exists. |
| Authentication or authorization | `ErrAuthentication`, `ErrAuthorization` | Correct credentials, ACLs, or bounded connection policy. |
| Server failure | `ServerError` and `ErrServerFailure` | Inspect category and `Retriable`; apply the operation-specific policy. |
| Canceled wait | `context.Canceled` or `context.DeadlineExceeded` | Determine whether the operation may already be in flight. |
| Uncertain writer bucket | `ErrWriterState` | Stop using that bucket, reconcile state, and replace the writer. |
| Closed resource | `ErrClosed` | Do not submit new work; create a new resource if appropriate. |

Unknown future server codes remain available through `ServerError`, match
`ErrServerFailure`, and are not retriable by default.

## Server error classification

Every known Fluss server error has a broad category such as `ErrMetadata`,
`ErrTimeout`, `ErrStorage`, `ErrSequence`, `ErrRecord`, or `ErrValidation`.
The typed error retains its protocol code, API key, endpoint, safe message, and
protocol retryability:

```go
var server *fgo.ServerError
if errors.As(err, &server) {
	log.Printf(
		"api=%d code=%d category-timeout=%t retriable=%t",
		server.API,
		server.Code,
		errors.Is(err, fgo.ErrTimeout),
		server.Retriable,
	)
}
```

`ServerError.Retriable` means the Fluss error class permits a retry. It does
not mean every operation can be repeated without changing its effect.
Applications should branch on the typed error and a known operation policy,
never on the message.

## Automatic retries

`WithRetryPolicy` applies only to the client's explicit safe-request
allowlist: protocol negotiation, catalog listing and existence checks, table
and schema metadata, partition listing, table statistics, and offset
resolution. The initial call counts toward `MaxAttempts`, backoff observes the
caller context, and cancellation stops further attempts.

Point and prefix lookups, log fetches, and all mutations are not automatically
retried by this policy. Bucket requests may invalidate stale metadata and
reroute once after a server metadata error. The metadata response indicates
that the addressed server could not perform the operation; this is separate
from retrying an ambiguous transport failure.

Do not wrap a mutation in a generic retry loop merely because its
`ServerError` is marked retriable. Use a server-provided idempotency contract,
an application idempotency key, or explicit reconciliation before repeating
it.

## Writer failures and uncertain outcomes

`Append`, `Upsert`, `PartialUpsert`, and `Delete` return a `WriteFuture`.
`Await(ctx)` cancellation stops that caller from waiting; it does not cancel a
mutation that the writer may already have sent. Await the same future again
with a live context when the application can continue waiting.

When a batch fails, its futures receive the original error and the writer
conservatively poisons that bucket. Later mutations routed to the same bucket
match `ErrWriterState`; other buckets in the same writer remain independent.
Do not automatically replay the uncertain records:

1. Stop accepting new work for the affected logical key or bucket.
2. Retain the original record identity and error for reconciliation.
3. For a primary-key table, read the authoritative row when that can establish
   whether the mutation took effect.
4. For a log table, use application event identity or another durable
   deduplication record; an absent acknowledgement alone does not prove the
   append was rejected.
5. Close the writer, accounting for a close or flush error, and create a new
   writer only after choosing whether each uncertain record must be replayed.

`Flush(ctx)` and `Close(ctx)` are barriers for previously accepted work, but
their context cancellation can also stop the caller before the barrier result
is known. Preserve the first causal write error rather than replacing it with
only a shutdown error.

## Partial results

Batch-shaped APIs preserve input association and successful work:

- `Lookup` returns one `LookupResult` per key. A missing row has
  `ErrNotFound`; other keys may still succeed.
- `PrefixLookup` returns one `PrefixLookupResult` per prefix.
- `LogScanner.Poll` may return records and `BucketErrors` together. Process the
  records, retain the bucket failures, and always release owned Arrow batches.
- `fadm.ListOffsets` and `fadm.TableStats` return one result per requested
  bucket.
- ACL creation and deletion return one result per input entry or filter after
  request-level validation succeeds.

Do not convert one element error into failure of the complete result set, and
do not drop failures merely because another element succeeded. Preserve the
original key, prefix, ACL, or bucket identity in application diagnostics.

## Cancellation and shutdown

Contexts bound waiting and request work; they do not provide a distributed
transaction rollback. If cancellation races with a mutation, classify the
outcome as uncertain until the API or application state proves otherwise.
Close child writers, scanners, and lookup clients before closing the shared
client. `Client.Close` is idempotent and prevents new requests, but it cannot
revoke a mutation already accepted by the server.
