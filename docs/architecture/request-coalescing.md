# Request coalescing

Concurrent callers requesting the same connection, logical table metadata,
physical partition metadata, dynamic partition creation, or table schema share
one operation. These paths use one internal generic coalescer with the following
context and ownership contract:

- the first caller is not a privileged owner;
- every caller waits with its own context and can cancel independently;
- the operation has a separate context and continues while at least one waiter
  remains;
- when the last waiter leaves, the operation context is canceled and a later
  caller may start a replacement;
- closing a coalescer cancels all operations and gives current and later callers
  the configured close error;
- all waiters receive the same result value and error identity;
- a successful resource result that no waiter claims is passed to an explicit
  disposer when the caller has not already transferred ownership to a cache.

Cache publication occurs inside the shared operation while holding the cache
owner's mutex. This prevents a new operation from starting between flight
completion and publication. Connections are owned by the connection cache after
publication and are closed by the connection manager. Metadata and schemas are
copied into their caches before the shared operation completes.

## Routing metadata ownership and refresh cost

The router owns its cache; it does not hand out cached maps. Route readers
take the existing read lock and return a server node by value. Table or
partition refreshes copy the incoming metadata (including nested replica
state) before taking the write lock. Before publishing, they check the
operation context again and verify the parent table ID for partitions.
Publication and invalidation update only the affected entry under the lock.
A partition refresh that must reload its parent table still copies that new
table response, but neither path copies unrelated cached metadata.

To measure a single partition refresh without network overhead:

```sh
go test ./pkg/fgo -run '^$' -bench '^BenchmarkRouterRefreshPartition$' -benchmem -benchtime=300ms -count=3
```

Local medians (Go 1.26.11, Linux/amd64, AMD Ryzen 7 7735HS; four buckets and
three replicas per partition):

| Cached partitions | Time before → after | Bytes/op before → after | Allocations/op before → after |
| --- | ---: | ---: | ---: |
| 1 | 7.068 → 3.241 µs | 4,464 → 1,936 | 47 → 23 |
| 100 | 122.378 → 3.263 µs | 167,736 → 1,936 | 1,732 → 23 |
| 1,000 | 1,348.903 → 3.358 µs | 1,673,252 → 1,936 | 17,034 → 23 |

These are in-memory measurements of the refresh path, not estimates of
end-to-end RPC latency.

## Build-vs-buy decision

`golang.org/x/sync/singleflight` was evaluated because it reliably suppresses
duplicate function calls. It was not selected for this use because `Do` gives
the first caller's function whatever context the caller captured, while
`DoChan` only separates result waiting. The package does not track independent
waiters, cancel work when the last waiter leaves, close a group with a stable
error, or dispose an unclaimed resource result. Adding those policies around
`singleflight` would retain nearly all of the custom state needed here.

The selected helper is deliberately small, unexported, generic, and directly
race-tested. No new module dependency is introduced. Re-evaluate this decision
if `singleflight` gains waiter-aware context and lifecycle semantics or the
client no longer needs those semantics.
