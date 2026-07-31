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
