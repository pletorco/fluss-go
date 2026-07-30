// Package fadm provides Apache Fluss 0.9.1 administrative operations.
//
// Construct a [Client] with [New] from an existing fgo.Client. The
// administrative client reuses the data client's negotiated connections and
// does not own them. Stop administrative work and close data-plane resources
// before closing the shared fgo client.
//
// The package covers databases, tables, schemas, partitions, offsets, ACLs,
// cluster configuration, server discovery and tags, rebalance operations,
// producer offsets, primary-key snapshots and leases, filesystem security
// tokens, lake snapshots, and table statistics.
//
// Methods validate local input before sending requests. Operations that span
// multiple buckets return one result per bucket so callers can distinguish
// partial failure from complete failure. Errors returned by the server retain
// the classifications defined by package fgo.
package fadm
