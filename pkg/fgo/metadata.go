package fgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Metadata lookup and routing errors.
var (
	ErrUnknownTable     = errors.New("fgo: unknown table")
	ErrUnknownPartition = errors.New("fgo: unknown partition")
	ErrUnknownBucket    = errors.New("fgo: unknown bucket")
	ErrNoBucketLeader   = errors.New("fgo: bucket has no tablet leader")
)

// TablePath identifies a logical table by database and table name.
type TablePath struct {
	// Database is the logical database name.
	Database string
	// Table is the logical table name.
	Table string
}

// String returns the canonical database.table path.
func (p TablePath) String() string { return p.Database + "." + p.Table }

// Validate checks that both path components are present.
func (p TablePath) Validate() error {
	if p.Database == "" || p.Table == "" {
		return fmt.Errorf("%w: table path requires database and table", ErrInvalidConfig)
	}
	return nil
}

// Node identifies one coordinator or tablet endpoint.
type Node struct {
	// ID is the server node identifier.
	ID int32
	// Address is the dialable host:port endpoint.
	Address string
	// Role is the negotiated Fluss server role.
	Role ServerRole
}

// TableMetadata contains table IDs, bucket leaders, and named partitions.
type TableMetadata struct {
	// Path is the logical table path.
	Path TablePath
	// ID is the server-assigned table identifier.
	ID int64
	// SchemaID identifies the current schema version.
	SchemaID int32
	// Buckets maps bucket IDs to current tablet leaders.
	Buckets map[int32]Node
	// Partitions maps canonical partition names to physical metadata.
	Partitions map[string]PartitionMetadata

	coordinator Node
	tablets     map[int32]Node
}

// PartitionMetadata describes a named partition and its tablet leaders.
type PartitionMetadata struct {
	// Path identifies the logical table and physical partition.
	Path PhysicalTablePath
	// ID is the server-assigned partition identifier.
	ID int64
	// Buckets maps bucket IDs to current tablet leaders.
	Buckets map[int32]Node

	coordinator Node
	tablets     map[int32]Node
}

// Metadata is an immutable snapshot of coordinator, tablet, and table routing.
type Metadata struct {
	// Coordinator is the current coordinator node.
	Coordinator Node
	// Tablets maps tablet node IDs to endpoints.
	Tablets map[int32]Node
	// Tables maps logical paths to current metadata snapshots.
	Tables map[TablePath]TableMetadata
}

// MetadataFetcher loads authoritative metadata for one logical table.
type MetadataFetcher func(context.Context, TablePath) (TableMetadata, error)

// PhysicalMetadataFetcher refreshes a single named partition. It is optional because a table
// metadata response can include all of its partitions.
type PhysicalMetadataFetcher func(context.Context, PhysicalTablePath) (PartitionMetadata, error)

// Router caches table and partition leaders and coalesces concurrent refreshes.
// A Router is safe for concurrent use.
type Router struct {
	mu               sync.RWMutex
	metadata         Metadata
	fetch            MetadataFetcher
	fetchPartition   PhysicalMetadataFetcher
	flights          *coalescer[TablePath, struct{}]
	partitionFlights *coalescer[string, struct{}]
}

// NewRouter creates an empty metadata router.
func NewRouter(coordinator Node, fetch MetadataFetcher) *Router {
	return &Router{
		metadata:         Metadata{Coordinator: coordinator, Tablets: make(map[int32]Node), Tables: make(map[TablePath]TableMetadata)},
		fetch:            fetch,
		flights:          newCoalescer[TablePath, struct{}](),
		partitionFlights: newCoalescer[string, struct{}](),
	}
}

// WithPhysicalMetadataFetcher configures refreshes for individual partitions.
// It is intended for construction time, before the router is shared by requests.
func (r *Router) WithPhysicalMetadataFetcher(fetch PhysicalMetadataFetcher) *Router {
	r.mu.Lock()
	r.fetchPartition = fetch
	r.mu.Unlock()
	return r
}

// Coordinator returns the current coordinator snapshot.
func (r *Router) Coordinator() Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.metadata.Coordinator
}

// Route returns the current tablet leader for a logical table bucket,
// refreshing missing metadata once.
func (r *Router) Route(ctx context.Context, path TablePath, bucket int32) (Node, error) {
	if err := path.Validate(); err != nil {
		return Node{}, err
	}
	if node, ok := r.lookup(path, bucket); ok {
		return node, nil
	}
	if err := r.Refresh(ctx, path); err != nil {
		return Node{}, err
	}
	if node, ok := r.lookup(path, bucket); ok {
		return node, nil
	}
	return Node{}, fmt.Errorf("%w: %s bucket %d", ErrUnknownBucket, path, bucket)
}

// RoutePhysical returns the leader for a bucket in a named partition. An empty partition names
// the unpartitioned table and behaves like Route.
func (r *Router) RoutePhysical(ctx context.Context, path PhysicalTablePath, bucket int32) (Node, error) {
	if err := path.Validate(); err != nil {
		return Node{}, err
	}
	if path.Partition == "" {
		return r.Route(ctx, path.TablePath, bucket)
	}
	if node, ok := r.lookupPartition(path, bucket); ok {
		return node, nil
	}
	if err := r.refreshPhysical(ctx, path, false); err != nil {
		return Node{}, err
	}
	if node, ok := r.lookupPartition(path, bucket); ok {
		return node, nil
	}
	return Node{}, fmt.Errorf("%w: %s bucket %d", ErrUnknownBucket, path, bucket)
}

// Refresh replaces cached metadata for path with an authoritative snapshot.
func (r *Router) Refresh(ctx context.Context, path TablePath) error {
	return r.refresh(ctx, path, true)
}

func (r *Router) refresh(ctx context.Context, path TablePath, force bool) error {
	if r.fetch == nil {
		return fmt.Errorf("%w: %s", ErrUnknownTable, path)
	}
	r.mu.RLock()
	if !force {
		if _, ok := r.metadata.Tables[path]; ok {
			r.mu.RUnlock()
			return nil
		}
	}
	r.mu.RUnlock()
	_, err := r.flights.Do(ctx, path, func(workCtx context.Context) (struct{}, error) {
		if !force {
			r.mu.RLock()
			_, cached := r.metadata.Tables[path]
			r.mu.RUnlock()
			if cached {
				return struct{}{}, nil
			}
		}
		table, fetchErr := r.fetch(workCtx, path)
		if fetchErr == nil && table.Path != path {
			fetchErr = fmt.Errorf("%w: refresh returned %s for %s", ErrUnknownTable, table.Path, path)
		}
		if fetchErr != nil {
			return struct{}{}, fetchErr
		}
		r.mu.Lock()
		next := cloneMetadata(r.metadata)
		applyTableServers(&next, table)
		next.Tables[path] = cloneTableMetadata(table)
		r.metadata = next
		r.mu.Unlock()
		return struct{}{}, nil
	}, nil)
	return err
}

// RefreshPhysical refreshes exactly one named partition. When a dedicated physical fetcher is
// unavailable, it performs one table refresh and uses the partition included in that response.
func (r *Router) RefreshPhysical(ctx context.Context, path PhysicalTablePath) error {
	return r.refreshPhysical(ctx, path, true)
}

func (r *Router) refreshPhysical(
	ctx context.Context,
	path PhysicalTablePath,
	force bool,
) error {
	if err := path.Validate(); err != nil {
		return err
	}
	if path.Partition == "" {
		return r.Refresh(ctx, path.TablePath)
	}
	if !r.hasTable(path.TablePath) {
		if err := r.refresh(ctx, path.TablePath, false); err != nil {
			return err
		}
	}
	key := physicalTableKey(path)
	_, err := r.partitionFlights.Do(ctx, key, func(workCtx context.Context) (struct{}, error) {
		if !force {
			r.mu.RLock()
			cached := r.hasPartition(path)
			r.mu.RUnlock()
			if cached {
				return struct{}{}, nil
			}
		}
		partition, fetchUsed, fetchErr := r.fetchPhysicalMetadata(workCtx, path, key)
		r.mu.Lock()
		defer r.mu.Unlock()
		if fetchErr == nil && fetchUsed {
			fetchErr = r.applyPhysicalMetadata(path, key, partition)
		}
		if fetchErr == nil && !r.hasPartition(path) {
			fetchErr = fmt.Errorf("%w: %s", ErrUnknownPartition, path)
		}
		return struct{}{}, fetchErr
	}, nil)
	return err
}

func (r *Router) fetchPhysicalMetadata(
	ctx context.Context,
	path PhysicalTablePath,
	key string,
) (PartitionMetadata, bool, error) {
	fetch := r.fetchPartition
	if fetch == nil {
		return PartitionMetadata{}, false, r.Refresh(ctx, path.TablePath)
	}
	partition, err := fetch(ctx, path)
	if err == nil && physicalTableKey(partition.Path) != key {
		err = fmt.Errorf("%w: refresh returned %s for %s", ErrUnknownPartition, partition.Path, path)
	}
	return partition, true, err
}

func (r *Router) applyPhysicalMetadata(path PhysicalTablePath, key string, partition PartitionMetadata) error {
	next := cloneMetadata(r.metadata)
	table, ok := next.Tables[path.TablePath]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownTable, path.TablePath)
	}
	if table.Partitions == nil {
		table.Partitions = make(map[string]PartitionMetadata)
	}
	table.Partitions[key] = clonePartitionMetadata(partition)
	next.Tables[path.TablePath] = table
	applyPartitionServers(&next, partition)
	r.metadata = next
	return nil
}

// Invalidate removes cached metadata for path.
func (r *Router) Invalidate(path TablePath) {
	r.mu.Lock()
	next := cloneMetadata(r.metadata)
	delete(next.Tables, path)
	r.metadata = next
	r.mu.Unlock()
}

// InvalidatePhysical removes one partition while retaining the table's other cached locations.
func (r *Router) InvalidatePhysical(path PhysicalTablePath) {
	if path.Partition == "" {
		r.Invalidate(path.TablePath)
		return
	}
	r.mu.Lock()
	next := cloneMetadata(r.metadata)
	if table, ok := next.Tables[path.TablePath]; ok {
		delete(table.Partitions, physicalTableKey(path))
		next.Tables[path.TablePath] = table
		r.metadata = next
	}
	r.mu.Unlock()
}

// RouteAfterMetadataError invalidates one stale cache entry and performs at most one refresh.
// Callers should use it only for errors matching ErrMetadata.
func (r *Router) RouteAfterMetadataError(ctx context.Context, path TablePath, bucket int32, cause error) (Node, error) {
	if !errors.Is(cause, ErrMetadata) {
		return Node{}, cause
	}
	r.Invalidate(path)
	return r.Route(ctx, path, bucket)
}

func (r *Router) lookup(path TablePath, bucket int32) (Node, bool) {
	r.mu.RLock()
	table, ok := r.metadata.Tables[path]
	if !ok {
		r.mu.RUnlock()
		return Node{}, false
	}
	node, ok := table.Buckets[bucket]
	r.mu.RUnlock()
	return node, ok
}

func (r *Router) lookupPartition(path PhysicalTablePath, bucket int32) (Node, bool) {
	r.mu.RLock()
	table, ok := r.metadata.Tables[path.TablePath]
	if !ok {
		r.mu.RUnlock()
		return Node{}, false
	}
	partition, ok := table.Partitions[physicalTableKey(path)]
	if !ok {
		r.mu.RUnlock()
		return Node{}, false
	}
	node, ok := partition.Buckets[bucket]
	r.mu.RUnlock()
	return node, ok
}

func (r *Router) hasPartition(path PhysicalTablePath) bool {
	table, ok := r.metadata.Tables[path.TablePath]
	if !ok {
		return false
	}
	_, ok = table.Partitions[physicalTableKey(path)]
	return ok
}

func (r *Router) hasTable(path TablePath) bool {
	r.mu.RLock()
	_, ok := r.metadata.Tables[path]
	r.mu.RUnlock()
	return ok
}

func physicalTableKey(path PhysicalTablePath) string {
	return path.String()
}

func cloneMetadata(metadata Metadata) Metadata {
	next := Metadata{Coordinator: metadata.Coordinator, Tablets: make(map[int32]Node, len(metadata.Tablets)), Tables: make(map[TablePath]TableMetadata, len(metadata.Tables))}
	for id, node := range metadata.Tablets {
		next.Tablets[id] = node
	}
	for path, table := range metadata.Tables {
		next.Tables[path] = cloneTableMetadata(table)
	}
	return next
}

func cloneTableMetadata(table TableMetadata) TableMetadata {
	next := TableMetadata{Path: table.Path, ID: table.ID, SchemaID: table.SchemaID, Buckets: make(map[int32]Node, len(table.Buckets)), Partitions: make(map[string]PartitionMetadata, len(table.Partitions)), coordinator: table.coordinator, tablets: cloneNodes(table.tablets)}
	for bucket, node := range table.Buckets {
		next.Buckets[bucket] = node
	}
	for name, partition := range table.Partitions {
		next.Partitions[name] = clonePartitionMetadata(partition)
	}
	return next
}

func clonePartitionMetadata(partition PartitionMetadata) PartitionMetadata {
	next := PartitionMetadata{Path: partition.Path, ID: partition.ID, Buckets: make(map[int32]Node, len(partition.Buckets)), coordinator: partition.coordinator, tablets: cloneNodes(partition.tablets)}
	for bucket, node := range partition.Buckets {
		next.Buckets[bucket] = node
	}
	return next
}

func applyTableServers(metadata *Metadata, table TableMetadata) {
	if table.coordinator.Address != "" {
		metadata.Coordinator = table.coordinator
	}
	if table.tablets != nil {
		metadata.Tablets = cloneNodes(table.tablets)
	}
}

func applyPartitionServers(metadata *Metadata, partition PartitionMetadata) {
	if partition.coordinator.Address != "" {
		metadata.Coordinator = partition.coordinator
	}
	if partition.tablets != nil {
		metadata.Tablets = cloneNodes(partition.tablets)
	}
}

func cloneNodes(nodes map[int32]Node) map[int32]Node {
	if nodes == nil {
		return nil
	}
	next := make(map[int32]Node, len(nodes))
	for id, node := range nodes {
		next[id] = node
	}
	return next
}
