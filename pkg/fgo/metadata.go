package fgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrUnknownTable     = errors.New("fgo: unknown table")
	ErrUnknownPartition = errors.New("fgo: unknown partition")
	ErrUnknownBucket    = errors.New("fgo: unknown bucket")
	ErrNoBucketLeader   = errors.New("fgo: bucket has no tablet leader")
)

type TablePath struct {
	Database string
	Table    string
}

func (p TablePath) String() string { return p.Database + "." + p.Table }

func (p TablePath) Validate() error {
	if p.Database == "" || p.Table == "" {
		return fmt.Errorf("%w: table path requires database and table", ErrInvalidConfig)
	}
	return nil
}

type Node struct {
	ID      int32
	Address string
	Role    ServerRole
}

type TableMetadata struct {
	Path       TablePath
	ID         int64
	SchemaID   int32
	Buckets    map[int32]Node
	Partitions map[string]PartitionMetadata

	coordinator Node
	tablets     map[int32]Node
}

// PartitionMetadata describes a named partition and its tablet leaders.
type PartitionMetadata struct {
	Path    PhysicalTablePath
	ID      int64
	Buckets map[int32]Node

	coordinator Node
	tablets     map[int32]Node
}

type Metadata struct {
	Coordinator Node
	Tablets     map[int32]Node
	Tables      map[TablePath]TableMetadata
}

type MetadataFetcher func(context.Context, TablePath) (TableMetadata, error)

// PhysicalMetadataFetcher refreshes a single named partition. It is optional because a table
// metadata response can include all of its partitions.
type PhysicalMetadataFetcher func(context.Context, PhysicalTablePath) (PartitionMetadata, error)

type Router struct {
	mu               sync.RWMutex
	metadata         Metadata
	fetch            MetadataFetcher
	fetchPartition   PhysicalMetadataFetcher
	flights          map[TablePath]*metadataFlight
	partitionFlights map[string]*partitionMetadataFlight
}

type metadataFlight struct {
	done chan struct{}
	err  error
}

type partitionMetadataFlight struct {
	done chan struct{}
	err  error
}

func NewRouter(coordinator Node, fetch MetadataFetcher) *Router {
	return &Router{
		metadata:         Metadata{Coordinator: coordinator, Tablets: make(map[int32]Node), Tables: make(map[TablePath]TableMetadata)},
		fetch:            fetch,
		flights:          make(map[TablePath]*metadataFlight),
		partitionFlights: make(map[string]*partitionMetadataFlight),
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

func (r *Router) Coordinator() Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.metadata.Coordinator
}

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
	if err := r.RefreshPhysical(ctx, path); err != nil {
		return Node{}, err
	}
	if node, ok := r.lookupPartition(path, bucket); ok {
		return node, nil
	}
	return Node{}, fmt.Errorf("%w: %s bucket %d", ErrUnknownBucket, path, bucket)
}

func (r *Router) Refresh(ctx context.Context, path TablePath) error {
	return r.refresh(ctx, path, true)
}

func (r *Router) refresh(ctx context.Context, path TablePath, force bool) error {
	if r.fetch == nil {
		return fmt.Errorf("%w: %s", ErrUnknownTable, path)
	}
	r.mu.Lock()
	if !force {
		if _, ok := r.metadata.Tables[path]; ok {
			r.mu.Unlock()
			return nil
		}
	}
	if flight := r.flights[path]; flight != nil {
		r.mu.Unlock()
		select {
		case <-flight.done:
			return flight.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	flight := &metadataFlight{done: make(chan struct{})}
	r.flights[path] = flight
	r.mu.Unlock()

	table, err := r.fetch(ctx, path)
	if err == nil && table.Path != path {
		err = fmt.Errorf("%w: refresh returned %s for %s", ErrUnknownTable, table.Path, path)
	}
	r.mu.Lock()
	if err == nil {
		next := cloneMetadata(r.metadata)
		applyTableServers(&next, table)
		next.Tables[path] = cloneTableMetadata(table)
		r.metadata = next
	}
	flight.err = err
	delete(r.flights, path)
	close(flight.done)
	r.mu.Unlock()
	return err
}

// RefreshPhysical refreshes exactly one named partition. When a dedicated physical fetcher is
// unavailable, it performs one table refresh and uses the partition included in that response.
func (r *Router) RefreshPhysical(ctx context.Context, path PhysicalTablePath) error {
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
	flight, owner, err := r.beginPartitionRefresh(ctx, key)
	if err != nil || !owner {
		return err
	}
	partition, fetchUsed, err := r.fetchPhysicalMetadata(ctx, path, key)
	return r.finishPartitionRefresh(path, key, flight, partition, fetchUsed, err)
}

func (r *Router) beginPartitionRefresh(
	ctx context.Context,
	key string,
) (*partitionMetadataFlight, bool, error) {
	r.mu.Lock()
	if flight := r.partitionFlights[key]; flight != nil {
		r.mu.Unlock()
		select {
		case <-flight.done:
			return nil, false, flight.err
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	flight := &partitionMetadataFlight{done: make(chan struct{})}
	r.partitionFlights[key] = flight
	r.mu.Unlock()
	return flight, true, nil
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

func (r *Router) finishPartitionRefresh(
	path PhysicalTablePath,
	key string,
	flight *partitionMetadataFlight,
	partition PartitionMetadata,
	fetchUsed bool,
	err error,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil && fetchUsed {
		err = r.applyPhysicalMetadata(path, key, partition)
	}
	if err == nil && !r.hasPartition(path) {
		err = fmt.Errorf("%w: %s", ErrUnknownPartition, path)
	}
	flight.err = err
	delete(r.partitionFlights, key)
	close(flight.done)
	return err
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
