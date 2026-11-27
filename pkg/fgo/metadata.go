package fgo

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

// ServerNode identifies one coordinator or tablet endpoint.
type ServerNode struct {
	// ID is the server node identifier.
	ID int32
	// Address is the dialable host:port endpoint.
	Address string
	// ServerType is the negotiated Fluss server type.
	ServerType ServerType
}

// BucketMetadata describes one bucket's leader and replica state.
type BucketMetadata struct {
	// ID is the bucket identifier within its logical table or physical partition.
	ID int32
	// Leader is nil while no leader is elected.
	Leader *ServerNode
	// Replicas lists all replica node IDs in server order.
	Replicas []int32
	// ISR lists the in-sync replica node IDs in server order.
	ISR []int32
	// LeaderEpoch is the current leader generation when LeaderEpochKnown is true.
	LeaderEpoch int32
	// LeaderEpochKnown reports whether Fluss supplied a leader epoch.
	LeaderEpochKnown bool
	// BucketEpoch is the generation of the complete leader/ISR state when BucketEpochKnown is true.
	BucketEpoch int32
	// BucketEpochKnown reports whether Fluss supplied a bucket epoch.
	BucketEpochKnown bool
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
	Buckets map[int32]ServerNode
	// BucketDetails maps bucket IDs to complete Fluss 1.0 leader and replica state.
	BucketDetails map[int32]BucketMetadata
	// BucketCount is the routing bucket count for the logical table.
	BucketCount int32
	// BucketCountEpoch is the table bucket-count generation when BucketCountEpochKnown is true.
	BucketCountEpoch int64
	// BucketCountEpochKnown reports whether Fluss supplied a bucket-count epoch.
	BucketCountEpochKnown bool
	// RemoteDataDirectory is the server-advertised remote storage directory.
	RemoteDataDirectory string
	// Partitions maps canonical partition names to physical metadata.
	Partitions map[string]PartitionMetadata

	coordinator ServerNode
	tablets     map[int32]ServerNode
}

// PartitionMetadata describes a named partition and its tablet leaders.
type PartitionMetadata struct {
	// Path identifies the logical table and physical partition.
	Path PhysicalTablePath
	// ID is the server-assigned partition identifier.
	ID int64
	// Buckets maps bucket IDs to current tablet leaders.
	Buckets map[int32]ServerNode
	// BucketDetails maps bucket IDs to complete Fluss 1.0 leader and replica state.
	BucketDetails map[int32]BucketMetadata
	// BucketCount is the partition's actual routing bucket count.
	BucketCount int32

	// Custom Router fetchers may not have a wire-provided parent identity.
	parentTableID      int64
	parentTableIDKnown bool
	coordinator        ServerNode
	tablets            map[int32]ServerNode
}

// Metadata describes a snapshot of coordinator, tablet, and table routing.
type Metadata struct {
	// Coordinator is the current coordinator node.
	Coordinator ServerNode
	// Tablets maps tablet node IDs to endpoints.
	Tablets map[int32]ServerNode
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
	metadata         Metadata // Owned by the router; cache map access requires mu.
	fetch            MetadataFetcher
	fetchPartition   PhysicalMetadataFetcher
	flights          *coalescer[TablePath, struct{}]
	partitionFlights *coalescer[string, struct{}]
}

// NewRouter creates an empty metadata router.
func NewRouter(coordinator ServerNode, fetch MetadataFetcher) *Router {
	return &Router{
		metadata:         Metadata{Coordinator: coordinator, Tablets: make(map[int32]ServerNode), Tables: make(map[TablePath]TableMetadata)},
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
func (r *Router) Coordinator() ServerNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.metadata.Coordinator
}

// Route returns the current tablet leader for a logical table bucket,
// refreshing missing metadata or a cached unavailable leader once.
func (r *Router) Route(ctx context.Context, path TablePath, bucket int32) (ServerNode, error) {
	node, _, err := r.routePhysical(ctx, PhysicalTablePath{TablePath: path}, bucket)
	return node, err
}

// RoutePhysical returns the leader for a bucket in a named partition. An empty partition names
// the unpartitioned table and behaves like Route. Missing metadata or a cached
// unavailable leader triggers at most one refresh per route.
func (r *Router) RoutePhysical(ctx context.Context, path PhysicalTablePath, bucket int32) (ServerNode, error) {
	node, _, err := r.routePhysical(ctx, path, bucket)
	return node, err
}

func (r *Router) routePhysical(ctx context.Context, path PhysicalTablePath, bucket int32) (ServerNode, int32, error) {
	if err := path.Validate(); err != nil {
		return ServerNode{}, 0, err
	}
	node, count, err := r.lookupPhysical(path, bucket)
	noLeader := errors.Is(err, ErrNoBucketLeader)
	if node != (ServerNode{}) || (err != nil && !noLeader) {
		return node, count, err
	}
	// An existing leaderless partition is a cache hit for a non-forced refresh.
	if err := r.refreshPhysical(ctx, path, noLeader); err != nil {
		return ServerNode{}, 0, err
	}
	return r.lookupPhysicalRequired(path, bucket)
}

func (r *Router) lookupPhysicalRequired(path PhysicalTablePath, bucket int32) (ServerNode, int32, error) {
	node, count, err := r.lookupPhysical(path, bucket)
	if node != (ServerNode{}) || err != nil {
		return node, count, err
	}
	return ServerNode{}, count, fmt.Errorf("%w: %s bucket %d", ErrUnknownBucket, path, bucket)
}

func (r *Router) lookupPhysical(path PhysicalTablePath, bucket int32) (ServerNode, int32, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	table, ok := r.metadata.Tables[path.TablePath]
	if !ok {
		return ServerNode{}, 0, nil
	}
	if path.Partition == "" {
		return routedBucket(path.String(), bucket, table.BucketCount, table.Buckets, table.BucketDetails)
	}
	partition, ok := table.Partitions[physicalTableKey(path)]
	if !ok {
		return ServerNode{}, 0, nil
	}
	return routedBucket(path.String(), bucket, partition.BucketCount, partition.Buckets, partition.BucketDetails)
}

func routedBucket(path string, bucket, count int32, leaders map[int32]ServerNode, details map[int32]BucketMetadata) (ServerNode, int32, error) {
	node, hasLeader := leaders[bucket]
	_, hasDetails := details[bucket]
	if !hasLeader && !hasDetails && (bucket < 0 || bucket >= count) {
		return ServerNode{}, count, nil
	}
	if count <= 0 {
		count = int32(len(details))
		if count == 0 {
			count = int32(len(leaders))
		}
	}
	if count <= 0 {
		return ServerNode{}, 0, fmt.Errorf("%w: %s has no routing bucket count", ErrMetadata, path)
	}
	if hasLeader {
		return node, count, nil
	}
	return ServerNode{}, count, fmt.Errorf("%w: %s bucket %d", ErrNoBucketLeader, path, bucket)
}

// Refresh replaces cached metadata for path with an authoritative snapshot.
func (r *Router) Refresh(ctx context.Context, path TablePath) error {
	return r.refresh(ctx, path, true)
}

func (r *Router) refresh(ctx context.Context, path TablePath, force bool) error {
	if r.fetch == nil {
		return fmt.Errorf("%w: %s", ErrUnknownTable, path)
	}
	if !force && r.hasTable(path) {
		return nil
	}
	_, err := r.flights.Do(ctx, path, func(workCtx context.Context) (struct{}, error) {
		if !force && r.hasTable(path) {
			return struct{}{}, nil
		}
		table, fetchErr := r.fetch(workCtx, path)
		if fetchErr == nil && table.Path != path {
			fetchErr = fmt.Errorf("%w: refresh returned %s for %s", ErrUnknownTable, table.Path, path)
		}
		if fetchErr != nil {
			return struct{}{}, fetchErr
		}
		if err := workCtx.Err(); err != nil {
			return struct{}{}, err
		}
		table = cloneTableMetadata(table)
		r.mu.Lock()
		defer r.mu.Unlock()
		// An abandoned flight may return after its replacement has already
		// published. Check cancellation while holding the publication lock.
		if err := workCtx.Err(); err != nil {
			return struct{}{}, err
		}
		r.metadata.Tables[path] = table
		r.setServersLocked(table.coordinator, table.tablets)
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
	handled, err := r.preparePhysicalRefresh(ctx, path)
	if handled || err != nil {
		return err
	}
	key := physicalTableKey(path)
	_, err = r.partitionFlights.Do(ctx, key, func(workCtx context.Context) (struct{}, error) {
		return struct{}{}, r.refreshPhysicalOnce(workCtx, path, key, force)
	}, nil)
	return err
}

func (r *Router) preparePhysicalRefresh(
	ctx context.Context,
	path PhysicalTablePath,
) (bool, error) {
	if err := path.Validate(); err != nil {
		return true, err
	}
	if path.Partition == "" {
		return true, r.Refresh(ctx, path.TablePath)
	}
	if r.hasTable(path.TablePath) {
		return false, nil
	}
	return false, r.refresh(ctx, path.TablePath, false)
}

func (r *Router) refreshPhysicalOnce(
	ctx context.Context,
	path PhysicalTablePath,
	key string,
	force bool,
) error {
	if !force {
		r.mu.RLock()
		cached := r.hasPartitionLocked(path)
		r.mu.RUnlock()
		if cached {
			return nil
		}
	}
	partition, fetchUsed, err := r.fetchPhysicalMetadata(ctx, path, key)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if fetchUsed {
		if partition.parentTableIDKnown && !r.hasTableID(path.TablePath, partition.parentTableID) {
			// The cached table may have been recreated. Refresh at most once;
			// publication still checks the current parent under the lock.
			if err := r.Refresh(ctx, path.TablePath); err != nil {
				return err
			}
		}
		partition = clonePartitionMetadata(partition)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if fetchUsed {
		return r.publishPartitionLocked(path, key, partition)
	}
	if !r.hasPartitionLocked(path) {
		return fmt.Errorf("%w: %s", ErrUnknownPartition, path)
	}
	return nil
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

// publishPartitionLocked inserts an owned partition snapshot while holding mu.
func (r *Router) publishPartitionLocked(path PhysicalTablePath, key string, partition PartitionMetadata) error {
	table, ok := r.metadata.Tables[path.TablePath]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownTable, path.TablePath)
	}
	if partition.parentTableIDKnown && partition.parentTableID != table.ID {
		return fmt.Errorf("%w: %s belongs to table ID %d, cached parent is %d", ErrMetadata, path, partition.parentTableID, table.ID)
	}
	table.Partitions[key] = partition
	r.setServersLocked(partition.coordinator, partition.tablets)
	return nil
}

// Invalidate removes cached metadata for path.
func (r *Router) Invalidate(path TablePath) {
	r.mu.Lock()
	delete(r.metadata.Tables, path)
	r.mu.Unlock()
}

// InvalidatePhysical removes one partition while retaining the table's other cached locations.
func (r *Router) InvalidatePhysical(path PhysicalTablePath) {
	if path.Partition == "" {
		r.Invalidate(path.TablePath)
		return
	}
	r.mu.Lock()
	if table, ok := r.metadata.Tables[path.TablePath]; ok {
		delete(table.Partitions, physicalTableKey(path))
	}
	r.mu.Unlock()
}

// RouteAfterMetadataError invalidates one stale cache entry and performs at most one refresh.
// Callers should use it only for errors matching ErrMetadata.
func (r *Router) RouteAfterMetadataError(ctx context.Context, path TablePath, bucket int32, cause error) (ServerNode, error) {
	if !errors.Is(cause, ErrMetadata) {
		return ServerNode{}, cause
	}
	r.Invalidate(path)
	return r.Route(ctx, path, bucket)
}

func (r *Router) hasPartitionLocked(path PhysicalTablePath) bool {
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

func (r *Router) hasTableID(path TablePath, tableID int64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	table, ok := r.metadata.Tables[path]
	return ok && table.ID == tableID
}

func physicalTableKey(path PhysicalTablePath) string {
	return path.String()
}

func cloneTableMetadata(table TableMetadata) TableMetadata {
	next := table
	next.Buckets = maps.Clone(table.Buckets)
	next.BucketDetails = cloneBucketDetails(table.BucketDetails)
	next.tablets = maps.Clone(table.tablets)
	next.Partitions = make(map[string]PartitionMetadata, len(table.Partitions))
	for name, partition := range table.Partitions {
		next.Partitions[name] = clonePartitionMetadata(partition)
	}
	return next
}

func clonePartitionMetadata(partition PartitionMetadata) PartitionMetadata {
	partition.Buckets = maps.Clone(partition.Buckets)
	partition.BucketDetails = cloneBucketDetails(partition.BucketDetails)
	partition.tablets = maps.Clone(partition.tablets)
	return partition
}

func cloneBucketDetails(details map[int32]BucketMetadata) map[int32]BucketMetadata {
	if details == nil {
		return nil
	}
	next := make(map[int32]BucketMetadata, len(details))
	for id, detail := range details {
		cloned := detail
		cloned.Replicas = append([]int32(nil), detail.Replicas...)
		cloned.ISR = append([]int32(nil), detail.ISR...)
		if detail.Leader != nil {
			leader := *detail.Leader
			cloned.Leader = &leader
		}
		next[id] = cloned
	}
	return next
}

// setServersLocked publishes server metadata from an owned snapshot. The
// tablets map is read-only once copied and may be shared with that snapshot.
func (r *Router) setServersLocked(coordinator ServerNode, tablets map[int32]ServerNode) {
	if coordinator.Address != "" {
		r.metadata.Coordinator = coordinator
	}
	if tablets != nil {
		r.metadata.Tablets = tablets
	}
}
